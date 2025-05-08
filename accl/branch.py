import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN, MSO_VERTICAL_ANCHOR
from pptx.dml.color import RGBColor
import matplotlib.pyplot as plt
from matplotlib.table import Table
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set page config
st.set_page_config(page_title="ACCLLP Integrated Dashboard", page_icon="📊", layout="wide")

# Initialize session state for file uploads and results
if 'init' not in st.session_state:
    st.session_state.init = True
    st.session_state.sales_file = None
    st.session_state.budget_file = None
    st.session_state.os_jan_file = None
    st.session_state.os_feb_file = None
    st.session_state.logo_file = None
    st.session_state.budget_results = None
    st.session_state.customers_results = None
    st.session_state.od_results = None
    st.session_state.product_results = None

#######################################################
# CENTRALIZED BRANCH MAPPING AND UTILITY FUNCTIONS
#######################################################

# CENTRALIZED BRANCH MAPPING
branch_mapping = {
    # Budget module mappings
    'PONDY': 'PUDUCHERRY', 'PDY': 'PUDUCHERRY', 'Puducherry - PDY': 'PUDUCHERRY',
    'COVAI': 'COIMBATORE', 'CBE': 'COIMBATORE', 'Coimbatore - CBE': 'COIMBATORE',
    'ERD': 'ERODE', 'Erode - ERD': 'ERODE', 'ERD002': 'ERODE', 'ERD001': 'ERODE',
    'ERDTD1': 'ERODE', 'ERD003': 'ERODE', 'ERD004': 'ERODE', 'ERD005': 'ERODE',
    'ERD007': 'ERODE',
    'KRR': 'KARUR',
    'Chennai - CHN': 'CHENNAI', 'CHN': 'CHENNAI',
    'Tirupur - TPR': 'TIRUPUR', 'TPR': 'TIRUPUR',
    'Madurai - MDU': 'MADURAI', 'MDU': 'MADURAI',
    'POULTRY': 'POULTRY', 'Poultry - PLT': 'POULTRY',
    'SALEM': 'SALEM', 'Salem - SLM': 'SALEM',
    'HO': 'HO',
    
    # NBC module mappings
    'SLM002': 'SALEM', 'SLMTD1': 'SALEM',
    'BHV1': 'BHAVANI',
    
    # OD and Product Growth module mappings
    'CBU': 'COIMBATORE',
    'VLR': 'VELLORE', 
    'TRZ': 'TRICHY', 
    'TVL': 'TIRUNELVELI',
    'NGS': 'NAGERCOIL', 
    'PONDICHERRY': 'PUDUCHERRY',
    'BLR': 'BANGALORE', 'BANGALORE': 'BANGALORE', 'BGLR': 'BANGALORE'
}

# NBC specific branch mapping
nbc_branch_mapping = {
    'PONDY': 'PONDY', 'PDY': 'PONDY',
    'COVAI': 'ERODE_CBE_KRR',  # Represents COIMBATORE
    'ERODE': 'ERODE_CBE_KRR',
    'KARUR': 'ERODE_CBE_KRR',
    'SLM002': 'SALEM', 'SLMTD1': 'SALEM',
    'BHV1': 'BHAVANI',
    'MDU': 'MADURAI',
    'CHENNAI': 'CHENNAI', 'CHN': 'CHENNAI',
    'TIRUPUR': 'TIRUPUR', 'TPR': 'TIRUPUR',
    'POULTRY': 'POULTRY'
}

# COMMON UTILITY FUNCTIONS
def get_excel_sheets(file):
    """Get sheet names from an Excel file."""
    try:
        xl = pd.ExcelFile(file)
        return xl.sheet_names
    except Exception as e:
        logger.error(f"Error reading Excel sheets: {e}")
        st.error(f"Error reading Excel sheets: {e}")
        return []

def map_branch(branch_name, case='upper'):
    """Map branch codes/names to standardized names."""
    if pd.isna(branch_name):
        return 'Unknown'
    
    # Extract branch name from formats like 'AAAA - BRANCH'
    branch_str = str(branch_name).strip().upper()
    if ' - ' in branch_str:
        branch_str = branch_str.split(' - ')[-1].strip()
    
    # Apply mapping
    mapped_branch = branch_mapping.get(branch_str, branch_str)
    
    # Return in requested case
    if case == 'title':
        return mapped_branch.title()
    else:  # default: upper
        return mapped_branch

def create_table_image(df, title, percent_cols=None):
    """Create a table image for display in Streamlit."""
    if df is None or df.empty:
        return None
        
    fig, ax = plt.subplots(figsize=(10, len(df) * 0.5))
    ax.axis('off')
    table = Table(ax, bbox=[0, 0, 1, 1])
    nrows, ncols = df.shape
    width, height = 1.0 / ncols, 1.0 / nrows
    
    # Add column headers
    for col_idx, col_name in enumerate(df.columns):
        table.add_cell(0, col_idx, width, height, text=str(col_name), loc='center', facecolor='#0070C0')
        table[0, col_idx].set_text_props(weight='bold', color='white')
    
    # Add data rows
    for row_idx in range(nrows):
        for col_idx in range(ncols):
            value = df.iloc[row_idx, col_idx]
            # Format percentage columns
            if percent_cols and col_idx in percent_cols:
                text = f"{value}%"
            else:
                text = str(value)
            
            facecolor = '#f0f0f0' if row_idx % 2 == 0 else 'white'
            
            # Format total row
            is_total = row_idx == nrows - 1 and str(df.iloc[row_idx, 0]).upper() in ['TOTAL', 'GRAND TOTAL']
            if is_total:
                facecolor = '#D3D3D3'
                table.add_cell(row_idx + 1, col_idx, width, height, text=text, loc='center', facecolor=facecolor).set_text_props(weight='bold')
            else:
                table.add_cell(row_idx + 1, col_idx, width, height, text=text, loc='center', facecolor=facecolor)
    
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    ax.add_table(table)
    plt.title(title, fontsize=14 if len(title) > 50 else 16, weight='bold', color='#0070C0', pad=20)
    plt.tight_layout()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=100)
    img_buffer.seek(0)
    plt.close()
    return img_buffer
def create_title_slide(prs, title, logo_file=None):
    """Create a standard title slide for PPT."""
    blank_slide_layout = prs.slide_layouts[6]  # Blank slide layout
    title_slide = prs.slides.add_slide(blank_slide_layout)

    # Company Name at the Top
    company_name = title_slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(12.33), Inches(1))
    company_frame = company_name.text_frame
    company_frame.text = "Asia Crystal Commodity LLP"
    p = company_frame.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    p.font.name = "Times New Roman"
    p.font.size = Pt(36)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 112, 192)

    # Add Logo if uploaded
    if logo_file is not None:
        try:
            logo_buffer = BytesIO(logo_file.read())
            logo = title_slide.shapes.add_picture(logo_buffer, Inches(5.665), Inches(1.5), width=Inches(2), height=Inches(2))
            # Reset file pointer for future use
            logo_file.seek(0)
        except Exception as e:
            logger.error(f"Error adding logo to slide: {e}")

    # Title
    title_box = title_slide.shapes.add_textbox(Inches(0.5), Inches(4.0), Inches(12.33), Inches(1))
    title_frame = title_box.text_frame
    title_frame.text = title
    p = title_frame.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    p.font.name = "Times New Roman"
    p.font.size = Pt(32)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 128, 0)

    # Subtitle
    subtitle = title_slide.shapes.add_textbox(Inches(0.5), Inches(5.0), Inches(12.33), Inches(1))
    subtitle_frame = subtitle.text_frame
    subtitle_frame.text = "ACCLLP"
    p = subtitle_frame.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    p.font.name = "Times New Roman"
    p.font.size = Pt(28)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 112, 192)
    
    return title_slide

def add_table_slide(prs, df, title, percent_cols=None):
    """Add a slide with a table to a PPT presentation."""
    if df is None or df.empty:
        return None
        
    # Add a blank slide
    slide_layout = prs.slide_layouts[6]  # Blank layout
    slide = prs.slides.add_slide(slide_layout)
    
    # Add title
    title_shape = slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(12.33), Inches(0.75))
    title_frame = title_shape.text_frame
    title_frame.text = title
    p = title_frame.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(24)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 112, 192)
    
    # Create table
    rows, cols = df.shape
    table = slide.shapes.add_table(rows + 1, cols, Inches(0.5), Inches(1.5), Inches(12.33), Inches(5.5)).table
    
    # Set column headers
    for col_idx, col_name in enumerate(df.columns):
        cell = table.cell(0, col_idx)
        cell.text = str(col_name)
        cell.text_frame.paragraphs[0].font.size = Pt(12)
        cell.text_frame.paragraphs[0].font.bold = True
        cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
        cell.vertical_anchor = MSO_VERTICAL_ANCHOR.MIDDLE
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0, 112, 192)
        cell.text_frame.paragraphs[0].font.color.rgb = RGBColor(255, 255, 255)
    
    # Populate table data
    for row_idx in range(df.shape[0]):
        is_total_row = row_idx == rows - 1 and str(df.iloc[row_idx, 0]).upper() in ['TOTAL', 'GRAND TOTAL']
        for col_idx in range(cols):
            cell = table.cell(row_idx + 1, col_idx)
            value = df.iloc[row_idx, col_idx]
            
            # Format value
            if percent_cols and col_idx in percent_cols:
                cell.text = f"{value}%"
            else:
                cell.text = str(value)
                
            # Format cell
            cell.text_frame.paragraphs[0].font.size = Pt(11)
            cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            cell.vertical_anchor = MSO_VERTICAL_ANCHOR.MIDDLE
            
            # Alternating row colors
            if row_idx % 2 == 0 and not is_total_row:
                cell.fill.solid()
                cell.fill.fore_color.rgb = RGBColor(240, 240, 240)
                
            # Total row formatting
            if is_total_row:
                cell.text_frame.paragraphs[0].font.bold = True
                cell.fill.solid()
                cell.fill.fore_color.rgb = RGBColor(211, 211, 211)
    
    return slide

def create_consolidated_ppt(all_dfs_with_titles, logo_file=None, title="ACCLLP Consolidated Report"):
    """Create a consolidated PPT with all report data."""
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)
        
        # Create title slide
        create_title_slide(prs, title, logo_file)
        
        # Add data slides
        for df_info in all_dfs_with_titles:
            if df_info and 'df' in df_info and 'title' in df_info:
                add_table_slide(
                    prs, 
                    df_info['df'], 
                    df_info['title'],
                    df_info.get('percent_cols')
                )
        
        # Save to buffer
        ppt_buffer = BytesIO()
        prs.save(ppt_buffer)
        ppt_buffer.seek(0)
        return ppt_buffer
    except Exception as e:
        logger.error(f"Error creating consolidated PPT: {e}")
        st.error(f"Error creating consolidated PPT: {e}")
        return None
#######################################################
# MODULE 1: BUDGET VS BILLED REPORT
#######################################################

def calculate_values(sales_df, budget_df, selected_month, sales_executives, budget_executives,
                     sales_date_col, sales_area_col, sales_value_col, sales_qty_col, sales_product_group_col, sales_sl_code_col, sales_exec_col,
                     budget_area_col, budget_value_col, budget_qty_col, budget_product_group_col, budget_sl_code_col, budget_exec_col, selected_branches=None):
    """
    Calculate budget vs billed values exactly as in the original code.
    Returns four DataFrames for the different report sections.
    """
    # Create copies to avoid modifying original DataFrames
    sales_df = sales_df.copy()
    budget_df = budget_df.copy()

    # Extract all branches before filtering
    raw_sales_branches = sales_df[sales_area_col].dropna().astype(str).str.upper().str.split(' - ').str[-1].unique().tolist()
    raw_budget_branches = budget_df[budget_area_col].dropna().astype(str).str.upper().str.split(' - ').str[-1].str.replace('AAAA - ', '', regex=False).unique().tolist()
    all_branches = sorted(set([branch_mapping.get(branch, branch) for branch in raw_sales_branches + raw_budget_branches]))

    # Filter Sales by selected executives
    if sales_executives:
        sales_df = sales_df[sales_df[sales_exec_col].isin(sales_executives)].copy()
    if budget_executives:
        budget_df = budget_df[budget_df[budget_exec_col].isin(budget_executives)].copy()

    if sales_df.empty or budget_df.empty:
        st.warning("No data found for selected executives in one or both files.")
        return None, None, None, None

    # Convert date column to datetime
    sales_df[sales_date_col] = pd.to_datetime(sales_df[sales_date_col], dayfirst=True, errors='coerce')
    filtered_sales_df = sales_df[sales_df[sales_date_col].dt.strftime('%b %y') == selected_month].copy()
    if filtered_sales_df.empty:
        st.warning(f"No sales data found for {selected_month} in '{sales_date_col}'. Check date format.")
        return None, None, None, None

    # Convert columns to string and clean
    filtered_sales_df[sales_area_col] = filtered_sales_df[sales_area_col].astype(str).str.strip()
    budget_df[budget_area_col] = budget_df[budget_area_col].astype(str).str.strip()
    filtered_sales_df[sales_product_group_col] = filtered_sales_df[sales_product_group_col].astype(str).str.strip()
    filtered_sales_df[sales_sl_code_col] = filtered_sales_df[sales_sl_code_col].astype(str).str.strip().str.replace('\.0$', '', regex=True)
    budget_df[budget_product_group_col] = budget_df[budget_product_group_col].astype(str).str.strip()
    budget_df[budget_sl_code_col] = budget_df[budget_sl_code_col].astype(str).str.strip().str.replace('\.0$', '', regex=True)

    # Convert values and quantities to numeric, preserving negatives
    for col, df_name in [(sales_value_col, 'Sales Value'), (sales_qty_col, 'Sales Qty'), 
                         (budget_value_col, 'Budget Value'), (budget_qty_col, 'Budget Qty')]:
        df = filtered_sales_df if col in [sales_value_col, sales_qty_col] else budget_df
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isna().any():
                st.warning(f"Non-numeric values found in {df_name} column '{col}'. Converted to 0.")
            df[col] = df[col].fillna(0)
        except Exception as e:
            st.error(f"Error converting {df_name} column '{col}' to numeric: {e}")
            return None, None, None, None

    # Parse budget branch names
    budget_df[budget_area_col] = budget_df[budget_area_col].str.split(' - ').str[-1].str.upper()
    budget_df[budget_area_col] = budget_df[budget_area_col].str.replace('AAAA - ', '', regex=False).str.upper()
    
    # Normalize branch names
    budget_df[budget_area_col] = budget_df[budget_area_col].replace(branch_mapping)
    filtered_sales_df[sales_area_col] = filtered_sales_df[sales_area_col].str.upper().replace(branch_mapping)

    # Apply branch filter
    if selected_branches:
        filtered_sales_df = filtered_sales_df[filtered_sales_df[sales_area_col].isin(selected_branches)].copy()
        budget_df = budget_df[budget_df[budget_area_col].isin(selected_branches)].copy()

    # Normalize Product Group and SL Code for matching
    filtered_sales_df[sales_product_group_col] = filtered_sales_df[sales_product_group_col].str.upper()
    filtered_sales_df[sales_sl_code_col] = filtered_sales_df[sales_sl_code_col].str.upper()
    budget_df[budget_product_group_col] = budget_df[budget_product_group_col].str.upper()
    budget_df[budget_sl_code_col] = budget_df[budget_sl_code_col].str.upper()

    # Budget Value (grouped by Area)
    budget_grouped_value = (budget_df.groupby(budget_area_col)
                            .agg({budget_value_col: 'sum'})
                            .reset_index()
                            .rename(columns={budget_area_col: 'Area', budget_value_col: 'Budget Value'}))

    # Budget Quantity (grouped by Area)
    budget_grouped_qty = (budget_df.groupby(budget_area_col)
                          .agg({budget_qty_col: 'sum'})
                          .reset_index()
                          .rename(columns={budget_area_col: 'Area', budget_qty_col: 'Budget Qty'}))

    # Budget vs Billed Value: Matching on Product Group and SL Code
    budget_pairs = budget_df[[budget_area_col, budget_product_group_col, budget_sl_code_col]].drop_duplicates()
    budget_pairs = budget_pairs.rename(columns={budget_area_col: 'Area', budget_product_group_col: 'Product Group', budget_sl_code_col: 'SL Code'})
    filtered_sales_value = pd.merge(filtered_sales_df, budget_pairs, 
                                    left_on=[sales_area_col, sales_product_group_col, sales_sl_code_col], 
                                    right_on=['Area', 'Product Group', 'SL Code'], 
                                    how='inner')
    sales_grouped_value = (filtered_sales_value.groupby(sales_area_col)
                           .agg({sales_value_col: 'sum'})
                           .reset_index()
                           .rename(columns={sales_area_col: 'Area', sales_value_col: 'Billed Value'}))

    # Budget vs Billed Quantity: Matching on Product Group and SL Code
    filtered_sales_qty = pd.merge(filtered_sales_df, budget_pairs, 
                                  left_on=[sales_area_col, sales_product_group_col, sales_sl_code_col], 
                                  right_on=['Area', 'Product Group', 'SL Code'], 
                                  how='inner')
    sales_grouped_qty = (filtered_sales_qty.groupby(sales_area_col)
                         .agg({sales_qty_col: 'sum'})
                         .reset_index()
                         .rename(columns={sales_area_col: 'Area', sales_qty_col: 'Billed Qty'}))

    # Overall Sales (Quantity): No matching
    sales_grouped_overall_qty = (filtered_sales_df.groupby(sales_area_col)
                                 .agg({sales_qty_col: 'sum'})
                                 .reset_index()
                                 .rename(columns={sales_area_col: 'Area', sales_qty_col: 'Billed Qty'}))

    # Overall Sales Invalue: No matching
    sales_grouped_overall_value = (filtered_sales_df.groupby(sales_area_col)
                                   .agg({sales_value_col: 'sum'})
                                   .reset_index()
                                   .rename(columns={sales_area_col: 'Area', sales_value_col: 'Billed Value'}))

    # Use all branches if no branches selected or all selected
    default_branches = selected_branches if selected_branches else all_branches

    # Budget vs Billed Value DataFrame (Slide 2)
    budget_vs_billed_value_df = pd.DataFrame({'Area': default_branches})
    budget_vs_billed_value_df = pd.merge(budget_vs_billed_value_df, budget_grouped_value, on='Area', how='left').fillna({'Budget Value': 0})
    budget_vs_billed_value_df = pd.merge(budget_vs_billed_value_df, sales_grouped_value, on='Area', how='left').fillna({'Billed Value': 0})
    # Apply logic: If Billed Value <= Budget Value, keep Billed Value; else, use Budget Value
    budget_vs_billed_value_df['Billed Value'] = budget_vs_billed_value_df.apply(
        lambda row: row['Billed Value'] if row['Billed Value'] <= row['Budget Value'] else row['Budget Value'], axis=1)
    budget_vs_billed_value_df['%'] = budget_vs_billed_value_df.apply(
        lambda row: int((row['Billed Value'] / row['Budget Value'] * 100)) if row['Budget Value'] != 0 else 0, axis=1)
    total_budget = budget_vs_billed_value_df['Budget Value'].sum()
    total_billed = budget_vs_billed_value_df['Billed Value'].sum()
    total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
    total_row = pd.DataFrame({'Area': ['TOTAL'], 'Budget Value': [total_budget], 'Billed Value': [total_billed], '%': [total_percentage]})
    budget_vs_billed_value_df = pd.concat([budget_vs_billed_value_df, total_row], ignore_index=True)
    budget_vs_billed_value_df['Budget Value'] = budget_vs_billed_value_df['Budget Value'].round(0).astype(int)
    budget_vs_billed_value_df['Billed Value'] = budget_vs_billed_value_df['Billed Value'].round(0).astype(int)

    # Budget vs Billed Quantity DataFrame (Slide 3)
    budget_vs_billed_qty_df = pd.DataFrame({'Area': default_branches})
    budget_vs_billed_qty_df = pd.merge(budget_vs_billed_qty_df, budget_grouped_qty, on='Area', how='left').fillna({'Budget Qty': 0})
    budget_vs_billed_qty_df = pd.merge(budget_vs_billed_qty_df, sales_grouped_qty, on='Area', how='left').fillna({'Billed Qty': 0})
    # Apply logic: If Billed Qty <= Budget Qty, keep Billed Qty; else, use Budget Qty
    budget_vs_billed_qty_df['Billed Qty'] = budget_vs_billed_qty_df.apply(
        lambda row: row['Billed Qty'] if row['Billed Qty'] <= row['Budget Qty'] else row['Budget Qty'], axis=1)
    budget_vs_billed_qty_df['%'] = budget_vs_billed_qty_df.apply(
        lambda row: int((row['Billed Qty'] / row['Budget Qty'] * 100)) if row['Budget Qty'] != 0 else 0, axis=1)
    total_budget = budget_vs_billed_qty_df['Budget Qty'].sum()
    total_billed = budget_vs_billed_qty_df['Billed Qty'].sum()
    total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
    total_row = pd.DataFrame({'Area': ['TOTAL'], 'Budget Qty': [total_budget], 'Billed Qty': [total_billed], '%': [total_percentage]})
    budget_vs_billed_qty_df = pd.concat([budget_vs_billed_qty_df, total_row], ignore_index=True)
    budget_vs_billed_qty_df['Budget Qty'] = budget_vs_billed_qty_df['Budget Qty'].round(0).astype(int)
    budget_vs_billed_qty_df['Billed Qty'] = budget_vs_billed_qty_df['Billed Qty'].round(0).astype(int)

    # Overall Sales DataFrame (Quantity) - Slide 4 (unchanged)
    overall_sales_qty_df = pd.DataFrame({'Area': default_branches})
    overall_sales_qty_df = pd.merge(overall_sales_qty_df, budget_grouped_qty, on='Area', how='left').fillna({'Budget Qty': 0})
    overall_sales_qty_df = pd.merge(overall_sales_qty_df, sales_grouped_overall_qty, on='Area', how='left').fillna({'Billed Qty': 0})
    overall_sales_qty_df['%'] = overall_sales_qty_df.apply(
        lambda row: int((row['Billed Qty'] / row['Budget Qty'] * 100)) if row['Budget Qty'] != 0 else 0, axis=1)
    total_budget = overall_sales_qty_df['Budget Qty'].sum()
    total_billed = overall_sales_qty_df['Billed Qty'].sum()
    total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
    total_row = pd.DataFrame({'Area': ['TOTAL'], 'Budget Qty': [total_budget], 'Billed Qty': [total_billed], '%': [total_percentage]})
    overall_sales_qty_df = pd.concat([overall_sales_qty_df, total_row], ignore_index=True)
    overall_sales_qty_df['Budget Qty'] = overall_sales_qty_df['Budget Qty'].round(0).astype(int)
    overall_sales_qty_df['Billed Qty'] = overall_sales_qty_df['Billed Qty'].round(0).astype(int)

    # Overall Sales Invalue DataFrame (Value) - Slide 5 (unchanged)
    overall_sales_value_df = pd.DataFrame({'Area': default_branches})
    overall_sales_value_df = pd.merge(overall_sales_value_df, budget_grouped_value, on='Area', how='left').fillna({'Budget Value': 0})
    overall_sales_value_df = pd.merge(overall_sales_value_df, sales_grouped_overall_value, on='Area', how='left').fillna({'Billed Value': 0})
    overall_sales_value_df['%'] = overall_sales_value_df.apply(
        lambda row: int((row['Billed Value'] / row['Budget Value'] * 100)) if row['Budget Value'] != 0 else 0, axis=1)
    total_budget = overall_sales_value_df['Budget Value'].sum()
    total_billed = overall_sales_value_df['Billed Value'].sum()
    total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
    total_row = pd.DataFrame({'Area': ['TOTAL'], 'Budget Value': [total_budget], 'Billed Value': [total_billed], '%': [total_percentage]})
    overall_sales_value_df = pd.concat([overall_sales_value_df, total_row], ignore_index=True)
    overall_sales_value_df['Budget Value'] = overall_sales_value_df['Budget Value'].round(0).astype(int)
    overall_sales_value_df['Billed Value'] = overall_sales_value_df['Billed Value'].round(0).astype(int)

    return budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df

def create_budget_ppt(budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df, month_title=None, logo_file=None):
    """Create PPT presentation with budget vs billed data"""
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)

        # Page 1: Title Slide (using blank layout to avoid placeholders)
        blank_slide_layout = prs.slide_layouts[6]  # Blank slide layout
        title_slide = prs.slides.add_slide(blank_slide_layout)

        # Company Name at the Top
        company_name = title_slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(12.33), Inches(1))
        company_frame = company_name.text_frame
        company_frame.text = "Asia Crystal Commodity LLP"
        p = company_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        p.font.name = "Times New Roman"
        p.font.size = Pt(36)
        p.font.bold = True
        p.font.color.rgb = RGBColor(0, 112, 192)

        # Add Logo if uploaded
        if logo_file is not None:
            try:
                logo_buffer = BytesIO(logo_file.read())
                logo = title_slide.shapes.add_picture(logo_buffer, Inches(5.665), Inches(1.5), width=Inches(2), height=Inches(2))
                # Reset file position
                logo_file.seek(0)
            except Exception as e:
                logger.error(f"Error adding logo: {e}")

        # Title
        title = title_slide.shapes.add_textbox(Inches(0.5), Inches(4.0), Inches(12.33), Inches(1))
        title_frame = title.text_frame
        title_frame.text = f"Monthly Review Meeting – {month_title}"
        p = title_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        p.font.name = "Times New Roman"
        p.font.size = Pt(32)
        p.font.bold = True
        p.font.color.rgb = RGBColor(0, 128, 0)

        # Subtitle
        subtitle = title_slide.shapes.add_textbox(Inches(0.5), Inches(5.0), Inches(12.33), Inches(1))
        subtitle_frame = subtitle.text_frame
        subtitle_frame.text = "ACCLLP"
        p = subtitle_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        p.font.name = "Times New Roman"
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = RGBColor(0, 112, 192)

        # Helper function to add centered table slides
        def add_table_slide(title_text, df):
            slide = prs.slides.add_slide(blank_slide_layout)
            
            # Add Title
            title_shape = slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(12.33), Inches(0.75))
            title_text_frame = title_shape.text_frame
            title_text_frame.text = title_text
            p = title_text_frame.paragraphs[0]
            p.alignment = PP_ALIGN.CENTER
            p.font.size = Pt(24)
            p.font.bold = True
            p.font.color.rgb = RGBColor(0, 112, 192)

            # Add Table (Centered)
            table_width = Inches(9)
            table_height = Inches(3)
            left = Inches(2.165)  # (13.33 - 9) / 2 to center the table
            top = Inches(1.5)
            rows, cols = df.shape[0] + 1, df.shape[1]
            table = slide.shapes.add_table(rows, cols, left, top, table_width, table_height).table

            # Set column headers
            for col_idx, col_name in enumerate(df.columns):
                cell = table.cell(0, col_idx)
                cell.text = str(col_name)
                cell.text_frame.paragraphs[0].font.size = Pt(14)
                cell.text_frame.paragraphs[0].font.bold = True
                cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
                cell.vertical_anchor = MSO_VERTICAL_ANCHOR.MIDDLE
                fill = cell.fill
                fill.solid()
                fill.fore_color.rgb = RGBColor(0, 112, 192)
                cell.text_frame.paragraphs[0].runs[0].font.color.rgb = RGBColor(255, 255, 255)

            # Populate table data
            total_row_idx = df.shape[0] - 1
            for row_idx in range(df.shape[0]):
                is_total_row = row_idx == total_row_idx
                for col_idx in range(df.shape[1]):
                    cell = table.cell(row_idx + 1, col_idx)
                    value = df.iloc[row_idx, col_idx]
                    cell.text = f"{value}%" if col_idx == 3 else str(value)
                    cell.text_frame.paragraphs[0].font.size = Pt(12)
                    cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
                    cell.vertical_anchor = MSO_VERTICAL_ANCHOR.MIDDLE
                    if is_total_row:
                        cell.text_frame.paragraphs[0].font.bold = True
                        fill = cell.fill
                        fill.solid()
                        fill.fore_color.rgb = RGBColor(211, 211, 211)

        # Page 2: Budget Against Billed Quantity
        add_table_slide("BUDGET AGAINST BILLED (Qty in Mt)", budget_vs_billed_qty_df)

        # Page 3: Budget Against Billed Value
        add_table_slide("BUDGET AGAINST BILLED (Value in Lakhs)", budget_vs_billed_value_df)

        # Page 4: Overall Sales (Quantity)
        add_table_slide("OVERALL SALES (Qty in Mt)", overall_sales_qty_df)

        # Page 5: Overall Sales Invalue
        add_table_slide("OVERALL SALES (Value in Lakhs)", overall_sales_value_df)

        ppt_buffer = BytesIO()
        prs.save(ppt_buffer)
        ppt_buffer.seek(0)
        return ppt_buffer
    except Exception as e:
        logger.error(f"Error creating PPT: {e}")
        st.error(f"Error creating PPT: {e}")
        return None
def create_budget_table_image(df, title):
    """Create a table image for the budget vs billed report"""
    fig, ax = plt.subplots(figsize=(10, len(df) * 0.5))
    ax.axis('off')
    table = Table(ax, bbox=[0, 0, 1, 1])
    nrows, ncols = df.shape
    width, height = 1.0 / ncols, 1.0 / nrows
    for col_idx, col_name in enumerate(df.columns):
        table.add_cell(0, col_idx, width, height, text=str(col_name), loc='center', facecolor='#0070C0')
        table[0, col_idx].set_text_props(weight='bold', color='white')
    for row_idx in range(nrows):
        for col_idx in range(ncols):
            value = df.iloc[row_idx, col_idx]
            text = f"{value}%" if col_idx == 3 else str(value)
            facecolor = '#f0f0f0' if row_idx % 2 == 0 else 'white'
            if row_idx == nrows - 1:
                facecolor = '#D3D3D3'
                table.add_cell(row_idx + 1, col_idx, width, height, text=text, loc='center', facecolor=facecolor).set_text_props(weight='bold')
            else:
                table.add_cell(row_idx + 1, col_idx, width, height, text=text, loc='center', facecolor=facecolor)
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    ax.add_table(table)
    plt.title(title, fontsize=16, weight='bold', color='#0070C0', pad=20)
    plt.tight_layout()
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=100)
    img_buffer.seek(0)
    plt.close()
    return img_buffer

def tab_budget_vs_billed():
    """Implements the Budget vs Billed Report Tab."""
    st.header("Budget vs Billed Report")
    
    dfs_info = []
    
    sales_file = st.session_state.sales_file
    budget_file = st.session_state.budget_file
    
    if not sales_file or not budget_file:
        st.warning("Please upload both Sales and Budget files in the sidebar")
        return dfs_info
    
    try:
        # Get sheet names
        sales_xl = pd.ExcelFile(sales_file)
        budget_xl = pd.ExcelFile(budget_file)
        sales_sheets = sales_xl.sheet_names
        budget_sheets = budget_xl.sheet_names

        # Sheet selection
        st.subheader("Sheet Selection")
        col1, col2 = st.columns(2)
        with col1:
            sales_sheet = st.selectbox("Sales Sheet Name", sales_sheets, index=sales_sheets.index('24-25') if '24-25' in sales_sheets else 0, key='budget_sales_sheet')
        with col2:
            budget_sheet = st.selectbox("Budget Sheet Name", budget_sheets, index=budget_sheets.index('Consolidate') if 'Consolidate' in budget_sheets else 0, key='budget_budget_sheet')

        # Row selection
        st.subheader("Header Row Selection")
        col1, col2 = st.columns(2)
        with col1:
            sales_header_row = st.number_input("Sales Header Row (1-based)", min_value=1, value=1, step=1, key='budget_sales_header') - 1
        with col2:
            budget_header_row = st.number_input("Budget Header Row (1-based)", min_value=1, value=2, step=1, key='budget_budget_header') - 1

        # Load data with selected sheets and header rows, enforce SL Code as string
        sales_df = pd.read_excel(sales_file, sheet_name=sales_sheet, header=sales_header_row, dtype={'SL Code': str})
        budget_df = pd.read_excel(budget_file, sheet_name=budget_sheet, header=budget_header_row, dtype={'SL Code': str})
        
        # Get available columns
        sales_columns = sales_df.columns.tolist()
        budget_columns = budget_df.columns.tolist()

        # Sales Column Mappings
        st.subheader("Sales Column Mapping")
        col1, col2, col3 = st.columns(3)
        with col1:
            sales_date_col = st.selectbox("Sales Date Column", sales_columns, key='budget_sales_date')
            sales_area_col = st.selectbox("Sales Area Column", sales_columns, key='budget_sales_area')
        with col2:
            sales_value_col = st.selectbox("Sales Value Column", sales_columns, key='budget_sales_value')
            sales_qty_col = st.selectbox("Sales Quantity Column", sales_columns, key='budget_sales_qty')
        with col3:
            sales_product_group_col = st.selectbox("Sales Product Group Column", sales_columns, key='budget_sales_product')
            sales_sl_code_col = st.selectbox("Sales SL Code Column", sales_columns, key='budget_sales_sl_code')
        
        sales_exec_col = st.selectbox("Sales Executive Column", sales_columns, key='budget_sales_exec')

        # Budget Column Mappings
        st.subheader("Budget Column Mapping")
        col1, col2, col3 = st.columns(3)
        with col1:
            budget_area_col = st.selectbox("Budget Area Column", budget_columns, key='budget_budget_area')
            budget_value_col = st.selectbox("Budget Value Column", budget_columns, key='budget_budget_value')
        with col2:
            budget_qty_col = st.selectbox("Budget Quantity Column", budget_columns, key='budget_budget_qty')
            budget_product_group_col = st.selectbox("Budget Product Group Column", budget_columns, key='budget_budget_product')
        with col3:
            budget_sl_code_col = st.selectbox("Budget SL Code Column", budget_columns, key='budget_budget_sl_code')
            budget_exec_col = st.selectbox("Budget Executive Column", budget_columns, key='budget_budget_exec')

        # Initialize session state for checkboxes
        if 'budget_sales_exec_all' not in st.session_state:
            st.session_state.budget_sales_exec_all = True
        if 'budget_budget_exec_all' not in st.session_state:
            st.session_state.budget_budget_exec_all = True
        if 'budget_branch_all' not in st.session_state:
            st.session_state.budget_branch_all = True

        # Executive selection with checkboxes
        st.subheader("Executive Selection")
        sales_executives = sorted(sales_df[sales_exec_col].dropna().unique().tolist())
        budget_executives = sorted(budget_df[budget_exec_col].dropna().unique().tolist())
        
        if not sales_executives or not budget_executives:
            st.error("No executives found in selected Executive columns.")
            return dfs_info

        col1, col2 = st.columns(2)
        with col1:
            st.write("**Sales Executives**")
            sales_select_all = st.checkbox("Select All Sales Executives", value=st.session_state.budget_sales_exec_all, key='budget_sales_exec_all')
            selected_sales_execs = []
            if sales_select_all != st.session_state.budget_sales_exec_all:
                st.session_state.budget_sales_exec_all = sales_select_all
                for exec in sales_executives:
                    st.session_state[f'budget_sales_exec_{exec}'] = sales_select_all
            for exec in sales_executives:
                if sales_select_all:
                    st.session_state[f'budget_sales_exec_{exec}'] = True
                if st.checkbox(exec, value=st.session_state.get(f'budget_sales_exec_{exec}', True), key=f'budget_sales_exec_{exec}'):
                    selected_sales_execs.append(exec)
                if not st.session_state.get(f'budget_sales_exec_{exec}', True) and st.session_state.budget_sales_exec_all:
                    st.session_state.budget_sales_exec_all = False
        
        with col2:
            st.write("**Budget Executives**")
            budget_select_all = st.checkbox("Select All Budget Executives", value=st.session_state.budget_budget_exec_all, key='budget_budget_exec_all')
            selected_budget_execs = []
            if budget_select_all != st.session_state.budget_budget_exec_all:
                st.session_state.budget_budget_exec_all = budget_select_all
                for exec in budget_executives:
                    st.session_state[f'budget_budget_exec_{exec}'] = budget_select_all
            for exec in budget_executives:
                if budget_select_all:
                    st.session_state[f'budget_budget_exec_{exec}'] = True
                if st.checkbox(exec, value=st.session_state.get(f'budget_budget_exec_{exec}', True), key=f'budget_budget_exec_{exec}'):
                    selected_budget_execs.append(exec)
                if not st.session_state.get(f'budget_budget_exec_{exec}', True) and st.session_state.budget_budget_exec_all:
                    st.session_state.budget_budget_exec_all = False
                    # Branch selection with checkboxes
        all_branches = pd.concat([sales_df[sales_area_col], budget_df[budget_area_col]]).dropna().str.upper().str.split(' - ').str[-1].unique().tolist()
        all_branches = sorted(set([branch_mapping.get(branch, branch) for branch in all_branches]))
        
        st.subheader("Branch Selection")
        branch_select_all = st.checkbox("Select All Branches", value=st.session_state.budget_branch_all, key='budget_branch_all')
        selected_branches = []
        if branch_select_all != st.session_state.budget_branch_all:
            st.session_state.budget_branch_all = branch_select_all
            for branch in all_branches:
                st.session_state[f'budget_branch_{branch}'] = branch_select_all
        
        # Create a multi-column layout for branch selection to save space
        branch_cols = st.columns(4)  # Adjust the number of columns based on how many branches there are
        
        for i, branch in enumerate(all_branches):
            col_idx = i % 4  # Distribute branches across columns
            with branch_cols[col_idx]:
                if branch_select_all:
                    st.session_state[f'budget_branch_{branch}'] = True
                if st.checkbox(branch, value=st.session_state.get(f'budget_branch_{branch}', True), key=f'budget_branch_{branch}'):
                    selected_branches.append(branch)
                if not st.session_state.get(f'budget_branch_{branch}', True) and st.session_state.budget_branch_all:
                    st.session_state.budget_branch_all = False

        # Month selection
        months = pd.to_datetime(sales_df[sales_date_col], dayfirst=True, errors='coerce').dt.strftime('%b %y').dropna().unique()
        if len(months) == 0:
            st.error(f"No valid months found in '{sales_date_col}'. Check date format.")
            return dfs_info
            
        selected_month = st.selectbox("Select Month", months, index=0, key='budget_month')
        
        # Calculate button
        if st.button("Calculate Budget vs Billed", key='budget_calculate'):
            # Use the fixed calculation function
            budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df = calculate_values(
                sales_df, budget_df, selected_month, selected_sales_execs, selected_budget_execs,
                sales_date_col, sales_area_col, sales_value_col, sales_qty_col, sales_product_group_col, sales_sl_code_col, sales_exec_col,
                budget_area_col, budget_value_col, budget_qty_col, budget_product_group_col, budget_sl_code_col, budget_exec_col,
                selected_branches
            )
            
            if all(df is not None for df in [budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df]):
                st.subheader("Results")
                
                # Display results in tabs for cleaner UI
                result_tabs = st.tabs(["Budget vs Billed Quantity", "Budget vs Billed Value", "Overall Sales (Qty)", "Overall Sales (Value)"])
                
                with result_tabs[0]:
                    st.write("**Budget vs Billed Quantity**")
                    st.dataframe(budget_vs_billed_qty_df)
                    img_buffer = create_budget_table_image(budget_vs_billed_qty_df, f"BUDGET AGAINST BILLED (Qty in Mt) - {selected_month}")
                    st.image(img_buffer, use_column_width=True)
        
                
                with result_tabs[1]:
                    st.write("**Budget vs Billed Value**")
                    st.dataframe(budget_vs_billed_value_df)
                    img_buffer = create_budget_table_image(budget_vs_billed_value_df, f"BUDGET AGAINST BILLED (Value in Lakhs) - {selected_month}")
                    st.image(img_buffer, use_column_width=True)
                    
                
                with result_tabs[2]:
                    st.write("**Overall Sales Quantity**")
                    st.dataframe(overall_sales_qty_df)
                    img_buffer = create_budget_table_image(overall_sales_qty_df, f"OVERALL SALES (Qty In Mt) - {selected_month}")
                    st.image(img_buffer, use_column_width=True)
                
                with result_tabs[3]:
                    st.write("**Overall Sales Value**")
                    st.dataframe(overall_sales_value_df)
                    img_buffer = create_budget_table_image(overall_sales_value_df, f"OVERALL SALES (Value in Lakhs) - {selected_month}")
                    st.image(img_buffer, use_column_width=True)
                
                # Create individual PPT
                logo_file = st.session_state.logo_file
                ppt_buffer = create_budget_ppt(    
                    budget_vs_billed_qty_df,
                    budget_vs_billed_value_df, 
                    overall_sales_qty_df, 
                    overall_sales_value_df, 
                    month_title=selected_month, 
                    logo_file=logo_file
                )
                
                if ppt_buffer:
                    st.download_button(
                        label="Download Budget vs Billed PPT",
                        data=ppt_buffer,
                        file_name=f"Budget_vs_Billed_{selected_month}.pptx",
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key='budget_download'
                    )
                
                # Prepare data for consolidated PPT
                dfs_info = [
                    {'df': budget_vs_billed_qty_df, 'title': f"BUDGET AGAINST BILLED QUANTITY (Qty in Mt) - {selected_month}", 'percent_cols': [3]},
                    {'df': budget_vs_billed_value_df, 'title': f"BUDGET AGAINST BILLED (Value in Lakhs) - {selected_month}", 'percent_cols': [3]},
                    {'df': overall_sales_qty_df, 'title': f"OVERALL SALES (Qty In Mt) - {selected_month}", 'percent_cols': [3]},
                    {'df': overall_sales_value_df, 'title': f"OVERALL SALES (Value in Lakhs) - {selected_month}", 'percent_cols': [3]}
                ]
                
                # Store results in session state
                st.session_state.budget_results = dfs_info
            else:
                st.error("Failed to calculate values. Check your data and selections.")
    except Exception as e:
        st.error(f"Error in Budget vs Billed tab: {e}")
        logger.error(f"Error in Budget vs Billed tab: {e}")
    
    return dfs_info
#######################################################
# MODULE 2: NUMBER OF BILLED CUSTOMERS
#######################################################

# This implementation fixes both Number of Billed Customers and OD Target together

# First, keep the NBC-specific branch mapping
nbc_branch_mapping = {
    'PONDY': 'PONDY', 'PDY': 'PONDY',
    'COVAI': 'ERODE_CBE_KRR',  # Represents COIMBATORE
    'ERODE': 'ERODE_CBE_KRR',
    'KARUR': 'ERODE_CBE_KRR',
    'SLM002': 'SALEM', 'SLMTD1': 'SALEM',
    'BHV1': 'BHAVANI',
    'MDU': 'MADURAI',
    'CHENNAI': 'CHENNAI', 'CHN': 'CHENNAI',
    'TIRUPUR': 'TIRUPUR', 'TPR': 'TIRUPUR',
    'POULTRY': 'POULTRY'
}

# NBC functions
def determine_financial_year(date):
    """Determine the financial year for a given date (April to March)."""
    year = date.year
    month = date.month
    if month >= 4:
        return f"{year % 100}-{year % 100 + 1}"  # e.g., April 2024 -> 24-25
    else:
        return f"{year % 100 - 1}-{year % 100}"  # e.g., February 2025 -> 24-25

def create_customer_table(sales_df, date_col, branch_col, customer_id_col, executive_col, selected_branches=None, selected_executives=None):
    """Create a table of unique customer counts per branch and month for available data."""
    sales_df = sales_df.copy()

    # Validate columns
    for col in [date_col, branch_col, customer_id_col, executive_col]:
        if col not in sales_df.columns:
            st.error(f"Column '{col}' not found in sales data.")
            return None

    # Convert date column to datetime with error handling
    try:
        sales_df[date_col] = pd.to_datetime(sales_df[date_col], errors='coerce')
    except Exception as e:
        st.error(f"Error converting '{date_col}' to datetime: {e}. Ensure dates are in a valid format.")
        return None

    valid_dates = sales_df[date_col].notna()
    if not valid_dates.any():
        st.error(f"Column '{date_col}' contains no valid dates.")
        return None

    # Extract financial year
    sales_df['Financial_Year'] = sales_df[date_col].apply(determine_financial_year)
    
    # Get list of available financial years
    available_financial_years = sorted(sales_df['Financial_Year'].unique())
    
    # Filter out invalid dates
    sales_df = sales_df[sales_df[date_col].notna()].copy()
    if sales_df.empty:
        st.error("No valid dates found in data after filtering.")
        return None

    # Extract month and year
    sales_df['Month'] = sales_df[date_col].dt.month
    sales_df['Year'] = sales_df[date_col].dt.year
    
    # Create Month-Year column (e.g., "APR-2024")
    sales_df['Month_Name'] = sales_df[date_col].dt.strftime('%b-%Y').str.upper()
    
    # Standardize branch names with error handling
    try:
        sales_df['Raw_Branch'] = sales_df[branch_col].astype(str).str.upper()
    except Exception as e:
        st.error(f"Error processing branch column '{branch_col}': {e}.")
        return None
    sales_df['Mapped_Branch'] = sales_df['Raw_Branch'].replace(nbc_branch_mapping)

    # Apply branch filter if selected
    if selected_branches:
        sales_df = sales_df[sales_df['Mapped_Branch'].isin(selected_branches)]
        if sales_df.empty:
            st.error("No data matches the selected branches.")
            return None

    # Apply executive filter if selected
    if selected_executives:
        sales_df = sales_df[sales_df[executive_col].isin(selected_executives)]
        if sales_df.empty:
            st.error("No data matches the selected executives.")
            return None

    # Create result dictionary to store tables for each financial year
    result_dict = {}
    
    # Process each financial year
    for fy in available_financial_years:
        fy_df = sales_df[sales_df['Financial_Year'] == fy].copy()
        
        if fy_df.empty:
            continue
        
        # Get available months for this financial year in chronological order
        fy_df['Date_Sort'] = fy_df[date_col]  # For sorting months chronologically
        available_months = fy_df.sort_values('Date_Sort')['Month_Name'].unique()
        
        # Determine branches to display based on selection
        if selected_branches:
            branches_to_display = selected_branches
        else:
            branches_to_display = sorted(fy_df['Mapped_Branch'].dropna().unique())
        
        # Calculate unique customer counts using customer_id_col
        grouped_df = fy_df.groupby(['Raw_Branch', 'Month_Name'])[customer_id_col].nunique().reset_index(name='Count')
        grouped_df['Mapped_Branch'] = grouped_df['Raw_Branch'].replace(nbc_branch_mapping)
        
        # Create pivot table
        pivot_df = grouped_df.pivot_table(
            values='Count',
            index='Mapped_Branch',
            columns='Month_Name',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Initialize result DataFrame with selected or all branches
        result_df = pd.DataFrame({'Branch Name': branches_to_display})
        result_df = pd.merge(result_df, pivot_df, left_on='Branch Name', right_on='Mapped_Branch', how='left').fillna(0)
        result_df = result_df.drop(columns=['Mapped_Branch'] if 'Mapped_Branch' in result_df.columns else [])
        
        # Ensure all available months are present
        for month in available_months:
            if month not in result_df.columns:
                result_df[month] = 0
        
        # Reorder columns
        result_df = result_df[['Branch Name'] + [month for month in available_months if month in result_df.columns]]
        
        # Add S.No column
        result_df.insert(0, 'S.No', range(1, len(result_df) + 1))
        
        # Add GRAND TOTAL row
        total_row = {'S.No': '', 'Branch Name': 'GRAND TOTAL'}
        for month in available_months:
            if month in result_df.columns:
                total_row[month] = result_df[month].sum()
        result_df = pd.concat([result_df, pd.DataFrame([total_row])], ignore_index=True)
        
        # Convert to integers
        for col in available_months:
            if col in result_df.columns:
                result_df[col] = result_df[col].round(0).astype(int)
        
        # Store result for this financial year
        result_dict[fy] = (result_df, list(available_months))
    
    return result_dict

def create_customer_table_image(df, title, sorted_months, financial_year):
    """Create a table image from the DataFrame."""
    fig, ax = plt.subplots(figsize=(14, len(df) * 0.6))
    ax.axis('off')
    columns = ['S.No', 'Branch Name'] + sorted_months
    rows = len(df)
    ncols = len(columns)
    width = 1.0 / ncols
    height = 1.0 / rows
    table = Table(ax, bbox=[0, 0, 1, 1])
    for col_idx, col_name in enumerate(columns):
        table.add_cell(0, col_idx, width, height, text=col_name, loc='center', facecolor='#0070C0')
        table[0, col_idx].set_text_props(weight='bold', color='white', fontsize=10)
    for row_idx in range(rows):
        for col_idx in range(ncols):
            value = df.iloc[row_idx, col_idx]
            text = str(value)
            facecolor = '#DDEBF7' if row_idx % 2 == 0 else 'white'
            if row_idx == rows - 1:  # GRAND TOTAL
                facecolor = '#D3D3D3'
                table.add_cell(row_idx + 1, col_idx, width, height, text=text, loc='center', facecolor=facecolor).set_text_props(weight='bold', fontsize=10)
            else:
                table.add_cell(row_idx + 1, col_idx, width, height, text=text, loc='center', facecolor=facecolor).set_text_props(fontsize=10)
    table[(0, 0)].width = 0.05
    table[(0, 1)].width = 0.15
    for col_idx in range(2, ncols):
        table[(0, col_idx)].width = 0.08
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    ax.add_table(table)
    plt.suptitle(f"{title} - FY {financial_year}", fontsize=14, weight='bold', color='#0070C0', y=1.02)
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=150)
    plt.close()
    return img_buffer

def create_customer_ppt_slide(slide, df, title, sorted_months, financial_year):
    """Add a slide with the billed customers table to the presentation."""
    title_shape = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(12.33), Inches(0.8))
    title_frame = title_shape.text_frame
    title_frame.text = f"{title} - FY {financial_year}"
    p = title_frame.paragraphs[0]
    p.font.size = Pt(24)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 112, 192)
    p.alignment = PP_ALIGN.CENTER
    columns = ['S.No', 'Branch Name'] + sorted_months
    rows = len(df) + 1
    ncols = len(columns)
    table_width = Inches(12.0)
    table_height = Inches(0.3 * len(df) + 0.4)
    left = Inches(0.65)
    top = Inches(1.2)
    table = slide.shapes.add_table(rows, ncols, left, top, table_width, table_height).table
    col_widths = [Inches(0.5), Inches(2.0)] + [Inches(0.75)] * len(sorted_months)
    for col_idx in range(ncols):
        table.columns[col_idx].width = col_widths[col_idx]
    for col_idx, col_name in enumerate(columns):
        cell = table.cell(0, col_idx)
        cell.text = col_name
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0, 112, 192)
        cell.text_frame.paragraphs[0].font.size = Pt(12)
        cell.text_frame.paragraphs[0].font.bold = True
        cell.text_frame.paragraphs[0].font.color.rgb = RGBColor(255, 255, 255)
        cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
    total_row_idx = len(df) - 1
    for row_idx in range(len(df)):
        is_total_row = row_idx == total_row_idx
        for col_idx in range(ncols):
            cell = table.cell(row_idx + 1, col_idx)
            value = df.iloc[row_idx, col_idx]
            cell.text = str(value)
            cell.text_frame.paragraphs[0].font.size = Pt(12)
            cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            cell.fill.solid()
            if is_total_row:
                cell.fill.fore_color.rgb = RGBColor(211, 211, 211)
                cell.text_frame.paragraphs[0].font.bold = True
            else:
                if row_idx % 2 == 0:
                    cell.fill.fore_color.rgb = RGBColor(221, 235, 247)
                else:
                    cell.fill.fore_color.rgb = RGBColor(255, 255, 255)

# Example of how to use this in a Streamlit app
def display_customer_tables(sales_df, date_col, branch_col, customer_id_col, executive_col, selected_branches=None, selected_executives=None):
    # Get customer tables for all available financial years
    result_dict = create_customer_table(sales_df, date_col, branch_col, customer_id_col, executive_col, 
                                        selected_branches, selected_executives)
    
    if not result_dict:
        st.error("No data available to display customer tables.")
        return
    
    # Create financial year selector
    available_fy = list(result_dict.keys())
    if not available_fy:
        st.error("No financial years found in the data.")
        return
    
    selected_fy = st.selectbox("Select Financial Year", available_fy)
    
    if selected_fy in result_dict:
        df, months = result_dict[selected_fy]
        
        # Display the table
        st.subheader(f"Branch-wise Unique Customer Count - FY {selected_fy}")
        st.dataframe(df)
        
        # Create and display table image
        img_buffer = create_customer_table_image(df, "Branch-wise Unique Customer Count", months, selected_fy)
        img_buffer.seek(0)
        st.image(img_buffer, use_column_width=True)
        
        # Provide download options
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        st.download_button(
            label="Download Table as Excel",
            data=excel_buffer,
            file_name=f"customer_table_FY{selected_fy}.xlsx",
            mime="application/vnd.ms-excel"
        )
# OD Target Functions (from nbc.py)
def extract_area_name(area):
    """Extract and standardize branch names."""
    if pd.isna(area) or str(area).strip() == '':
        return None  # Return None for empty/null values
    
    area = str(area).strip().upper()
    if area == 'HO' or area.endswith('-HO'):
        return None
    
    # Standardize branch variations
    branch_variations = {
        'PUDUCHERRY': ['PUDUCHERRY', 'PONDY', 'PUDUCHERRY - PONDY', 'PONDICHERRY', 'PUDUCHERI', 'aaaa - PUDUCHERRY', 'AAAA - PUDUCHERRY'],
        'COIMBATORE': ['COIMBATORE', 'CBE', 'COIMBATORE - CBE', 'COIMBATURE', 'aaaa - COIMBATORE', 'AAAA - COIMBATORE'],
        'KARUR': ['KARUR', 'KRR', 'KARUR - KRR', 'aaaa - KARUR', 'AAAA - KARUR'],
        'MADURAI': ['MADURAI', 'MDU', 'MADURAI - MDU', 'MADURA', 'aaaa - MADURAI', 'AAAA - MADURAI'],
        'CHENNAI': ['CHENNAI', 'CHN', 'aaaa - CHENNAI', 'AAAA - CHENNAI'],
    }
    
    for standard_name, variations in branch_variations.items():
        for variation in variations:
            if variation in area:
                return standard_name
    
    # Handle prefixes (case-insensitive)
    prefixes = ['AAAA - ', 'aaaa - ', 'BBB - ', 'bbb - ', 'ASIA CRYSTAL COMMODITY LLP - ']
    for prefix in prefixes:
        if area.startswith(prefix.upper()):
            return area[len(prefix):].strip()
    
    # Handle separators
    separators = [' - ', '-', ':']
    for sep in separators:
        if sep in area:
            return area.split(sep)[-1].strip()
    
    return area  # Fallback to original name if no transformations apply

def extract_executive_name(executive):
    """Normalize executive names, treating null/empty as 'BLANK'."""
    if pd.isna(executive) or str(executive).strip() == '':
        return 'BLANK'  # Treat null/empty as 'BLANK'
    return str(executive).strip().upper()  # Standardize to uppercase

def filter_os_qty(os_df, os_area_col, os_qty_col, os_due_date_col, os_exec_col, 
                 selected_branches=None, selected_years=None, till_month=None, selected_executives=None):
    """Filter by due date and aggregate net values by area, applying branch/executive filters."""
    # Validate columns
    required_columns = [os_area_col, os_qty_col, os_due_date_col, os_exec_col]
    for col in required_columns:
        if col not in os_df.columns:
            st.error(f"Column '{col}' not found in OS data.")
            return None, None, None

    # Create a copy to avoid modifying the original DataFrame
    os_df = os_df.copy()
    
    # Normalize area and executive names
    os_df[os_area_col] = os_df[os_area_col].apply(extract_area_name)
    os_df[os_exec_col] = os_df[os_exec_col].apply(extract_executive_name)

    # Convert due date to datetime
    try:
        os_df[os_due_date_col] = pd.to_datetime(os_df[os_due_date_col], errors='coerce')
    except Exception as e:
        st.error(f"Error converting '{os_due_date_col}' to datetime: {e}. Ensure dates are in 'YYYY-MM-DD' format.")
        return None, None, None

    # Filter by years and till month
    start_date, end_date = None, None
    if selected_years and till_month:
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        till_month_num = month_map.get(till_month)
        if not till_month_num:
            st.error("Invalid month selected.")
            return None, None, None
        
        # Ensure all selected years are included
        selected_years = [int(year) for year in selected_years]
        earliest_year = min(selected_years)
        latest_year = max(selected_years)
        start_date = datetime(earliest_year, 1, 1)
        end_date = (datetime(latest_year, till_month_num, 1) + relativedelta(months=1) - relativedelta(days=1))
        
        # Filter data within the date range
        os_df = os_df[
            (os_df[os_due_date_col].notna()) &
            (os_df[os_due_date_col] >= start_date) &
            (os_df[os_due_date_col] <= end_date)
        ]
        if os_df.empty:
            st.error(f"No data matches the period from Jan {earliest_year} to {end_date.strftime('%b %Y')}.")
            return None, None, None

    # Apply branch filter
    all_branches = sorted(os_df[os_area_col].dropna().unique())
    if not selected_branches:
        selected_branches = all_branches  # Default to all branches if none selected
    if sorted(selected_branches) != all_branches:
        os_df = os_df[os_df[os_area_col].isin(selected_branches)]
        if os_df.empty:
            st.error("No data matches the selected branches.")
            return None, None, None

    # Apply executive filter
    all_executives = sorted(os_df[os_exec_col].dropna().unique())
    if selected_executives and sorted(selected_executives) != sorted(all_executives):
        os_df = os_df[os_df[os_exec_col].isin(selected_executives)]
        if os_df.empty:
            st.error("No data matches the selected executives.")
            return None, None, None

    # Convert net value to numeric
    os_df[os_qty_col] = pd.to_numeric(os_df[os_qty_col], errors='coerce').fillna(0)
    if os_df[os_qty_col].isna().any():
        st.warning(f"Non-numeric values in '{os_qty_col}' replaced with 0.")

    # MODIFIED: Filter to keep only positive values in net value column
    os_df_positive = os_df[os_df[os_qty_col] > 0].copy()
    
    # If filtering results in an empty DataFrame, show a warning
    if os_df_positive.empty:
        st.warning("No positive net values found in the filtered data.")
        # You could return None here, or continue with the original data depending on preference
        # For now, we'll continue with the modified data (which is empty)

    # Aggregate net values by area (only positive values)
    os_grouped_qty = (os_df_positive.groupby(os_area_col)
                     .agg({os_qty_col: 'sum'})
                     .reset_index()
                     .rename(columns={os_area_col: 'Area', os_qty_col: 'TARGET'}))
    os_grouped_qty['TARGET'] = os_grouped_qty['TARGET'] / 100000  # Convert to lakhs

    # Determine branches to display
    branches_to_display = selected_branches if selected_branches else all_branches

    # Initialize result DataFrame
    result_df = pd.DataFrame({'Area': branches_to_display})
    result_df = pd.merge(result_df, os_grouped_qty, on='Area', how='left').fillna({'TARGET': 0})

    # Add TOTAL row
    total_row = pd.DataFrame([{'Area': 'TOTAL', 'TARGET': result_df['TARGET'].sum()}])
    result_df = pd.concat([result_df, total_row], ignore_index=True)

    # Round TARGET
    result_df['TARGET'] = result_df['TARGET'].round(2)

    return result_df, start_date, end_date

def create_od_table_image(df, title, columns_to_show=None):
    """Create a table image from the OD Target DataFrame."""
    if columns_to_show is None:
        columns_to_show = ['Area', 'TARGET (Lakhs)']
    
    fig, ax = plt.subplots(figsize=(10, len(df) * 0.5))
    ax.axis('off')
    nrows, ncols = len(df), len(columns_to_show)
    table = Table(ax, bbox=[0, 0, 1, 1])

    # Header
    for col_idx, col_name in enumerate(columns_to_show):
        table.add_cell(0, col_idx, 1.0/ncols, 1.0/nrows, text=col_name, loc='center', facecolor='#F2F2F2')
        table[0, col_idx].set_text_props(weight='bold', color='black', fontsize=12)

    # Data rows
    for row_idx in range(len(df)):
        for col_idx, col_name in enumerate(columns_to_show):
            value = df.iloc[row_idx]['Area'] if col_name == 'Area' else df.iloc[row_idx]['TARGET']
            text = str(value) if col_name == 'Area' else f"{float(value):.2f}"
            facecolor = '#DDEBF7' if row_idx % 2 == 0 else 'white'
            if row_idx == len(df) - 1 and df.iloc[row_idx, 0] == 'TOTAL':
                facecolor = '#D3D3D3'
                table.add_cell(row_idx + 1, col_idx, 1.0/ncols, 1.0/nrows, text=text, loc='center', facecolor=facecolor).set_text_props(weight='bold', fontsize=12)
            else:
                table.add_cell(row_idx + 1, col_idx, 1.0/ncols, 1.0/nrows, text=text, loc='center', facecolor=facecolor).set_text_props(fontsize=10)

    # Adjust column widths
    table[(0, 0)].width = 0.6
    table[(0, 1)].width = 0.4

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    ax.add_table(table)
    plt.suptitle(title, fontsize=16, weight='bold', color='black', y=1.05)

    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=150)
    plt.close()
    return img_buffer

def create_od_ppt_slide(slide, df, title):
    """Add a slide with the OD Target table to the presentation."""
    title_shape = slide.shapes.title
    title_shape.text = title
    title_shape.text_frame.paragraphs[0].font.size = Pt(28)
    title_shape.text_frame.paragraphs[0].font.bold = True
    title_shape.text_frame.paragraphs[0].font.color.rgb = RGBColor(0, 0, 0)
    title_shape.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER

    columns_to_show = ['Area', 'TARGET (Lakhs)']
    rows, cols = df.shape[0] + 1, len(columns_to_show)
    table = slide.shapes.add_table(rows, cols, Inches(0.5), Inches(1.5), Inches(12), Inches(5)).table

    # Header
    for col_idx, col_name in enumerate(columns_to_show):
        cell = table.cell(0, col_idx)
        cell.text = col_name
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0, 112, 192)
        cell.text_frame.paragraphs[0].font.size = Pt(14)
        cell.text_frame.paragraphs[0].font.bold = True
        cell.text_frame.paragraphs[0].font.color.rgb = RGBColor(255, 255, 255)
        cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER

    # Data rows
    for row_idx in range(len(df)):
        is_total_row = row_idx == len(df) - 1
        for col_idx, col_name in enumerate(columns_to_show):
            cell = table.cell(row_idx + 1, col_idx)
            value = df.iloc[row_idx]['Area'] if col_name == 'Area' else df.iloc[row_idx]['TARGET']
            cell.text = str(value) if col_name == 'Area' else f"{float(value):.2f}"
            cell.text_frame.paragraphs[0].font.size = Pt(12)
            cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            cell.fill.solid()
            cell.fill.fore_color.rgb = RGBColor(255, 255, 204) if is_total_row else (RGBColor(221, 235, 247) if row_idx % 2 == 0 else RGBColor(255, 255, 255))
            if is_total_row:
                cell.text_frame.paragraphs[0].font.bold = True
# Function to create combined PPT of both NBC and OD Target
def create_combined_nbc_od_ppt(customer_df, customer_title, sorted_months, od_target_df, od_title, logo_file=None):
    """Create a single PPT with two slides: one for billed customers, one for OD Target."""
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)

        # Create title slide
        create_title_slide(prs, "Number of Billed Customers & OD Target Report", logo_file)

        # Slide 1: Number of Billed Customers
        slide_layout = prs.slide_layouts[6]  # Blank slide
        slide1 = prs.slides.add_slide(slide_layout)
        create_customer_ppt_slide(slide1, customer_df, customer_title, sorted_months)

        # Slide 2: OD Target
        slide_layout = prs.slide_layouts[1]  # Title and content
        slide2 = prs.slides.add_slide(slide_layout)
        create_od_ppt_slide(slide2, od_target_df, od_title)

        ppt_buffer = BytesIO()
        prs.save(ppt_buffer)
        ppt_buffer.seek(0)
        return ppt_buffer
    except Exception as e:
        logger.error(f"Error creating Combined NBC & OD Target PPT: {e}")
        st.error(f"Error creating Combined NBC & OD Target PPT: {e}")
        return None

# Fixed tab function that properly handles both NBC and OD Target
# This is the complete tab_billed_customers function with proper indentation
# It should be used to replace your existing function

def tab_billed_customers():
    """Number of Billed Customers Report and OD Target Report Tab."""
    st.header("Number of Billed Customers & OD Target Report")
    
    dfs_info = []
    
    # Check if required files are uploaded
    sales_file = st.session_state.sales_file
    os_jan_file = st.session_state.os_jan_file
    os_feb_file = st.session_state.os_feb_file
    
    if not sales_file:
        st.warning("Please upload the Sales file in the sidebar")
        return dfs_info
    
    # Create tabs for NBC and OD reports
    nbc_tab, od_tab = st.tabs(["Number of Billed Customers", "OD Target"])
    
    # Number of Billed Customers tab
    with nbc_tab:
        st.subheader("Number of Billed Customers Setup")
        
        try:
            # Get sheet names
            sales_sheets = get_excel_sheets(sales_file)
            
            if not sales_sheets:
                st.error("No sheets found in Sales file.")
                return dfs_info
                
            # Sheet selection
            sales_sheet = st.selectbox("Select Sales Sheet", sales_sheets, key='nbc_sales_sheet')
            sales_df = pd.read_excel(sales_file, sheet_name=sales_sheet)
            columns = sales_df.columns.tolist()
            
            # Column mapping
            st.subheader("Column Mapping")
            col1, col2 = st.columns(2)
            with col1:
                date_col = st.selectbox(
                    "Date Column",
                    columns,
                    help="This column should contain dates (e.g., '2024-04-01').",
                    key='nbc_date_col'
                )
                branch_col = st.selectbox(
                    "Branch Column",
                    columns,
                    help="This column should contain branch names.",
                    key='nbc_branch_col'
                )
            with col2:
                customer_id_col = st.selectbox(
                    "Customer ID Column",
                    columns,
                    index=columns.index('SL Code') if 'SL Code' in columns else 0,
                    help="This column should contain unique customer identifiers.",
                    key='nbc_customer_id_col'
                )
                executive_col = st.selectbox(
                    "Executive Column",
                    columns,
                    index=columns.index('Executive') if 'Executive' in columns else 0,
                    help="This column should contain executive names for filtering.",
                    key='nbc_executive_col'
                )
                # Branch and Executive filters
            st.subheader("Filter Options")
            filter_tab1, filter_tab2 = st.tabs(["Branches", "Executives"])
            
            with filter_tab1:
                # Get all branches using NBC mapping
                raw_branches = sales_df[branch_col].dropna().astype(str).str.upper().unique().tolist()
                all_nbc_branches = sorted(set([nbc_branch_mapping.get(branch.split(' - ')[-1], branch.split(' - ')[-1]) for branch in raw_branches]))
                
                branch_select_all = st.checkbox("Select All Branches", value=True, key='nbc_branch_all')
                if branch_select_all:
                    selected_branches = all_nbc_branches
                else:
                    selected_branches = st.multiselect("Select Branches", all_nbc_branches, key='nbc_branches')
            
            with filter_tab2:
                # Get all executives
                all_executives = sorted(sales_df[executive_col].dropna().unique().tolist())
                
                exec_select_all = st.checkbox("Select All Executives", value=True, key='nbc_exec_all')
                if exec_select_all:
                    selected_executives = all_executives
                else:
                    selected_executives = st.multiselect("Select Executives", all_executives, key='nbc_executives')
            
            # Generate report button
            if st.button("Generate Billed Customers Report", key='nbc_generate'):
                results = create_customer_table(
                    sales_df, date_col, branch_col, customer_id_col, executive_col,
                    selected_branches=selected_branches,
                    selected_executives=selected_executives
                )
                
                if results:
                    st.subheader("Results")
                    for fy, (result_df, sorted_months) in results.items():
                        st.write(f"**Financial Year: {fy}**")
                        st.dataframe(result_df)
                        
                        # Create table image
                        title = "NUMBER OF BILLED CUSTOMERS"
                        img_buffer = create_customer_table_image(result_df, title, sorted_months, fy)
                        if img_buffer:
                            st.image(img_buffer, use_column_width=True)
                        
                        # Store NBC results for combined PPT
                        # Store NBC results for consolidated PPT
                        customers_dfs = [{'df': result_df, 'title': f"NUMBER OF BILLED CUSTOMERS - FY {fy}"}]
                        st.session_state.customers_results = customers_dfs
                        
                        # Prepare data for consolidated PPT
                        dfs_info.append({'df': result_df, 'title': f"NUMBER OF BILLED CUSTOMERS - FY {fy}"})
                else:
                    st.error("Failed to generate Number of Billed Customers report. Check your data and selections.")
        except Exception as e:
            st.error(f"Error in Number of Billed Customers tab: {e}")
            logger.error(f"Error in Number of Billed Customers tab: {e}")
    
    # OD Target tab
    with od_tab:
        st.subheader("OD Target Setup")
        
        # Check if required OS files are available
        if not os_jan_file or not os_feb_file:
            missing_files = []
            if not os_jan_file:
                missing_files.append("OS-First")
            if not os_feb_file:
                missing_files.append("OS-Second")
            st.error(f"Please upload {' and '.join(missing_files)} files in the sidebar for OD Target calculation.")
        else:
            try:
                # OS file and sheet selection
                st.subheader("File and Sheet Selection")
                # Select which file to use
                os_file_choice = st.radio(
                    "Choose OS file for OD Target calculation:", 
                    ["OS-First", "OS-Second"],
                    key="od_file_choice"
                )
                
                # Get the correct file based on selection
                os_file = os_jan_file if os_file_choice == "OS-First" else os_feb_file
                
                # Get sheet names from selected file
                os_sheets = get_excel_sheets(os_file)
                
                if not os_sheets:
                    st.error(f"No sheets found in {os_file_choice} file.")
                else:
                    # Sheet selection
                    os_sheet = st.selectbox(
                        f"Select {os_file_choice} Sheet", 
                        os_sheets, 
                        key='od_sheet'
                    )
                    
                    # Header row selection
                    header_row = st.number_input(
                        "Header Row (1-based)", 
                        min_value=1, 
                        max_value=11, 
                        value=2, 
                        key='od_header_row'
                    ) - 1
                    
                    # Load data
                    os_df = pd.read_excel(os_file, sheet_name=os_sheet, header=header_row)

                    if st.checkbox("Preview Raw OS Data", key='od_preview'):
                        st.write(f"Raw {os_file_choice} Data (first 20 rows):")
                        st.dataframe(os_df.head(20))

                    columns = os_df.columns.tolist()
                    st.subheader("OS Column Mapping")
                    col1, col2 = st.columns(2)
                    with col1:
                        os_area_col = st.selectbox(
                            "Area Column",
                            columns,
                            help="Contains branch names (e.g., COMPANY).",
                            key='od_area_col'
                        )
                        os_qty_col = st.selectbox(
                            "Net Value Column",
                            columns,
                            help="Contains net values in INR.",
                            key='od_qty_col'
                        )
                    with col2:
                        os_due_date_col = st.selectbox(
                            "Due Date Column",
                            columns,
                            help="Contains due dates (e.g., '2018-02-15').",
                            key='od_due_date_col'
                        )
                        os_exec_col = st.selectbox(
                            "Executive Column",
                            columns,
                            help="Contains executive names (BLANK for null/empty).",
                            key='od_exec_col'
                        )
                        # Due Date Filter
                    st.subheader("Due Date Filter")
                    try:
                        os_df[os_due_date_col] = pd.to_datetime(os_df[os_due_date_col], errors='coerce')
                        years = sorted(os_df[os_due_date_col].dt.year.dropna().astype(int).unique())
                    except Exception as e:
                        st.error(f"Error processing due dates: {e}. Ensure valid date format.")
                        years = []

                    if not years:
                        st.warning(f"No valid years found in {os_file_choice}'s due date column.")
                    else:
                        selected_years = st.multiselect(
                            "Select years for filtering",
                            options=[str(year) for year in years],
                            default=[str(year) for year in years],
                            key='od_year_multiselect'
                        )

                        if not selected_years:
                            st.error("Please select at least one year.")
                        else:
                            month_options = ['January', 'February', 'March', 'April', 'May', 'June',
                                           'July', 'August', 'September', 'October', 'November', 'December']
                            till_month = st.selectbox("Select end month", month_options, key='od_till_month')
                    
                    # OD Branch Selection
                    st.subheader("Branch Selection")
                    os_branches = sorted(set([b for b in os_df[os_area_col].dropna().apply(extract_area_name) if b]))
                    if not os_branches:
                        st.error(f"No valid branches found in {os_file_choice} data. Check area column.")
                    else:
                        os_branch_select_all = st.checkbox("Select All OS Branches", value=True, key='od_branch_all')
                        if os_branch_select_all:
                            selected_os_branches = os_branches
                        else:
                            selected_os_branches = st.multiselect("Select OS Branches", os_branches, key='od_branches')
                    
                    # OD Executive Selection
                    st.subheader("Executive Selection")
                    os_executives = sorted(set([e for e in os_df[os_exec_col].apply(extract_executive_name) if e]))
                    
                    os_exec_select_all = st.checkbox("Select All OS Executives", value=True, key='od_exec_all')
                    if os_exec_select_all:
                        selected_os_executives = os_executives
                    else:
                        selected_os_executives = st.multiselect("Select OS Executives", os_executives, key='od_executives')
                    
                    # Generate OD Target report button
                    if st.button("Generate OD Target Report", key='nbc_od_generate'):
                        if selected_years and till_month:
                            od_target_df, start_date, end_date = filter_os_qty(
                                os_df, os_area_col, os_qty_col, os_due_date_col, os_exec_col,
                                selected_branches=selected_os_branches,
                                selected_years=selected_years,
                                till_month=till_month,
                                selected_executives=selected_os_executives
                            )
                            
                            if od_target_df is not None:
                                # Create title for OD Target
                                start_str = start_date.strftime('%b %Y') if start_date else 'All Periods'
                                end_str = end_date.strftime('%b %Y') if end_date else 'All Periods'

                                
                                od_title = f"OD Target-{end_str}(Value in Lakhs)"
                                st.subheader(od_title)
                                st.dataframe(od_target_df)
                                
                                # Create table image
                                img_buffer = create_od_table_image(od_target_df, od_title)
                                if img_buffer:
                                    st.image(img_buffer, use_column_width=True)
                                
                                # Store OD results as part of customers_results for combined PPT
                                if 'customers_results' not in st.session_state or st.session_state.customers_results is None:
                                    st.session_state.customers_results = []
                                st.session_state.customers_results.append({'df': od_target_df, 'title': od_title})
                                
                                # Add OD results to dfs_info for consolidated PPT
                                dfs_info.append({'df': od_target_df, 'title': od_title})
                        else:
                            st.error("Please select at least one year and a month.")
            except Exception as e:
                st.error(f"Error in OD Target tab: {e}")
                logger.error(f"Error in OD Target tab: {e}")
    
    # Combined download section at the bottom of the tab
    st.divider()
    st.subheader("Combined Report")
    
    if hasattr(st.session_state, 'nbc_results') and hasattr(st.session_state, 'od_results'):
        try:
            # Create combined PPT of both reports
            nbc_data = st.session_state.nbc_results
            od_data = st.session_state.od_results
            
            ppt_buffer = create_combined_nbc_od_ppt(
                nbc_data['df'],
                nbc_data['title'],
                nbc_data['sorted_months'],
                od_data['df'],
                od_data['title'],
                st.session_state.logo_file
            )
            
            if ppt_buffer:
                st.download_button(
                    label="Download Combined NBC & OD Target PPT",
                    data=ppt_buffer,
                    file_name="NBC_OD_Target_Combined.pptx",
                    mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                    key='nbc_od_combined_download'
                )
        except Exception as e:
            st.error(f"Error creating combined report: {e}")
            logger.error(f"Error creating combined report: {e}")
            
        # Also add data for the main consolidated report
        if hasattr(st.session_state, 'nbc_results') and isinstance(st.session_state.nbc_results, dict) and 'df' in st.session_state.nbc_results:
            if 'customers_results' not in st.session_state or not st.session_state.customers_results:
                st.session_state.customers_results = []
            # Add NBC data to customers_results if not already there
            nbc_data = {'df': st.session_state.nbc_results['df'], 'title': st.session_state.nbc_results.get('title', 'Number of Billed Customers')}
            if not any(d.get('title') == nbc_data['title'] for d in st.session_state.customers_results):
                st.session_state.customers_results.append(nbc_data)

        if hasattr(st.session_state, 'od_results') and isinstance(st.session_state.od_results, dict) and 'df' in st.session_state.od_results:
            if 'customers_results' not in st.session_state or not st.session_state.customers_results:
                st.session_state.customers_results = []
            # Add OD data to customers_results if not already there
            od_data = {'df': st.session_state.od_results['df'], 'title': st.session_state.od_results.get('title', 'OD Target')}
            if not any(d.get('title') == od_data['title'] for d in st.session_state.customers_results):
                st.session_state.customers_results.append(od_data)
                
    elif hasattr(st.session_state, 'nbc_results'):
        st.info("Only Number of Billed Customers report is available. Generate OD Target report to enable combined download.")
    elif hasattr(st.session_state, 'od_results'):
        st.info("Only OD Target report is available. Generate Number of Billed Customers report to enable combined download.")
    else:
        st.info("Generate both Number of Billed Customers and OD Target reports to enable combined download.")
    
    return dfs_info
#######################################################
# MODULE 3: OD TARGET VS COLLECTION REPORT
#######################################################

def validate_numeric_column(df, col_name, file_name):
    """Validate that a column contains numeric data."""
    try:
        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        if df[col_name].isna().all():
            return False, f"Column '{col_name}' in {file_name} contains no valid numeric data."
        return True, None
    except Exception as e:
        return False, f"Column '{col_name}' in {file_name} contains non-numeric data: {e}"

def get_available_months(os_first, os_second, total_sale,
                         os_first_due_date_col, os_first_ref_date_col,
                         os_second_due_date_col, os_second_ref_date_col,
                         sale_bill_date_col, sale_due_date_col):
    """Get unique months from date columns across all files."""
    months = set()
    
    for df, date_cols in [
        (os_first, [os_first_due_date_col, os_first_ref_date_col]),
        (os_second, [os_second_due_date_col, os_second_ref_date_col]),
        (total_sale, [sale_bill_date_col, sale_due_date_col])
    ]:
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                valid_dates = df[col].dropna()
                month_years = valid_dates.dt.strftime('%b-%y').unique()
                months.update(month_years)
    
    months = sorted(list(months), key=lambda x: pd.to_datetime("01-" + x, format="%d-%b-%y"))
    return months

def calculate_od_values(os_first, os_second, total_sale, selected_month_str,
                        os_first_due_date_col, os_first_ref_date_col, os_first_unit_col, os_first_net_value_col, os_first_exec_col,
                        os_second_due_date_col, os_second_ref_date_col, os_second_unit_col, os_second_net_value_col, os_second_exec_col,
                        sale_bill_date_col, sale_due_date_col, sale_branch_col, sale_value_col, sale_exec_col,
                        os_first_executives, os_second_executives, sale_executives,
                        os_first_branches, os_second_branches, sale_branches):
    """Calculate OD Target vs Collection metrics for a selected month with individual filters."""
    
    # Create copies to avoid modifying original DataFrames
    os_first = os_first.copy()
    os_second = os_second.copy()
    total_sale = total_sale.copy()
    
    # Validate numeric columns
    for df, col, file in [
        (os_first, os_first_net_value_col, "OS-First"),
        (os_second, os_second_net_value_col, "OS-Second"),
        (total_sale, sale_value_col, "Total Sale")
    ]:
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isna().all():
                st.error(f"Column '{col}' in {file} contains no valid numeric data.")
                return None
        except Exception as e:
            st.error(f"Error processing column '{col}' in {file}: {e}")
            return None

    # Filter out negative values in OS-First and OS-Second Net Value columns
    os_first_initial_rows = os_first.shape[0]
    os_first = os_first[os_first[os_first_net_value_col] >= 0]
    os_first_filtered_rows = os_first.shape[0]
    logger.debug(f"OS-First: Filtered out {os_first_initial_rows - os_first_filtered_rows} rows with negative Net Value")

    os_second_initial_rows = os_second.shape[0]
    os_second = os_second[os_second[os_second_net_value_col] >= 0]
    os_second_filtered_rows = os_second.shape[0]
    logger.debug(f"OS-Second: Filtered out {os_second_initial_rows - os_second_filtered_rows} rows with negative Net Value")

    # Convert date columns to datetime and map branches
    os_first[os_first_due_date_col] = pd.to_datetime(os_first[os_first_due_date_col], errors='coerce')
    os_first[os_first_ref_date_col] = pd.to_datetime(os_first.get(os_first_ref_date_col), errors='coerce')
    os_first["Branch"] = os_first[os_first_unit_col].apply(map_branch, case='title')

    os_second[os_second_due_date_col] = pd.to_datetime(os_second[os_second_due_date_col], errors='coerce')
    os_second[os_second_ref_date_col] = pd.to_datetime(os_second.get(os_second_ref_date_col), errors='coerce')
    os_second["Branch"] = os_second[os_second_unit_col].apply(map_branch, case='title')

    total_sale[sale_bill_date_col] = pd.to_datetime(total_sale[sale_bill_date_col], errors='coerce')
    total_sale[sale_due_date_col] = pd.to_datetime(total_sale[sale_due_date_col], errors='coerce')
    total_sale["Branch"] = total_sale[sale_branch_col].apply(map_branch, case='title')

    # Apply executive filtering
    if os_first_executives:
        os_first = os_first[os_first[os_first_exec_col].isin(os_first_executives)]
    if os_second_executives:
        os_second = os_second[os_second[os_second_exec_col].isin(os_second_executives)]
    if sale_executives:
        total_sale = total_sale[total_sale[sale_exec_col].isin(sale_executives)]

    # Check if any dataframe is empty after filtering
    empty_dfs = []
    if os_first.empty:
        empty_dfs.append("OS-First")
    if os_second.empty:
        empty_dfs.append("OS-Second")
    if total_sale.empty:
        empty_dfs.append("Total Sale")
    
    if empty_dfs:
        st.error(f"No data remains after executive filtering for: {', '.join(empty_dfs)}")
        return None

    # Apply branch filtering
    if os_first_branches:
        os_first = os_first[os_first["Branch"].isin(os_first_branches)]
    if os_second_branches:
        os_second = os_second[os_second["Branch"].isin(os_second_branches)]
    if sale_branches:
        total_sale = total_sale[total_sale["Branch"].isin(sale_branches)]

    # Check if any dataframe is empty after branch filtering
    empty_dfs = []
    if os_first.empty:
        empty_dfs.append("OS-First")
    if os_second.empty:
        empty_dfs.append("OS-Second")
    if total_sale.empty:
        empty_dfs.append("Total Sale")
    
    if empty_dfs:
        st.error(f"No data remains after branch filtering for: {', '.join(empty_dfs)}")
        return None

    # Define date ranges from selected_month_str
    specified_date = pd.to_datetime("01-" + selected_month_str, format="%d-%b-%y")
    specified_month_end = specified_date + pd.offsets.MonthEnd(0)

    # Calculate Due Target: Sum of Net Value from OS Jan where Due Date <= specified_month_end
    due_target = os_first[os_first[os_first_due_date_col] <= specified_month_end]
    due_target_sum = due_target.groupby("Branch")[os_first_net_value_col].sum().reset_index()
    due_target_sum.columns = ["Branch", "Due Target"]

    # Calculate Collection Achieved: OS Jan (Due Date <= specified_month_end) - OS Feb (Due Date <= specified_month_end AND Ref Date < specified_date)
    os_first_coll = os_first[os_first[os_first_due_date_col] <= specified_month_end]
    os_first_coll_sum = os_first_coll.groupby("Branch")[os_first_net_value_col].sum().reset_index()
    os_first_coll_sum.columns = ["Branch", "OS Jan Coll"]

    os_second_coll = os_second[(os_second[os_second_due_date_col] <= specified_month_end) & 
                              (os_second[os_second_ref_date_col] < specified_date)]
    os_second_coll_sum = os_second_coll.groupby("Branch")[os_second_net_value_col].sum().reset_index()
    os_second_coll_sum.columns = ["Branch", "OS Feb Coll"]

    collection = os_first_coll_sum.merge(os_second_coll_sum, on="Branch", how="outer").fillna(0)
    collection["Collection Achieved"] = collection["OS Jan Coll"] - collection["OS Feb Coll"]
    collection["Overall % Achieved"] = np.where(collection["OS Jan Coll"] > 0, 
                                               (collection["Collection Achieved"] / collection["OS Jan Coll"]) * 100, 
                                               0)

    # Merge with Due Target
    collection = collection.merge(due_target_sum[["Branch", "Due Target"]], on="Branch", how="outer").fillna(0)

    # Calculate Overdue: Total Sale where Bill Date and Due Date both in the specified month
    overdue = total_sale[(total_sale[sale_bill_date_col].between(specified_date, specified_month_end)) & 
                        (total_sale[sale_due_date_col].between(specified_date, specified_month_end))]
    overdue_sum = overdue.groupby("Branch")[sale_value_col].sum().reset_index()
    overdue_sum.columns = ["Branch", "For the month Overdue"]

    # Calculate Month Collection
    # Sales in specified month
    sale_value = overdue.groupby("Branch")[sale_value_col].sum().reset_index()
    sale_value.columns = ["Branch", "Sale Value"]

    # OS Feb with Ref Date in specified month and Due Date in specified month
    os_second_month = os_second[(os_second[os_second_ref_date_col].between(specified_date, specified_month_end)) & 
                               (os_second[os_second_due_date_col].between(specified_date, specified_month_end))]
    os_second_month_sum = os_second_month.groupby("Branch")[os_second_net_value_col].sum().reset_index()
    os_second_month_sum.columns = ["Branch", "OS Month Collection"]

    month_collection = sale_value.merge(os_second_month_sum, on="Branch", how="outer").fillna(0)
    month_collection["For the month Collection"] = month_collection["Sale Value"] - month_collection["OS Month Collection"]
    month_collection_final = month_collection[["Branch", "For the month Collection"]]

    # Combine all metrics
    final = collection.drop(columns=["OS Jan Coll", "OS Feb Coll"]).merge(overdue_sum, on="Branch", how="outer")\
            .merge(month_collection_final, on="Branch", how="outer").fillna(0)
    final["% Achieved (Selected Month)"] = np.where(final["For the month Overdue"] > 0, 
                                                   (final["For the month Collection"] / final["For the month Overdue"]) * 100, 
                                                   0)

    # Handle branch names and exclude HO
    final["Branch"] = final["Branch"].replace({"Puducherry": "Pondicherry"})

    # Convert to lakhs and round
    val_cols = ["Due Target", "Collection Achieved", "For the month Overdue", "For the month Collection"]
    final[val_cols] = final[val_cols].div(100000)
    round_cols = val_cols + ["Overall % Achieved", "% Achieved (Selected Month)"]
    final[round_cols] = final[round_cols].round(2)

    # Reorder columns
    final = final[["Branch", "Due Target", "Collection Achieved", "Overall % Achieved", 
                  "For the month Overdue", "For the month Collection", "% Achieved (Selected Month)"]]
    # Sort by branch
    final.sort_values("Branch", inplace=True)
    
    # Add TOTAL row
    total_row = {'Branch': 'TOTAL'}
    for col in final.columns[1:]:
        if col in ["Overall % Achieved", "% Achieved (Selected Month)"]:
            avg_val = final[col].replace([np.inf, -np.inf], 0).mean()
            total_row[col] = round(avg_val, 2)
        else:
            total_row[col] = round(final[col].sum(), 2)
    
    final = pd.concat([final, pd.DataFrame([total_row])], ignore_index=True)
    
    return final

def create_od_ppt(df, title, logo_file=None):
    """Create a PPT for OD Target vs Collection Report."""
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)
        
        # Create title slide
        create_title_slide(prs, title, logo_file)
        
        # Add data slide
        add_table_slide(prs, df, f"Branch-wise Performance - {title}", percent_cols=[3, 6])
        
        # Save to buffer
        ppt_buffer = BytesIO()
        prs.save(ppt_buffer)
        ppt_buffer.seek(0)
        return ppt_buffer
    except Exception as e:
        logger.error(f"Error creating OD PPT: {e}")
        st.error(f"Error creating OD PPT: {e}")
        return None

def tab_od_target():
    """OD Target vs Collection Report Tab."""
    st.header("OD Target vs Collection Report")
    
    dfs_info = []
    
    os_jan_file = st.session_state.os_jan_file
    os_feb_file = st.session_state.os_feb_file
    sales_file = st.session_state.sales_file
    
    if not os_jan_file or not os_feb_file or not sales_file:
        st.warning("Please upload all required files in the sidebar (OS Jan, OS Feb, and Sales files)")
        return dfs_info
    
    try:
        # Get sheet names
        os_jan_sheets = get_excel_sheets(os_jan_file)
        os_feb_sheets = get_excel_sheets(os_feb_file)
        sales_sheets = get_excel_sheets(sales_file)
        
        if not os_jan_sheets or not os_feb_sheets or not sales_sheets:
            st.error("No sheets found in one or more files")
            return dfs_info
            
        # Sheet selection
        st.subheader("Sheet Selection")
        col1, col2, col3 = st.columns(3)
        with col1:
            os_jan_sheet = st.selectbox("OS-First Sheet", os_jan_sheets, key='od_os_jan_sheet')
            os_jan_header = st.number_input("OS-First Header Row (1-based)", min_value=1, max_value=11, value=1, key='od_os_jan_header') - 1
        with col2:
            os_feb_sheet = st.selectbox("OS-Second Sheet", os_feb_sheets, key='od_os_feb_sheet')
            os_feb_header = st.number_input("OS-Second Header Row (1-based)", min_value=1, max_value=11, value=1, key='od_os_feb_header') - 1
        with col3:
            sales_sheet = st.selectbox("Sales Sheet", sales_sheets, key='od_sales_sheet')
            sales_header = st.number_input("Sales Header Row (1-based)", min_value=1, max_value=11, value=1, key='od_sales_header') - 1
        
        # Load data
        os_jan = pd.read_excel(os_jan_file, sheet_name=os_jan_sheet, header=os_jan_header)
        os_feb = pd.read_excel(os_feb_file, sheet_name=os_feb_sheet, header=os_feb_header)
        total_sale = pd.read_excel(sales_file, sheet_name=sales_sheet, header=sales_header)
        
        # Column mappings
        st.subheader("OS-First Column Mapping")
        os_jan_cols = os_jan.columns.tolist()
        col1, col2 = st.columns(2)
        with col1:
            os_jan_due_date_col = st.selectbox("Due Date Column", os_jan_cols, 
                                              index=os_jan_cols.index("Due Date") if "Due Date" in os_jan_cols else 0, 
                                              key='od_os_jan_due_date')
            os_jan_ref_date_col = st.selectbox("Reference Date Column", os_jan_cols, key='od_os_jan_ref_date')
        with col2:
            os_jan_unit_col = st.selectbox("Branch Column", os_jan_cols, 
                                          index=os_jan_cols.index("Unit") if "Unit" in os_jan_cols else 0, 
                                          key='od_os_jan_unit')
            os_jan_net_value_col = st.selectbox("Net Value Column", os_jan_cols, key='od_os_jan_net_value')
        os_jan_exec_col = st.selectbox("Executive Column", os_jan_cols, key='od_os_jan_exec')
        
        st.subheader("OS-Second Column Mapping")
        os_feb_cols = os_feb.columns.tolist()
        col1, col2 = st.columns(2)
        with col1:
            os_feb_due_date_col = st.selectbox("Due Date Column", os_feb_cols, 
                                              index=os_feb_cols.index("Due Date") if "Due Date" in os_feb_cols else 0, 
                                              key='od_os_feb_due_date')
            os_feb_ref_date_col = st.selectbox("Reference Date Column", os_feb_cols, key='od_os_feb_ref_date')
        with col2:
            os_feb_unit_col = st.selectbox("Branch Column", os_feb_cols, 
                                          index=os_feb_cols.index("Unit") if "Unit" in os_feb_cols else 0, 
                                          key='od_os_feb_unit')
            os_feb_net_value_col = st.selectbox("Net Value Column", os_feb_cols, key='od_os_feb_net_value')
        os_feb_exec_col = st.selectbox("Executive Column", os_feb_cols, key='od_os_feb_exec')
        
        st.subheader("Total Sale Column Mapping")
        sales_cols = total_sale.columns.tolist()
        col1, col2 = st.columns(2)
        with col1:
            sale_bill_date_col = st.selectbox("Bill Date Column", sales_cols, 
                                             index=sales_cols.index("Bill Date") if "Bill Date" in sales_cols else 0, 
                                             key='od_sale_bill_date')
            sale_due_date_col = st.selectbox("Due Date Column", sales_cols, 
                                            index=sales_cols.index("Due Date") if "Due Date" in sales_cols else 0, 
                                            key='od_sale_due_date')
        with col2:
            sale_branch_col = st.selectbox("Branch Column", sales_cols, 
                                          index=sales_cols.index("Unit") if "Unit" in sales_cols else 0, 
                                          key='od_sale_branch')
            sale_value_col = st.selectbox("Value Column", sales_cols, key='od_sale_value')
        sale_exec_col = st.selectbox("Executive Column", sales_cols, key='od_sale_exec')
        
        # Get available months from all date columns
        available_months = get_available_months(
            os_jan, os_feb, total_sale,
            os_jan_due_date_col, os_jan_ref_date_col,
            os_feb_due_date_col, os_feb_ref_date_col,
            sale_bill_date_col, sale_due_date_col
        )
        
        if not available_months:
            st.error("No valid months found in the date columns. Please check column selections.")
            return dfs_info
            
        selected_month_str = st.selectbox("Select Month", available_months, index=len(available_months)-1, key='od_month')
        
        # Filters for branches and executives
        st.subheader("Filter Options")
        filter_tabs = st.tabs(["OS-First Branches", "OS-Second Branches", "Sales Branches", 
                             "OS-First Executives", "OS-Second Executives", "Sales Executives"])
        
        # OS Jan Branch Selection
        with filter_tabs[0]:
            os_jan[os_jan_unit_col] = os_jan[os_jan_unit_col].astype(str)
            os_jan_branches = sorted(set([map_branch(b, 'title') for b in os_jan[os_jan_unit_col].dropna()]))
            
            os_jan_branch_select_all = st.checkbox("Select All OS-First Branches", value=True, key='od_os_jan_branch_all')
            if os_jan_branch_select_all:
                selected_os_jan_branches = os_jan_branches
            else:
                selected_os_jan_branches = st.multiselect("Select OS-First Branches", os_jan_branches, key='od_os_jan_branches')
        
        # OS Feb Branch Selection
        with filter_tabs[1]:
            os_feb[os_feb_unit_col] = os_feb[os_feb_unit_col].astype(str)
            os_feb_branches = sorted(set([map_branch(b, 'title') for b in os_feb[os_feb_unit_col].dropna()]))
            
            os_feb_branch_select_all = st.checkbox("Select All OS-Second Branches", value=True, key='od_os_feb_branch_all')
            if os_feb_branch_select_all:
                selected_os_feb_branches = os_feb_branches
            else:
                selected_os_feb_branches = st.multiselect("Select OS-Second Branches", os_feb_branches, key='od_os_feb_branches')
        
        # Sales Branch Selection
        with filter_tabs[2]:
            total_sale[sale_branch_col] = total_sale[sale_branch_col].astype(str)
            sale_branches = sorted(set([map_branch(b, 'title') for b in total_sale[sale_branch_col].dropna()]))
            
            sale_branch_select_all = st.checkbox("Select All Sales Branches", value=True, key='od_sale_branch_all')
            if sale_branch_select_all:
                selected_sale_branches = sale_branches
            else:
                selected_sale_branches = st.multiselect("Select Sales Branches", sale_branches, key='od_sale_branches')
        
        # OS Jan Executive Selection
        with filter_tabs[3]:
            os_jan_executives = sorted(os_jan[os_jan_exec_col].dropna().unique().tolist())
            
            os_jan_exec_select_all = st.checkbox("Select All OS-First Executives", value=True, key='od_os_jan_exec_all')
            if os_jan_exec_select_all:
                selected_os_jan_executives = os_jan_executives
            else:
                selected_os_jan_executives = st.multiselect("Select OS-First Executives", os_jan_executives, key='od_os_jan_executives')
        
        # OS Feb Executive Selection
        with filter_tabs[4]:
            os_feb_executives = sorted(os_feb[os_feb_exec_col].dropna().unique().tolist())
            
            os_feb_exec_select_all = st.checkbox("Select All OS-Second Executives", value=True, key='od_os_feb_exec_all')
            if os_feb_exec_select_all:
                selected_os_feb_executives = os_feb_executives
            else:
                selected_os_feb_executives = st.multiselect("Select OS-Second Executives", os_feb_executives, key='od_os_feb_executives')
        
        # Sales Executive Selection
        with filter_tabs[5]:
            sale_executives = sorted(total_sale[sale_exec_col].dropna().unique().tolist())
            
            sale_exec_select_all = st.checkbox("Select All Sales Executives", value=True, key='od_sale_exec_all')
            if sale_exec_select_all:
                selected_sale_executives = sale_executives
            else:
                selected_sale_executives = st.multiselect("Select Sales Executives", sale_executives, key='od_sale_executives')
        
        # Generate report button
        if st.button("Generate OD Target vs Collection Report", key='od_generate'):
            final = calculate_od_values(
                os_jan, os_feb, total_sale, selected_month_str,
                os_jan_due_date_col, os_jan_ref_date_col, os_jan_unit_col, os_jan_net_value_col, os_jan_exec_col,
                os_feb_due_date_col, os_feb_ref_date_col, os_feb_unit_col, os_feb_net_value_col, os_feb_exec_col,
                sale_bill_date_col, sale_due_date_col, sale_branch_col, sale_value_col, sale_exec_col,
                selected_os_jan_executives, selected_os_feb_executives, selected_sale_executives,
                selected_os_jan_branches, selected_os_feb_branches, selected_sale_branches
            )
            
            if final is not None and not final.empty:
                st.subheader("Results")
                
                # Display the results
                st.dataframe(final)
                
                # Create table image
                img_buffer = create_table_image(final, f"OD TARGET VS COLLECTION - {selected_month_str}", percent_cols=[3, 6])
                if img_buffer:
                    st.image(img_buffer, use_column_width=True)
                
                # Prepare data for consolidated PPT
                dfs_info = [{'df': final, 'title': f"OD TARGET VS COLLECTION - {selected_month_str} (Value in Lakhs)", 'percent_cols': [3, 6]}]
                
                # Store results in session state
                st.session_state.od_results = dfs_info
                
                # Download individual PPT
                ppt_buffer = create_od_ppt(
                    final, 
                    f"OD Target vs Collection - {selected_month_str}",
                    st.session_state.logo_file
                )
                
                if ppt_buffer:
                    unique_id = str(uuid.uuid4())[:8]
                    st.download_button(
                        label="Download OD Target vs Collection PPT",
                        data=ppt_buffer,
                        file_name=f"OD_Target_vs_Collection_{selected_month_str}_{unique_id}.pptx",
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"od_download_{unique_id}"
                    )
            else:
                st.error("Failed to generate OD Target vs Collection report. Check your data and selections.")
    except Exception as e:
        st.error(f"Error in OD Target tab: {e}")
        logger.error(f"Error in OD Target tab: {e}")
    
    return dfs_info
#######################################################
# MODULE 4: PRODUCT GROWTH REPORT
#######################################################

def standardize_name(name):
    """Standardize a name by removing extra spaces, special characters, and converting to title case."""
    if pd.isna(name) or not name:
        return ""
    name = str(name).strip().lower()
    name = ''.join(c for c in name if c.isalnum() or c.isspace())
    name = ' '.join(word.capitalize() for word in name.split())
    # Unify 'General' variants
    general_variants = ['general', 'gen', 'generals', 'general ', 'genral', 'generl']
    if any(variant in name.lower() for variant in general_variants):
        return 'General'
    return name

def log_non_numeric_values(df, col):
    """Log non-numeric values in a column before conversion."""
    if col in df.columns:
        non_numeric = df[df[col].apply(lambda x: not isinstance(x, (int, float)) and pd.notna(x))][col].unique()
        if len(non_numeric) > 0:
            logger.warning(f"Non-numeric values in {col}: {non_numeric}")

# Fixed version of the calculate_product_growth function with better error handling

def calculate_product_growth(ly_df, cy_df, budget_df, ly_months, cy_months, ly_date_col, cy_date_col, 
                           ly_qty_col, cy_qty_col, ly_value_col, cy_value_col, 
                           budget_qty_col, budget_value_col, ly_product_col, cy_product_col, 
                           ly_company_group_col, cy_company_group_col, budget_company_group_col, budget_product_group_col,
                           ly_exec_col, cy_exec_col, budget_exec_col, selected_executives=None, 
                           selected_company_groups=None):
    """Calculate product growth metrics comparing last year, current year and budget data."""
    try:
        # Debug information to help troubleshoot
        st.write("Processing data...")
        
        # Create copies to avoid modifying original DataFrames
        ly_df = ly_df.copy()
        cy_df = cy_df.copy()
        budget_df = budget_df.copy()

        # Validate columns
        required_cols = [
            (ly_df, [ly_date_col, ly_qty_col, ly_value_col, ly_product_col, ly_company_group_col, ly_exec_col], "Last Year"),
            (cy_df, [cy_date_col, cy_qty_col, cy_value_col, cy_product_col, cy_company_group_col, cy_exec_col], "Current Year"),
            (budget_df, [budget_qty_col, budget_value_col, budget_product_group_col, budget_company_group_col, budget_exec_col], "Budget")
        ]
        
        for df, cols, df_name in required_cols:
            missing_cols = [col for col in cols if col not in df.columns]
            if missing_cols:
                st.error(f"Missing columns in {df_name} data: {missing_cols}")
                return None

        # Apply executive filter
        if selected_executives:
            st.write(f"Applying executive filter: {len(selected_executives)} executives selected")
            if ly_exec_col in ly_df.columns:
                ly_df = ly_df[ly_df[ly_exec_col].isin(selected_executives)]
            if cy_exec_col in cy_df.columns:
                cy_df = cy_df[cy_df[cy_exec_col].isin(selected_executives)]
            if budget_exec_col in budget_df.columns:
                budget_df = budget_df[budget_df[budget_exec_col].isin(selected_executives)]

        if ly_df.empty or cy_df.empty or budget_df.empty:
            empty_dfs = []
            if ly_df.empty: empty_dfs.append("Last Year")
            if cy_df.empty: empty_dfs.append("Current Year")
            if budget_df.empty: empty_dfs.append("Budget")
            
            st.warning(f"No data remains after executive filtering for: {', '.join(empty_dfs)}. Please check executive selections.")
            return None

        # Check and convert date columns
        st.write("Converting date columns...")
        try:
            ly_df[ly_date_col] = pd.to_datetime(ly_df[ly_date_col], dayfirst=True, errors='coerce')
            cy_df[cy_date_col] = pd.to_datetime(cy_df[cy_date_col], dayfirst=True, errors='coerce')
        except Exception as e:
            st.error(f"Error converting date columns: {e}")
            return None
        
        # Check for valid dates
        if ly_df[ly_date_col].isna().all() or cy_df[cy_date_col].isna().all():
            st.error("Date columns contain no valid dates. Check date formats.")
            return None
        
        available_ly_months = ly_df[ly_date_col].dt.strftime('%b %y').dropna().unique().tolist()
        available_cy_months = cy_df[cy_date_col].dt.strftime('%b %y').dropna().unique().tolist()

        st.write(f"Available LY months: {available_ly_months}")
        st.write(f"Available CY months: {available_cy_months}")
        
        if not available_ly_months or not available_cy_months:
            st.error("No valid months extracted from date columns. Please check the date column format.")
            return None

        # Apply month filter
        st.write(f"Filtering for LY months: {ly_months}")
        st.write(f"Filtering for CY months: {cy_months}")
        
        if not ly_months or not cy_months:
            st.error("No months selected for comparison. Please select months for both LY and CY.")
            return None
            
        try:
            ly_filtered_df = ly_df[ly_df[ly_date_col].dt.strftime('%b %y').isin(ly_months)]
            cy_filtered_df = cy_df[cy_df[cy_date_col].dt.strftime('%b %y').isin(cy_months)]
        except Exception as e:
            st.error(f"Error filtering by months: {e}")
            return None

        if ly_filtered_df.empty or cy_filtered_df.empty:
            st.warning(f"No data for selected months (LY: {', '.join(ly_months)}, CY: {', '.join(cy_months)})")
            return None

        # Standardize product and company group names
        st.write("Standardizing product and company group names...")
        
        # Handle possible missing values before applying standardize_name
        ly_filtered_df[ly_product_col] = ly_filtered_df[ly_product_col].fillna("")
        cy_filtered_df[cy_product_col] = cy_filtered_df[cy_product_col].fillna("")
        budget_df[budget_product_group_col] = budget_df[budget_product_group_col].fillna("")
        
        ly_filtered_df[ly_company_group_col] = ly_filtered_df[ly_company_group_col].fillna("")
        cy_filtered_df[cy_company_group_col] = cy_filtered_df[cy_company_group_col].fillna("")
        budget_df[budget_company_group_col] = budget_df[budget_company_group_col].fillna("")
        
        # Apply standardization
        ly_filtered_df[ly_product_col] = ly_filtered_df[ly_product_col].apply(standardize_name)
        cy_filtered_df[cy_product_col] = cy_filtered_df[cy_product_col].apply(standardize_name)
        budget_df[budget_product_group_col] = budget_df[budget_product_group_col].apply(standardize_name)

        ly_filtered_df[ly_company_group_col] = ly_filtered_df[ly_company_group_col].apply(standardize_name)
        cy_filtered_df[cy_company_group_col] = cy_filtered_df[cy_company_group_col].apply(standardize_name)
        budget_df[budget_company_group_col] = budget_df[budget_company_group_col].apply(standardize_name)

        # Apply company group filter
        if selected_company_groups:
            st.write(f"Applying company group filter: {selected_company_groups}")
            selected_company_groups = [standardize_name(g) for g in selected_company_groups]
            
            # Filter data but keep track of what's removed
            ly_before = len(ly_filtered_df)
            cy_before = len(cy_filtered_df)
            budget_before = len(budget_df)
            
            ly_filtered_df = ly_filtered_df[ly_filtered_df[ly_company_group_col].isin(selected_company_groups)]
            cy_filtered_df = cy_filtered_df[cy_filtered_df[cy_company_group_col].isin(selected_company_groups)]
            budget_df = budget_df[budget_df[budget_company_group_col].isin(selected_company_groups)]
            
            st.write(f"Filtered out {ly_before - len(ly_filtered_df)} rows from LY data")
            st.write(f"Filtered out {cy_before - len(cy_filtered_df)} rows from CY data")
            st.write(f"Filtered out {budget_before - len(budget_df)} rows from Budget data")

            if ly_filtered_df.empty or cy_filtered_df.empty or budget_df.empty:
                empty_dfs = []
                if ly_filtered_df.empty: empty_dfs.append("Last Year")
                if cy_filtered_df.empty: empty_dfs.append("Current Year")
                if budget_df.empty: empty_dfs.append("Budget")
                
                st.warning(f"No data remains after filtering for company groups: {selected_company_groups} in {', '.join(empty_dfs)}")
                return None

        # Convert quantity and value columns to numeric
        st.write("Converting quantity and value columns to numeric...")
        for df, qty_col, value_col, df_name in [
            (ly_filtered_df, ly_qty_col, ly_value_col, "Last Year"), 
            (cy_filtered_df, cy_qty_col, cy_value_col, "Current Year"), 
            (budget_df, budget_qty_col, budget_value_col, "Budget")
        ]:
            try:
                df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
                df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)
                
                # Log some stats to help debug
                st.write(f"{df_name} qty range: {df[qty_col].min()} to {df[qty_col].max()}")
                st.write(f"{df_name} value range: {df[value_col].min()} to {df[value_col].max()}")
                
            except Exception as e:
                st.error(f"Error converting numeric columns in {df_name} data: {e}")
                return None

        # Get unique company groups from all datasets
        st.write("Determining company groups...")
        
        # Print what we have in each dataset
        st.write(f"LY company groups: {ly_filtered_df[ly_company_group_col].unique().tolist()}")
        st.write(f"CY company groups: {cy_filtered_df[cy_company_group_col].unique().tolist()}")
        st.write(f"Budget company groups: {budget_df[budget_company_group_col].unique().tolist()}")
        
        company_groups = selected_company_groups if selected_company_groups else pd.concat([
            ly_filtered_df[ly_company_group_col], 
            cy_filtered_df[cy_company_group_col], 
            budget_df[budget_company_group_col]
        ]).dropna().unique().tolist()
        
        st.write(f"Found {len(company_groups)} company groups: {company_groups}")

        if not company_groups:
            st.warning("No valid company groups found in the data. Please check company group columns.")
            return None

        result = {}
        st.write("Processing data by company group...")
        
        for group in company_groups:
            st.write(f"Processing group: {group}")
            # Filter data by company group
            ly_group_df = ly_filtered_df[ly_filtered_df[ly_company_group_col] == group]
            cy_group_df = cy_filtered_df[cy_filtered_df[cy_company_group_col] == group]
            budget_group_df = budget_df[budget_df[budget_company_group_col] == group]
            
            st.write(f"Group {group}: LY rows: {len(ly_group_df)}, CY rows: {len(cy_group_df)}, Budget rows: {len(budget_group_df)}")
            
            # Check if we have data for this group in all datasets
            if ly_group_df.empty or cy_group_df.empty or budget_group_df.empty:
                st.warning(f"Insufficient data for company group '{group}'. Skipping this group.")
                continue

            # Collect products specific to this company group
            ly_products = ly_group_df[ly_product_col].dropna().apply(standardize_name).unique().tolist()
            cy_products = cy_group_df[cy_product_col].dropna().apply(standardize_name).unique().tolist()
            budget_products = budget_group_df[budget_product_group_col].dropna().apply(standardize_name).unique().tolist()
            group_products = sorted(set(ly_products + cy_products + budget_products))
            
            st.write(f"Group {group}: Found {len(group_products)} products")
            
            # Restrict 'Gc' to 'General'
            if group != 'General':
                group_products = [p for p in group_products if p != 'Gc']

            if not group_products:
                st.warning(f"No products found for company group '{group}'. Skipping this group.")
                continue

            # Aggregate data
            st.write(f"Aggregating data for group {group}...")
            try:
                ly_qty = ly_group_df.groupby(ly_product_col)[ly_qty_col].sum().reset_index().rename(columns={ly_product_col: 'PRODUCT NAME', ly_qty_col: 'LY_QTY'})
                cy_qty = cy_group_df.groupby(cy_product_col)[cy_qty_col].sum().reset_index().rename(columns={cy_product_col: 'PRODUCT NAME', cy_qty_col: 'CY_QTY'})
                ly_value = ly_group_df.groupby(ly_product_col)[ly_value_col].sum().reset_index().rename(columns={ly_product_col: 'PRODUCT NAME', ly_value_col: 'LY_VALUE'})
                cy_value = cy_group_df.groupby(cy_product_col)[cy_value_col].sum().reset_index().rename(columns={cy_product_col: 'PRODUCT NAME', cy_value_col: 'CY_VALUE'})
                budget_qty = budget_group_df.groupby(budget_product_group_col)[budget_qty_col].sum().reset_index().rename(columns={budget_product_group_col: 'PRODUCT NAME', budget_qty_col: 'BUDGET_QTY'})
                budget_value = budget_group_df.groupby(budget_product_group_col)[budget_value_col].sum().reset_index().rename(columns={budget_product_group_col: 'PRODUCT NAME', budget_value_col: 'BUDGET_VALUE'})
            except Exception as e:
                st.error(f"Error aggregating data for group {group}: {e}")
                continue

            # Create a DataFrame with all product groups for this company group
            all_products_df = pd.DataFrame({'PRODUCT NAME': group_products})

            # Merge with left join to include all product groups
            qty_df = all_products_df.merge(ly_qty, on='PRODUCT NAME', how='left').merge(cy_qty, on='PRODUCT NAME', how='left').merge(budget_qty, on='PRODUCT NAME', how='left').fillna(0)
            value_df = all_products_df.merge(ly_value, on='PRODUCT NAME', how='left').merge(cy_value, on='PRODUCT NAME', how='left').merge(budget_value, on='PRODUCT NAME', how='left').fillna(0)

            # Define function for calculating achievement percentage
            def calc_achievement(row, cy_col, ly_col):
                if pd.isna(row[ly_col]) or row[ly_col] == 0:
                    return 0.00 if row[cy_col] == 0 else 100.00
                return ((row[cy_col] - row[ly_col]) / row[ly_col]) * 100

            # Calculate achievement percentages without rounding (we'll round all columns together later)
            qty_df['ACHIEVEMENT %'] = qty_df.apply(lambda row: calc_achievement(row, 'CY_QTY', 'LY_QTY'), axis=1)
            value_df['ACHIEVEMENT %'] = value_df.apply(lambda row: calc_achievement(row, 'CY_VALUE', 'LY_VALUE'), axis=1)

            # Round all numeric columns to 2 decimal places
            numeric_cols_qty = ['LY_QTY', 'BUDGET_QTY', 'CY_QTY', 'ACHIEVEMENT %']
            numeric_cols_value = ['LY_VALUE', 'BUDGET_VALUE', 'CY_VALUE', 'ACHIEVEMENT %']
            
            for col in numeric_cols_qty:
                qty_df[col] = qty_df[col].round(2)
            
            for col in numeric_cols_value:
                value_df[col] = value_df[col].round(2)
            
            # Rename columns to uppercase
            qty_df = qty_df[['PRODUCT NAME', 'LY_QTY', 'BUDGET_QTY', 'CY_QTY', 'ACHIEVEMENT %']]
            value_df = value_df[['PRODUCT NAME', 'LY_VALUE', 'BUDGET_VALUE', 'CY_VALUE', 'ACHIEVEMENT %']]

            # Add totals - make sure totals are also rounded
            qty_totals = pd.DataFrame({
                'PRODUCT NAME': ['TOTAL'],
                'LY_QTY': [qty_df['LY_QTY'].sum().round(2)],
                'BUDGET_QTY': [qty_df['BUDGET_QTY'].sum().round(2)],
                'CY_QTY': [qty_df['CY_QTY'].sum().round(2)],
                'ACHIEVEMENT %': [qty_df['ACHIEVEMENT %'].replace([np.inf, -np.inf], 0).mean().round(2) if len(qty_df) > 1 else 0]
            })
            qty_df = pd.concat([qty_df, qty_totals], ignore_index=True)

            value_totals = pd.DataFrame({
                'PRODUCT NAME': ['TOTAL'],
                'LY_VALUE': [value_df['LY_VALUE'].sum().round(2)],
                'BUDGET_VALUE': [value_df['BUDGET_VALUE'].sum().round(2)],
                'CY_VALUE': [value_df['CY_VALUE'].sum().round(2)],
                'ACHIEVEMENT %': [value_df['ACHIEVEMENT %'].replace([np.inf, -np.inf], 0).mean().round(2) if len(value_df) > 1 else 0]
            })
            value_df = pd.concat([value_df, value_totals], ignore_index=True)

            result[group] = {'qty_df': qty_df, 'value_df': value_df}

        if not result:
            st.error("No data available for report. Please check filters and data content.")
            return None

        # Remove debug statements in production
        st.write("Product growth calculation complete!")
        st.write(f"Generated reports for {len(result)} company groups")
        
        return result
    
    except Exception as e:
        st.error(f"Error in product growth calculation: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None
def create_product_growth_ppt(group_results, month_title, logo_file=None):
    """Create a PPT for Product Growth analysis."""
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)
        
        # Create title slide
        create_title_slide(prs, f"Product Growth by Company Group – {month_title}", logo_file)
        
        # Add data slides for each company group
        for group, data in group_results.items():
            qty_df = data['qty_df']
            value_df = data['value_df']
            
            add_table_slide(prs, qty_df, f"{group} - Quantity Growth (Qty in Mt)", percent_cols=[4])
            add_table_slide(prs, value_df, f"{group} - Value Growth (Value in Lakhs)", percent_cols=[4])
        
        # Save to buffer
        ppt_buffer = BytesIO()
        prs.save(ppt_buffer)
        ppt_buffer.seek(0)
        return ppt_buffer
    except Exception as e:
        logger.error(f"Error creating Product Growth PPT: {e}")
        st.error(f"Error creating Product Growth PPT: {e}")
        return None

def tab_product_growth():
    """Product Growth Report Tab."""
    st.header("Product Growth Dashboard")
    
    dfs_info = []
    
    sales_file = st.session_state.sales_file
    budget_file = st.session_state.budget_file
    
    if not sales_file or not budget_file:
        st.warning("Please upload both Sales and Budget files in the sidebar")
        return dfs_info
    
    try:
        # Get sheet names
        sales_sheets = get_excel_sheets(sales_file)
        budget_sheets = get_excel_sheets(budget_file)
        
        if not sales_sheets or not budget_sheets:
            st.error("No sheets found in files")
            return dfs_info
            
        # Sheet selection
        st.subheader("Configure Files")
        col1, col2 = st.columns(2)
        with col1:
            ly_sheet = st.selectbox("Last Year Sales Sheet", sales_sheets, key='pg_ly_sheet')
            ly_header = st.number_input("Last Year Header Row (1-based)", min_value=1, max_value=11, value=2, key='pg_ly_header') - 1
            cy_sheet = st.selectbox("Current Year Sales Sheet", sales_sheets, key='pg_cy_sheet')
            cy_header = st.number_input("Current Year Header Row (1-based)", min_value=1, max_value=11, value=2, key='pg_cy_header') - 1
        with col2:
            budget_sheet = st.selectbox("Budget Sheet", budget_sheets, key='pg_budget_sheet')
            budget_header = st.number_input("Budget Header Row (1-based)", min_value=1, max_value=11, value=2, key='pg_budget_header') - 1
        
        # Load data
        ly_df = pd.read_excel(sales_file, sheet_name=ly_sheet, header=ly_header)
        cy_df = pd.read_excel(sales_file, sheet_name=cy_sheet, header=cy_header)
        budget_df = pd.read_excel(budget_file, sheet_name=budget_sheet, header=budget_header)
        
        # Column mappings
        st.subheader("Last Year Column Mapping")
        ly_cols = ly_df.columns.tolist()
        col1, col2 = st.columns(2)
        with col1:
            ly_date_col = st.selectbox("Date Column", ly_cols, key='pg_ly_date')
            ly_qty_col = st.selectbox("Quantity Column", ly_cols, key='pg_ly_qty')
            ly_value_col = st.selectbox("Value Column", ly_cols, key='pg_ly_value')
        with col2:
            ly_product_col = st.selectbox("Product Group Column", ly_cols, key='pg_ly_product')
            ly_company_group_col = st.selectbox("Company Group Column", ly_cols, key='pg_ly_company_group')
            ly_exec_col = st.selectbox("Executive Column", ly_cols, key='pg_ly_exec')
        
        st.subheader("Current Year Column Mapping")
        cy_cols = cy_df.columns.tolist()
        col1, col2 = st.columns(2)
        with col1:
            cy_date_col = st.selectbox("Date Column", cy_cols, key='pg_cy_date')
            cy_qty_col = st.selectbox("Quantity Column", cy_cols, key='pg_cy_qty')
            cy_value_col = st.selectbox("Value Column", cy_cols, key='pg_cy_value')
        with col2:
            cy_product_col = st.selectbox("Product Group Column", cy_cols, key='pg_cy_product')
            cy_company_group_col = st.selectbox("Company Group Column", cy_cols, key='pg_cy_company_group')
            cy_exec_col = st.selectbox("Executive Column", cy_cols, key='pg_cy_exec')
        
        st.subheader("Budget Column Mapping")
        budget_cols = budget_df.columns.tolist()
        col1, col2 = st.columns(2)
        with col1:
            budget_qty_col = st.selectbox("Quantity Column", budget_cols, key='pg_budget_qty')
            budget_value_col = st.selectbox("Value Column", budget_cols, key='pg_budget_value')
            budget_product_group_col = st.selectbox("Product Group Column", budget_cols, key='pg_budget_product')
        with col2:
            budget_company_group_col = st.selectbox("Company Group Column", budget_cols, key='pg_budget_company_group')
            budget_exec_col = st.selectbox("Executive Column", budget_cols, key='pg_budget_exec')
            # Remove empty company groups and standardize names
        for df, col in [(ly_df, ly_company_group_col), (cy_df, cy_company_group_col), (budget_df, budget_company_group_col)]:
            df[col] = df[col].replace("", np.nan)
            df.dropna(subset=[col], inplace=True)
            df[col] = df[col].apply(standardize_name)
        
        # Get unique standardized company groups
        ly_groups = sorted(ly_df[ly_company_group_col].dropna().unique().tolist())
        cy_groups = sorted(cy_df[cy_company_group_col].dropna().unique().tolist())
        budget_groups = sorted(budget_df[budget_company_group_col].dropna().unique().tolist())
        
        all_company_groups = sorted(set(ly_groups + cy_groups + budget_groups))
        
        # Month selection
        st.subheader("Select Month Range")
        
        # Get available months
        ly_months = pd.to_datetime(ly_df[ly_date_col], dayfirst=True, errors='coerce', format='mixed').dt.strftime('%b %y').dropna().unique().tolist()
        cy_months = pd.to_datetime(cy_df[cy_date_col], dayfirst=True, errors='coerce', format='mixed').dt.strftime('%b %y').dropna().unique().tolist()
        
        if not ly_months or not cy_months:
            st.error("No valid months found in LY or CY data. Please check date columns.")
            return dfs_info
        
        col1, col2 = st.columns(2)
        with col1:
            selected_ly_months = st.multiselect("Last Year Months", sorted(ly_months), key='pg_ly_months')
        with col2:
            selected_cy_months = st.multiselect("Current Year Months", sorted(cy_months), key='pg_cy_months')
        
        # Filters for executives and company groups
        st.subheader("Filter Options")
        filter_tabs = st.tabs(["Executives", "Company Groups"])
        
        # Executive selection
        with filter_tabs[0]:
            all_executives = sorted(set(pd.concat([ly_df[ly_exec_col], cy_df[cy_exec_col], budget_df[budget_exec_col]]).dropna().unique().tolist()))
            
            exec_select_all = st.checkbox("Select All Executives", value=True, key='pg_exec_all')
            if exec_select_all:
                selected_executives = all_executives
            else:
                selected_executives = st.multiselect("Select Executives", all_executives, key='pg_executives')
        
        # Company group selection
        with filter_tabs[1]:
            group_select_all = st.checkbox("Select All Company Groups", value=True, key='pg_group_all')
            if group_select_all:
                selected_company_groups = all_company_groups
            else:
                selected_company_groups = st.multiselect("Select Company Groups", all_company_groups, key='pg_groups')
        
        # Generate report button
        if st.button("Generate Product Growth Report", key='pg_generate'):
            if selected_ly_months and selected_cy_months:
                month_title = f"LY: {', '.join(selected_ly_months)} vs CY: {', '.join(selected_cy_months)}"
                
                group_results = calculate_product_growth(
                    ly_df, cy_df, budget_df, selected_ly_months, selected_cy_months,
                    ly_date_col, cy_date_col, ly_qty_col, cy_qty_col, ly_value_col, cy_value_col,
                    budget_qty_col, budget_value_col, ly_product_col, cy_product_col,
                    ly_company_group_col, cy_company_group_col, budget_company_group_col, budget_product_group_col,
                    ly_exec_col, cy_exec_col, budget_exec_col,
                    selected_executives, selected_company_groups
                )
                
                if group_results:
                    st.subheader("Results")
                    
                    # Display results and collect data for consolidated PPT
                    dfs_info = []
                    
                    for group, data in group_results.items():
                        st.write(f"**{group} - Quantity Growth (Qty in Mt)**")
                        st.dataframe(data['qty_df'])
                        
                        st.write(f"**{group} - Value Growth (Value in Lakhs)**")
                        st.dataframe(data['value_df'])
                        
                        # Add to dfs_info for consolidated PPT
                        dfs_info.append({'df': data['qty_df'], 'title': f"{group} - Quantity Growth (Qty in Mt)", 'percent_cols': [4]})
                        dfs_info.append({'df': data['value_df'], 'title': f"{group} - Value Growth (Value in Lakhs)", 'percent_cols': [4]})
                    
                    # Store results in session state
                    st.session_state.product_results = dfs_info
                    
                    # Create individual PPT
                    ppt_buffer = create_product_growth_ppt(group_results, month_title, st.session_state.logo_file)
                    
                    if ppt_buffer:
                        unique_id = str(uuid.uuid4())[:8]
                        st.download_button(
                            label="Download Product Growth PPT",
                            data=ppt_buffer,
                            file_name=f"Product_Growth_{month_title.replace(', ', '_')}_{unique_id}.pptx",
                            mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                            key=f"pg_download_{unique_id}"
                        )
                else:
                    st.error("Failed to generate Product Growth report. Check your data and selections.")
            else:
                st.warning("Select at least one month for LY and CY.")
    except Exception as e:
        st.error(f"Error in Product Growth tab: {e}")
        logger.error(f"Error in Product Growth tab: {e}")
    
    return dfs_info
#######################################################
# SIDEBAR AND MAIN APPLICATION
#######################################################

def sidebar_ui():
    """Sidebar UI for file uploads and app info."""
    with st.sidebar:
        st.title("ACCLLP Dashboard")
        st.subheader("File Uploads")
        
        # Sales file upload
        sales_file = st.file_uploader("Upload Sales Excel File", type=["xlsx"], key="upload_sales")
        if sales_file:
            st.session_state.sales_file = sales_file
            st.success("✅ Sales file uploaded")
        
        # Budget file upload
        budget_file = st.file_uploader("Upload Budget Excel File", type=["xlsx"], key="upload_budget")
        if budget_file:
            st.session_state.budget_file = budget_file
            st.success("✅ Budget file uploaded")
        
        # OS Jan file upload
        os_jan_file = st.file_uploader("Upload OS-First Excel File", type=["xlsx"], key="upload_os_jan")
        if os_jan_file:
            st.session_state.os_jan_file = os_jan_file
            st.success("✅ OS-First file uploaded")
        
        # OS Feb file upload
        os_feb_file = st.file_uploader("Upload OS-Second Excel File", type=["xlsx"], key="upload_os_feb")
        if os_feb_file:
            st.session_state.os_feb_file = os_feb_file
            st.success("✅ OS-Second file uploaded")
        
        # Logo file upload
        logo_file = st.file_uploader("Upload Logo (Optional)", type=["png", "jpg", "jpeg"], key="upload_logo")
        if logo_file:
            st.session_state.logo_file = logo_file
            st.image(logo_file, width=100, caption="Logo Preview")
            st.success("✅ Logo uploaded")
        
        st.divider()

def main():
    """Main application function."""
    sidebar_ui()
    
    st.title("ACCLLP Integrated Dashboard")
    
    # Check if files are uploaded and show appropriate messages
    if (not st.session_state.sales_file or 
        not st.session_state.budget_file or 
        not st.session_state.os_jan_file or 
        not st.session_state.os_feb_file):
        st.warning("Please upload all required files in the sidebar to access full functionality")
        
        # Show file status
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Required Files:")
            st.markdown(f"- Sales File: {'✅ Uploaded' if st.session_state.sales_file else '❌ Missing'}")
            st.markdown(f"- Budget File: {'✅ Uploaded' if st.session_state.budget_file else '❌ Missing'}")
        with col2:
            st.markdown("####  ")
            st.markdown(f"- OS-First File: {'✅ Uploaded' if st.session_state.os_jan_file else '❌ Missing'}")  # Changed label only
            st.markdown(f"- OS-Second File: {'✅ Uploaded' if st.session_state.os_feb_file else '❌ Missing'}")  # Changed label only
    
    # Create tabs for each module
    tabs = st.tabs([
        "Budget vs Billed",     # First tab
        "OD Target vs Collection",  # Second tab 
        "Product Growth",       # Third tab
        "Number of Billed Customers"  # Fourth tab
    ])
    
    # Tab content
    with tabs[0]:
        budget_dfs = tab_budget_vs_billed()
    
    with tabs[1]:
        od_dfs = tab_od_target()
    
    with tabs[2]:
        product_dfs = tab_product_growth()
    
    with tabs[3]:
        customers_dfs = tab_billed_customers()
    
    # Add a consolidated PPT download option if any reports have been generated
    st.divider()
    st.subheader("Consolidated Report")

    # Collect all available reports from session state in the same order as tabs
    all_dfs_info = []
    collected_sections = []

    # 1. Budget vs Billed (same as first tab)
    if hasattr(st.session_state, 'budget_results') and st.session_state.budget_results:
        all_dfs_info.extend(st.session_state.budget_results)
        collected_sections.append(f"Budget: {len(st.session_state.budget_results)} reports")

    # 2. OD Target vs Collection (same as second tab)
    if hasattr(st.session_state, 'od_results') and st.session_state.od_results:
        # Check if it's already a list or a dictionary
        if isinstance(st.session_state.od_results, list):
            all_dfs_info.extend(st.session_state.od_results)
            collected_sections.append(f"OD: {len(st.session_state.od_results)} reports")
        elif isinstance(st.session_state.od_results, dict) and 'df' in st.session_state.od_results:
            all_dfs_info.append({
                'df': st.session_state.od_results['df'], 
                'title': st.session_state.od_results.get('title', 'OD Target')
            })
            collected_sections.append("OD: 1 report")

    # 3. Product Growth (same as third tab)
    if hasattr(st.session_state, 'product_results') and st.session_state.product_results:
        all_dfs_info.extend(st.session_state.product_results)
        collected_sections.append(f"Product: {len(st.session_state.product_results)} reports")

    # 4. Number of Billed Customers (same as fourth tab)
    if hasattr(st.session_state, 'customers_results') and st.session_state.customers_results:
        all_dfs_info.extend(st.session_state.customers_results)
        collected_sections.append(f"Customers: {len(st.session_state.customers_results)} reports")

    # Add NBC results if separate from customers_results
    if hasattr(st.session_state, 'nbc_results') and isinstance(st.session_state.nbc_results, dict) and 'df' in st.session_state.nbc_results:
        all_dfs_info.append({
            'df': st.session_state.nbc_results['df'], 
            'title': st.session_state.nbc_results.get('title', 'Number of Billed Customers')
        })
        collected_sections.append("NBC: 1 report")

    # Display consolidated report options
    if all_dfs_info:
        st.info(f"Reports collected: {', '.join(collected_sections)}")
        title = st.text_input("Enter Consolidated Report Title", "ACCLLP Consolidated Report")
        
        if st.button("Generate Consolidated PPT"):
            with st.spinner("Creating consolidated PowerPoint..."):
                consolidated_ppt = create_consolidated_ppt(
                    all_dfs_info,
                    st.session_state.logo_file,
                    title
                )
                
                if consolidated_ppt:
                    unique_id = str(uuid.uuid4())[:8]
                    st.success(f"PowerPoint created with {len(all_dfs_info)} slides!")
                    st.download_button(
                        label="Download Consolidated PPT",
                        data=consolidated_ppt,
                        file_name=f"ACCLLP_Consolidated_Report_{unique_id}.pptx",
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"consolidated_download_{unique_id}"
                    )
                else:
                    st.error("Failed to create consolidated PowerPoint. Check the reports data.")
    else:
        st.info("Generate at least one report from any tab to enable the consolidated report option")

if __name__ == "__main__":
    main()
