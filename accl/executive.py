import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.table import Table
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.dml.color import RGBColor
from io import BytesIO
import math
import traceback
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize session state variables
if 'sales_file' not in st.session_state:
    st.session_state.sales_file = None
if 'budget_file' not in st.session_state:
    st.session_state.budget_file = None
if 'os_jan_file' not in st.session_state:
    st.session_state.os_jan_file = None
if 'os_feb_file' not in st.session_state:
    st.session_state.os_feb_file = None
if 'logo_file' not in st.session_state:
    st.session_state.logo_file = None

# Standardized branch mapping across all modules
nbc_branch_mapping = {
    'PONDY': 'PONDY', 'PDY': 'PONDY', 'PUDUCHERRY': 'PONDY',
    'COVAI': 'ERODE_CBE_KRR', 'ERODE': 'ERODE_CBE_KRR', 'KARUR': 'ERODE_CBE_KRR',
    'SLM002': 'SALEM', 'SLMTD1': 'SALEM', 'BHV1': 'BHAVANI',
    'MDU': 'MADURAI',
    'CHENNAI': 'CHENNAI', 'CHN': 'CHENNAI',
    'TIRUPUR': 'TIRUPUR', 'TPR': 'TIRUPUR',
    'POULTRY': 'POULTRY'
}

#######################################################
# SHARED UTILITY FUNCTIONS
#######################################################

def extract_executive_name(executive):
    """Normalize executive names, treating null/empty as 'BLANK'."""
    if pd.isna(executive) or str(executive).strip() == '':
        return 'BLANK'  # Treat null/empty as 'BLANK'
    return str(executive).strip().upper()  # Standardize to uppercase

def get_excel_sheets(file):
    """Get sheet names from an Excel file."""
    try:
        xl = pd.ExcelFile(file)
        return xl.sheet_names
    except Exception as e:
        st.error(f"Error reading Excel sheets: {e}")
        return []

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
    """Add a slide with a table from a DataFrame.
    
    Args:
        prs: PowerPoint presentation object
        df: DataFrame to convert to table
        title: Slide title
        percent_cols: List of column indices that should be formatted as percentages
    """
    if percent_cols is None:
        percent_cols = []
    
    # Use blank layout
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)
    
    # Add title
    title_shape = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(12.33), Inches(0.8))
    title_frame = title_shape.text_frame
    title_frame.text = title
    p = title_frame.paragraphs[0]
    p.font.size = Pt(28)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 112, 192)
    p.alignment = PP_ALIGN.CENTER
    
    # Get the columns and rows for table
    columns = df.columns.tolist()
    num_rows = len(df) + 1  # +1 for header row
    num_cols = len(columns)
    
    # Create table
    table = slide.shapes.add_table(num_rows, num_cols, Inches(0.5), Inches(1.5), Inches(12), Inches(0.3 * len(df) + 0.3)).table
    
    # Set column widths based on content type
    # Adjust the first column (usually an index or name) to be wider
    if num_cols > 0:
        table.columns[0].width = Inches(3.0)
    
    # Distribute remaining width among other columns
    remaining_width = 12.0 - 3.0  # 12 inches total width minus first column
    if num_cols > 1:
        col_width = remaining_width / (num_cols - 1)
        for i in range(1, num_cols):
            table.columns[i].width = Inches(col_width)
    
    # Add header row
    for i, col_name in enumerate(columns):
        cell = table.cell(0, i)
        cell.text = str(col_name)
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0, 112, 192)
        cell.text_frame.paragraphs[0].font.size = Pt(14)
        cell.text_frame.paragraphs[0].font.bold = True
        cell.text_frame.paragraphs[0].font.color.rgb = RGBColor(255, 255, 255)
        cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
    
    # Add data rows
    for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
        is_total_row = 'TOTAL' in str(row.iloc[0])
        
        for col_idx, col_name in enumerate(columns):
            cell = table.cell(row_idx, col_idx)
            value = row[col_name]
            
            # Format percentages
            if col_idx in percent_cols and isinstance(value, (int, float)) and not pd.isna(value):
                cell.text = f"{value}%"
            else:
                cell.text = str(value)
            
            # Format cell
            cell.text_frame.paragraphs[0].font.size = Pt(12)
            cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            
            # Set background color
            cell.fill.solid()
            if is_total_row:
                cell.fill.fore_color.rgb = RGBColor(211, 211, 211)  # Light gray for total row
                cell.text_frame.paragraphs[0].font.bold = True
            else:
                if row_idx % 2 == 0:
                    cell.fill.fore_color.rgb = RGBColor(221, 235, 247)  # Light blue
                else:
                    cell.fill.fore_color.rgb = RGBColor(255, 255, 255)  # White

def create_table_image(df, title, percent_cols=None):
    """Create a table image from a DataFrame."""
    if percent_cols is None:
        percent_cols = []
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, len(df) * 0.5))
    ax.axis('off')
    
    # Create table
    columns = df.columns.tolist()
    rows = len(df)
    ncols = len(columns)
    
    table = Table(ax, bbox=[0, 0, 1, 1])
    
    # Add header row
    for col_idx, col_name in enumerate(columns):
        table.add_cell(0, col_idx, 1.0/ncols, 1.0/rows, text=col_name, loc='center', facecolor='#0070C0')
        table[0, col_idx].set_text_props(weight='bold', color='white', fontsize=12)
    
    # Add data rows
    for row_idx in range(len(df)):
        for col_idx, col_name in enumerate(columns):
            value = df.iloc[row_idx, col_idx]
            
            # Format percentages
            if col_idx in percent_cols and isinstance(value, (int, float)) and not pd.isna(value):
                text = f"{value}%"
            else:
                text = str(value)
            
            # Set background color
            is_total_row = 'TOTAL' in str(df.iloc[row_idx, 0])
            if is_total_row:
                facecolor = '#D3D3D3'  # Light gray
                fontweight = 'bold'
            else:
                facecolor = '#DDEBF7' if row_idx % 2 == 0 else 'white'
                fontweight = 'normal'
            
            # Add cell
            table.add_cell(row_idx + 1, col_idx, 1.0/ncols, 1.0/rows, text=text, loc='center', facecolor=facecolor)
            table[row_idx + 1, col_idx].set_text_props(fontsize=10, weight=fontweight)
    
    # Set column widths - make first column wider
    if ncols > 0:
        table[(0, 0)].width = 0.25
    
    # Distribute remaining width
    if ncols > 1:
        remaining_width = 0.75  # 1.0 - 0.25
        col_width = remaining_width / (ncols - 1)
        for i in range(1, ncols):
            table[(0, i)].width = col_width
    
    # Set font size and add table to axis
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    ax.add_table(table)
    
    # Add title
    plt.suptitle(title, fontsize=16, weight='bold', color='#0070C0', y=1.02)
    
    # Save to buffer
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=150)
    plt.close()
    
    # Reset buffer position for reading
    img_buffer.seek(0)
    return img_buffer

def create_consolidated_ppt(dfs_info, logo_file=None, title="Consolidated Report"):
    """Create a consolidated PPT from multiple DataFrames.
    
    Args:
        dfs_info: List of dicts with keys 'df', 'title', and 'percent_cols'
        logo_file: Optional logo file to add to title slide
        title: Title for the presentation
    
    Returns:
        BytesIO buffer containing the PPT
    """
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)
        
        # Create title slide
        create_title_slide(prs, title, logo_file)
        
        # Add data slides
        for df_info in dfs_info:
            df = df_info['df']
            slide_title = df_info['title']
            percent_cols = df_info.get('percent_cols', [])
            
            add_table_slide(prs, df, slide_title, percent_cols)
        
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
# MODULE 1: BUDGET VS BILLED (From budget2.py)
#######################################################

def calculate_budget_values(sales_df, budget_df, selected_month, sales_executives,
                           sales_date_col, sales_area_col, sales_value_col, sales_qty_col, sales_product_group_col, sales_sl_code_col, sales_exec_col,
                           budget_area_col, budget_value_col, budget_qty_col, budget_product_group_col, budget_sl_code_col, budget_exec_col):
    """Calculate Budget vs Billed metrics."""
    try:
        # Convert columns to proper types
        sales_df[sales_date_col] = pd.to_datetime(sales_df[sales_date_col], dayfirst=True, errors='coerce')
        sales_df[sales_value_col] = pd.to_numeric(sales_df[sales_value_col], errors='coerce').fillna(0)
        sales_df[sales_qty_col] = pd.to_numeric(sales_df[sales_qty_col], errors='coerce').fillna(0)
        
        budget_df[budget_value_col] = pd.to_numeric(budget_df[budget_value_col], errors='coerce').fillna(0)
        budget_df[budget_qty_col] = pd.to_numeric(budget_df[budget_qty_col], errors='coerce').fillna(0)
        
        # Filter sales data by selected month
        filtered_sales_df = sales_df[sales_df[sales_date_col].dt.strftime('%b %y') == selected_month].copy()
        if filtered_sales_df.empty:
            st.error(f"No sales data found for {selected_month}")
            return None, None, None, None
        
        # Filter by selected executives
        filtered_sales_df = filtered_sales_df[filtered_sales_df[sales_exec_col].isin(sales_executives)]
        if filtered_sales_df.empty:
            st.error(f"No sales data found for selected executives")
            return None, None, None, None
        
        # Process budget data: Group by executive, SL Code, Product Group
        budget_grouped_value = budget_df.groupby([budget_sl_code_col, budget_product_group_col, budget_exec_col])[budget_value_col].sum().reset_index()
        budget_grouped_qty = budget_df.groupby([budget_sl_code_col, budget_product_group_col, budget_exec_col])[budget_qty_col].sum().reset_index()
        
        # Sum budget values by executive
        budget_value_per_exec = budget_grouped_value.groupby(budget_exec_col)[budget_value_col].sum().reset_index()
        budget_value_per_exec.columns = ['Executive', 'Budget Value']
        
        budget_qty_per_exec = budget_grouped_qty.groupby(budget_exec_col)[budget_qty_col].sum().reset_index()
        budget_qty_per_exec.columns = ['Executive', 'Budget Qty']
        
        # Group budget data by SL Code and Product Group
        budget_grouped_value_pg = budget_df.groupby([budget_sl_code_col, budget_product_group_col])[budget_value_col].sum().reset_index()
        budget_grouped_value_pg.columns = ['SL Code', 'Product Group', 'Budget Value']
        
        budget_grouped_qty_pg = budget_df.groupby([budget_sl_code_col, budget_product_group_col])[budget_qty_col].sum().reset_index()
        budget_grouped_qty_pg.columns = ['SL Code', 'Product Group', 'Budget Qty']
        
        # Slide 2: Budget Against Billed Value (SL Code + Product Group)
        # Add Product Group to sales_df from budget mapping
        sales_value_with_pg = pd.merge(filtered_sales_df,
                                     budget_grouped_value_pg[['SL Code', 'Product Group']],
                                     left_on=[sales_sl_code_col, sales_product_group_col],
                                     right_on=['SL Code', 'Product Group'],
                                     how='inner')
        
        # Sum Billed Value by executive
        sales_grouped_value = (sales_value_with_pg.groupby(sales_exec_col)
                             .agg({sales_value_col: 'sum'})
                             .reset_index()
                             .rename(columns={sales_exec_col: 'Executive', sales_value_col: 'Billed Value'}))
        
        # Merge with budget values and calculate percentage
        budget_vs_billed_value_df = pd.DataFrame({'Executive': sales_executives})
        budget_vs_billed_value_df = pd.merge(budget_vs_billed_value_df, budget_value_per_exec, on='Executive', how='left').fillna({'Budget Value': 0})
        budget_vs_billed_value_df = pd.merge(budget_vs_billed_value_df, sales_grouped_value, on='Executive', how='left').fillna({'Billed Value': 0})
        
        # Cap Billed Value at Budget Value for each executive
        budget_vs_billed_value_df['Billed Value'] = budget_vs_billed_value_df.apply(
            lambda row: row['Budget Value'] if row['Billed Value'] > row['Budget Value'] and row['Billed Value'] >= 0 else row['Billed Value'], axis=1)
        
        budget_vs_billed_value_df['%'] = budget_vs_billed_value_df.apply(
            lambda row: int((row['Billed Value'] / row['Budget Value'] * 100)) if row['Budget Value'] != 0 else 0, axis=1)
        
        # Add TOTAL row
        total_budget = budget_vs_billed_value_df['Budget Value'].sum()
        total_billed = budget_vs_billed_value_df['Billed Value'].sum()
        total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
        total_row = pd.DataFrame({'Executive': ['TOTAL'], 'Budget Value': [total_budget], 'Billed Value': [total_billed], '%': [total_percentage]})
        budget_vs_billed_value_df = pd.concat([budget_vs_billed_value_df, total_row], ignore_index=True)
        
        # Round values for display
        budget_vs_billed_value_df['Budget Value'] = budget_vs_billed_value_df['Budget Value'].round(2)
        budget_vs_billed_value_df['Billed Value'] = budget_vs_billed_value_df['Billed Value'].round(2)
        
        # Slide 3: Budget Against Billed Quantity (SL Code + Product Group)
        sales_qty_with_pg = pd.merge(filtered_sales_df,
                                 budget_grouped_qty_pg[['SL Code', 'Product Group']],
                                 left_on=[sales_sl_code_col, sales_product_group_col],
                                 right_on=['SL Code', 'Product Group'],
                                 how='inner')
    
        # Sum Billed Qty by executive
        sales_grouped_qty = (sales_qty_with_pg.groupby(sales_exec_col)
                         .agg({sales_qty_col: 'sum'})
                         .reset_index()
                         .rename(columns={sales_exec_col: 'Executive', sales_qty_col: 'Billed Qty'}))

        # Merge with budget quantities and cap at executive level
        budget_vs_billed_qty_df = pd.DataFrame({'Executive': sales_executives})
        budget_vs_billed_qty_df = pd.merge(budget_vs_billed_qty_df, budget_qty_per_exec, on='Executive', how='left').fillna({'Budget Qty': 0})
        budget_vs_billed_qty_df = pd.merge(budget_vs_billed_qty_df, sales_grouped_qty, on='Executive', how='left').fillna({'Billed Qty': 0})
        
        # Cap Billed Qty at Budget Qty for each executive
        budget_vs_billed_qty_df['Billed Qty'] = budget_vs_billed_qty_df.apply(
            lambda row: row['Budget Qty'] if row['Billed Qty'] > row['Budget Qty'] and row['Billed Qty'] >= 0 else row['Billed Qty'], axis=1)
        
        budget_vs_billed_qty_df['%'] = budget_vs_billed_qty_df.apply(
            lambda row: int((row['Billed Qty'] / row['Budget Qty'] * 100)) if row['Budget Qty'] != 0 else 0, axis=1)
        total_budget = budget_vs_billed_qty_df['Budget Qty'].sum()
        total_billed = budget_vs_billed_qty_df['Billed Qty'].sum()
        total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
        total_row = pd.DataFrame({'Executive': ['TOTAL'], 'Budget Qty': [total_budget], 'Billed Qty': [total_billed], '%': [total_percentage]})
        budget_vs_billed_qty_df = pd.concat([budget_vs_billed_qty_df, total_row], ignore_index=True)
        budget_vs_billed_qty_df['Budget Qty'] = budget_vs_billed_qty_df['Budget Qty'].round(2)
        budget_vs_billed_qty_df['Billed Qty'] = budget_vs_billed_qty_df['Billed Qty'].round(2)

        # Slide 4: Overall Sales Quantity (SL Code only, no Product Group, unchanged)
        sales_grouped_qty_sl = (filtered_sales_df.groupby(sales_exec_col)
                            .agg({sales_qty_col: 'sum'})
                            .reset_index()
                            .rename(columns={sales_exec_col: 'Executive', sales_qty_col: 'Billed Qty'}))

        overall_sales_qty_df = pd.DataFrame({'Executive': sales_executives})
        overall_sales_qty_df = pd.merge(overall_sales_qty_df, budget_qty_per_exec, on='Executive', how='left').fillna({'Budget Qty': 0})
        overall_sales_qty_df = pd.merge(overall_sales_qty_df, sales_grouped_qty_sl, on='Executive', how='left').fillna({'Billed Qty': 0})
        
        # Round Budget Qty and Billed Qty before calculating percentage
        overall_sales_qty_df['Budget Qty'] = overall_sales_qty_df['Budget Qty'].round(2)
        overall_sales_qty_df['Billed Qty'] = overall_sales_qty_df['Billed Qty'].round(2)
        
        # Calculate percentage after rounding
        overall_sales_qty_df['%'] = overall_sales_qty_df.apply(
            lambda row: int((row['Billed Qty'] / row['Budget Qty'] * 100)) if row['Budget Qty'] != 0 else 0, axis=1)
        
        total_budget = overall_sales_qty_df['Budget Qty'].sum()
        total_billed = overall_sales_qty_df['Billed Qty'].sum()
        total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
        total_row = pd.DataFrame({'Executive': ['TOTAL'], 'Budget Qty': [total_budget], 'Billed Qty': [total_billed], '%': [total_percentage]})
        overall_sales_qty_df = pd.concat([overall_sales_qty_df, total_row], ignore_index=True)

        # Slide 5: Overall Sales Value (SL Code only, no Product Group, unchanged)
        sales_grouped_value_sl = (filtered_sales_df.groupby(sales_exec_col)
                              .agg({sales_value_col: 'sum'})
                              .reset_index()
                              .rename(columns={sales_exec_col: 'Executive', sales_value_col: 'Billed Value'}))

        overall_sales_value_df = pd.DataFrame({'Executive': sales_executives})
        overall_sales_value_df = pd.merge(overall_sales_value_df, budget_value_per_exec, on='Executive', how='left').fillna({'Budget Value': 0})
        overall_sales_value_df = pd.merge(overall_sales_value_df, sales_grouped_value_sl, on='Executive', how='left').fillna({'Billed Value': 0})
        
        # Round Budget Value and Billed Value before calculating percentage
        overall_sales_value_df['Budget Value'] = overall_sales_value_df['Budget Value'].round(0).astype(int)
        overall_sales_value_df['Billed Value'] = overall_sales_value_df['Billed Value'].round(0).astype(int)
        
        # Calculate percentage after rounding
        overall_sales_value_df['%'] = overall_sales_value_df.apply(
            lambda row: int((row['Billed Value'] / row['Budget Value'] * 100)) if row['Budget Value'] != 0 else 0, axis=1)
        
        total_budget = overall_sales_value_df['Budget Value'].sum()
        total_billed = overall_sales_value_df['Billed Value'].sum()
        total_percentage = int((total_billed / total_budget * 100)) if total_budget != 0 else 0
        total_row = pd.DataFrame({'Executive': ['TOTAL'], 'Budget Value': [total_budget], 'Billed Value': [total_billed], '%': [total_percentage]})
        overall_sales_value_df = pd.concat([overall_sales_value_df, total_row], ignore_index=True)
        
        return budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df
    except Exception as e:
        logger.error(f"Error in calculate_budget_values: {e}")
        logger.error(traceback.format_exc())
        st.error(f"Error calculating budget values: {e}")
        return None, None, None, None

def create_budget_ppt(budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df, month_title=None, logo_file=None):
    """Create a PPT presentation with budget vs billed data."""
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)

        # Create title slide
        create_title_slide(prs, f"Monthly Review Meeting – {month_title}", logo_file)
        
        # Helper function to split DataFrame if needed
        def process_df_for_slides(df, title_base):
            # Exclude "ACCLP" row
            df = df[df['Executive'] != "ACCLP"].copy()
            
            # Check if we need to split (more than 12 executives)
            num_executives = df[df['Executive'] != 'TOTAL'].shape[0]
            split_threshold = 12
            
            if num_executives <= split_threshold:
                add_table_slide(prs, df, title_base, percent_cols=[3])
                return
            
            # Extract data rows and total row
            data_rows = df[df['Executive'] != 'TOTAL'].copy()
            total_row = df[df['Executive'] == 'TOTAL'].copy()
            
            # Calculate split point
            split_point = math.ceil(num_executives / 2)
            
            # Split into parts
            part1 = data_rows.iloc[:split_point].copy()
            part2 = data_rows.iloc[split_point:].copy()
            
            # Calculate part totals and add them
            for i, part in enumerate([part1, part2], 1):
                # Calculate total for this part
                part_total = {}
                for col in df.columns:
                    if col == 'Executive':
                        part_total[col] = f'PART {i} TOTAL'
                    elif col == '%':
                        budget_col = 'Budget Value' if 'Budget Value' in df.columns else 'Budget Qty'
                        billed_col = 'Billed Value' if 'Billed Value' in df.columns else 'Billed Qty'
                        budget_sum = part[budget_col].sum()
                        billed_sum = part[billed_col].sum()
                        part_total[col] = int((billed_sum / budget_sum * 100)) if budget_sum != 0 else 0
                    else:
                        part_total[col] = part[col].sum()
                
                # Add part total row
                part_with_total = pd.concat([part, pd.DataFrame([part_total])], ignore_index=True)
                
                # Add slide for this part
                add_table_slide(prs, part_with_total, f"{title_base} - Part {i}", percent_cols=[3])
            
            # Add final slide with grand total
            add_table_slide(prs, total_row, f"{title_base} - Grand Total", percent_cols=[3])

        # Add data slides
        process_df_for_slides(budget_vs_billed_qty_df, "BUDGET AGAINST BILLED (Qty in Mt)")
        process_df_for_slides(budget_vs_billed_value_df, "BUDGET AGAINST BILLED (Value in Lakhs)")
        process_df_for_slides(overall_sales_qty_df, "OVERALL SALES (Qty in Mt)")
        process_df_for_slides(overall_sales_value_df, "OVERALL SALES (Value in Lakhs)")

        # Save to buffer
        ppt_buffer = BytesIO()
        prs.save(ppt_buffer)
        ppt_buffer.seek(0)
        return ppt_buffer
    except Exception as e:
        logger.error(f"Error creating Budget PPT: {e}")
        st.error(f"Error creating Budget PPT: {e}")
        return None

#######################################################
# MODULE 2: NUMBER OF BILLED CUSTOMERS (From nbc2.py)
#######################################################

def determine_financial_year(date):
    """Determine the financial year for a given date (April to March)."""
    year = date.year
    month = date.month
    if month >= 4:
        return f"{year % 100}-{year % 100 + 1}"  # e.g., April 2024 -> 24-25
    else:
        return f"{year % 100 - 1}-{year % 100}"  # e.g., February 2025 -> 24-25

def create_customer_table(sales_df, date_col, branch_col, customer_id_col, executive_col, selected_branches=None, selected_executives=None):
    """Create a table of unique customer counts per executive and month, based on data in the selected sheet."""
    sales_df = sales_df.copy()

    # Validate columns
    if date_col not in sales_df.columns:
        st.error(f"Date column '{date_col}' not found in sales data.")
        return None
    if branch_col not in sales_df.columns:
        st.error(f"Branch column '{branch_col}' not found in sales data.")
        return None
    if customer_id_col not in sales_df.columns:
        st.error(f"Customer ID column '{customer_id_col}' not found in sales data.")
        return None
    if executive_col not in sales_df.columns:
        st.error(f"Executive column '{executive_col}' not found in sales data.")
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

    # Extract financial year from the data
    sales_df['Financial_Year'] = sales_df[date_col].apply(determine_financial_year)
    
    # Identify all financial years in the data
    available_financial_years = sales_df['Financial_Year'].dropna().unique()
    if len(available_financial_years) == 0:
        st.error("No valid financial years found in the data.")
        return None
    
    # Process data for each financial year
    result_dict = {}
    
    for fin_year in available_financial_years:
        # Filter data for the current financial year
        fy_df = sales_df[sales_df['Financial_Year'] == fin_year].copy()
        if fy_df.empty:
            continue
        
        # Filter out invalid dates
        fy_df = fy_df[fy_df[date_col].notna()].copy()
        if fy_df.empty:
            continue

        # Extract month and year
        fy_df['Month'] = fy_df[date_col].dt.month
        fy_df['Year'] = fy_df[date_col].dt.year

        # Define month mapping with dynamic year suffix for January-March
        year_suffix = fin_year.split('-')[1]  # Get the second year from e.g., "24-25"
        month_mapping = {
            4: 'APRIL', 5: 'MAY', 6: 'JUNE', 7: 'JULY', 8: 'AUGUST', 9: 'SEPTEMBER',
            10: 'OCTOBER', 11: 'NOVEMBER', 12: 'DECEMBER', 
            1: f'JAN20{year_suffix}', 2: f'FEB20{year_suffix}', 3: f'MARCH'
        }

        # Create Month_Name column
        fy_df['Month_Name'] = fy_df.apply(
            lambda row: month_mapping[row['Month']] if row['Month'] in month_mapping else None,
            axis=1
        )

        # Get unique months that exist in the data
        available_months = fy_df['Month_Name'].dropna().unique()
        if len(available_months) == 0:
            continue
        
        # Create a chronological ordering of the available months
        # Start with April-December
        chronological_months = []
        for month_num in range(4, 13):
            month_name = month_mapping.get(month_num)
            if month_name in available_months:
                chronological_months.append(month_name)
        
        # Then January-March of the next year
        for month_num in range(1, 4):
            month_name = month_mapping.get(month_num)
            if month_name in available_months:
                chronological_months.append(month_name)

        # Standardize branch names with error handling
        try:
            fy_df['Raw_Branch'] = fy_df[branch_col].astype(str).str.upper()
        except Exception as e:
            st.error(f"Error processing branch column '{branch_col}': {e}.")
            continue
            
        # Only include this line if nbc_branch_mapping is defined elsewhere
        if 'nbc_branch_mapping' in globals():
            fy_df['Mapped_Branch'] = fy_df['Raw_Branch'].replace(nbc_branch_mapping)
        else:
            fy_df['Mapped_Branch'] = fy_df['Raw_Branch']

        # Apply branch filter if selected
        if selected_branches:
            fy_df = fy_df[fy_df['Mapped_Branch'].isin(selected_branches)]
            if fy_df.empty:
                continue

        # Apply executive filter if selected
        if selected_executives:
            fy_df = fy_df[fy_df[executive_col].isin(selected_executives)]
            if fy_df.empty:
                continue

        # Determine executives to display
        executives_to_display = selected_executives if selected_executives else sorted(fy_df[executive_col].dropna().unique())
        if not executives_to_display:
            continue

        # Calculate unique customer counts by executive and month
        grouped_df = fy_df.groupby([executive_col, 'Month_Name'])[customer_id_col].nunique().reset_index(name='Count')
        pivot_df = grouped_df.pivot_table(
            values='Count',
            index=executive_col,
            columns='Month_Name',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        # Initialize result DataFrame with executives
        result_df = pd.DataFrame({'Executive Name': executives_to_display})
        result_df = pd.merge(result_df, pivot_df, left_on='Executive Name', right_on=executive_col, how='left').fillna(0)
        result_df = result_df.drop(columns=[executive_col] if executive_col in result_df.columns else [])

        # Only include columns for months that are actually present in the data
        # Reorder columns based on chronological month order
        actual_month_columns = [month for month in chronological_months if month in result_df.columns]
        all_columns = ['Executive Name'] + actual_month_columns
        result_df = result_df[all_columns]

        # Add S.No column as string type to prevent Arrow conversion issues
        result_df.insert(0, 'S.No', [str(i) for i in range(1, len(result_df) + 1)])
        
        # Add GRAND TOTAL row
        total_row = {'S.No': '0', 'Executive Name': 'GRAND TOTAL'}
        for month in actual_month_columns:
            if month in result_df.columns:
                total_row[month] = result_df[month].sum()
        result_df = pd.concat([result_df, pd.DataFrame([total_row])], ignore_index=True)

        # Convert to integers for month columns only
        for col in actual_month_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].round(0).astype(int)
        
        # Add this financial year's result to the dictionary
        result_dict[fin_year] = (result_df, actual_month_columns)
    
    return result_dict

def create_customer_table_image(df, title, sorted_months, financial_year):
    """Create a table image from the DataFrame with executives."""
    fig, ax = plt.subplots(figsize=(14, len(df) * 0.6))
    ax.axis('off')
    columns = list(df.columns)
    
    # Only check for columns that actually exist in the data
    expected_columns = {'S.No', 'Executive Name'}.union(set(sorted_months))
    actual_columns = set(columns)
    if not {'S.No', 'Executive Name'}.issubset(actual_columns):
        st.warning(f"Missing essential columns in customer DataFrame for image: S.No or Executive Name")
        return BytesIO()

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
    table[(0, 1)].width = 0.25  # Increased for longer executive names
    for col_idx in range(2, ncols):
        table[(0, col_idx)].width = 0.07
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    ax.add_table(table)
    plt.suptitle(title, fontsize=14, weight='bold', color='#0070C0', y=1.02)
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=150)
    plt.close()
    return img_buffer

def create_customer_ppt_slide(slide, df, title, sorted_months, is_last_slide=False):
    """Add a slide with the billed customers table by executive."""
    if df.empty or len(df.columns) < 2:
        st.warning(f"Skipping customer slide: DataFrame is empty or has insufficient columns {df.columns.tolist()}")
        return

    # Add title
    title_shape = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(12.33), Inches(0.8))
    title_frame = title_shape.text_frame
    title_frame.text = title
    p = title_frame.paragraphs[0]
    p.font.size = Pt(24)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 112, 192)
    p.alignment = PP_ALIGN.CENTER

    # Use the actual columns from the DataFrame
    columns = list(df.columns)
    
    # Only verify essential columns
    if 'S.No' not in columns or 'Executive Name' not in columns:
        st.warning(f"Missing essential columns in customer DataFrame: S.No or Executive Name")
        return

    # Table dimensions: include header row
    num_rows = len(df) + 1  # Data rows + header row
    num_cols = len(columns)

    table_width = Inches(12.0)
    table_height = Inches(0.3 * len(df) + 0.3)
    left = Inches(0.65)
    top = Inches(1.2)
    table = slide.shapes.add_table(num_rows, num_cols, left, top, table_width, table_height).table

    # Set column widths
    col_widths = [Inches(0.5), Inches(3.0)] + [Inches(0.75)] * (len(columns) - 2)
    for col_idx in range(num_cols):
        table.columns[col_idx].width = col_widths[col_idx]

    # Add header row
    for col_idx, col_name in enumerate(columns):
        cell = table.cell(0, col_idx)
        cell.text = col_name
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0, 112, 192)
        cell.text_frame.paragraphs[0].font.size = Pt(12)
        cell.text_frame.paragraphs[0].font.bold = True
        cell.text_frame.paragraphs[0].font.color.rgb = RGBColor(255, 255, 255)
        cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER

    # Add data rows
    for row_idx, (index, row) in enumerate(df.iterrows(), start=1):
        is_total_row = index == len(df) - 1  # Check if it's the last row (GRAND TOTAL)
        for col_idx, col_name in enumerate(columns):
            cell = table.cell(row_idx, col_idx)
            try:
                value = row[col_name]
                cell.text = str(value)
            except (KeyError, ValueError) as e:
                cell.text = ""
                st.warning(f"Error accessing {col_name} at row {index} in customer slide: {e}")
            cell.text_frame.paragraphs[0].font.size = Pt(12)
            cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            cell.fill.solid()
            if is_total_row:
                cell.fill.fore_color.rgb = RGBColor(211, 211, 211)
                cell.text_frame.paragraphs[0].font.bold = True
            else:
                if (row_idx - 1) % 2 == 0:  # Adjust for header row
                    cell.fill.fore_color.rgb = RGBColor(221, 235, 247)
                else:
                    cell.fill.fore_color.rgb = RGBColor(255, 255, 255)

#######################################################
# MODULE 3: OD TARGET REPORT (From nbc2.py)
#######################################################

def extract_area_name(area):
    """Extract and standardize branch names, merging variations."""
    if pd.isna(area) or not str(area).strip():
        return None
    area = str(area).strip()
    area_upper = area.upper()
    if area_upper == 'HO' or area_upper.endswith('-HO'):
        return None
    branch_variations = {
        'PUDUCHERRY': ['PUDUCHERRY', 'PONDY', 'PUDUCHERRY - PONDY', 'PONDICHERRY', 'PUDUCHERI', 'aaaa - PUDUCHERRY', 'AAAA - PUDUCHERRY'],
        'COIMBATORE': ['COIMBATORE', 'CBE', 'COIMBATORE - CBE', 'COIMBATURE', 'aaaa - COIMBATORE', 'AAAA - COIMBATORE'],
        'KARUR': ['KARUR', 'KRR', 'KARUR - KRR', 'aaaa - KARUR', 'AAAA - KARUR'],
        'MADURAI': ['MADURAI', 'MDU', 'MADURAI - MDU', 'MADURA', 'aaaa - MADURAI', 'AAAA - MADURAI'],
        'CHENNAI': ['CHENNAI', 'CHN', 'aaaa - CHENNAI', 'AAAA - CHENNAI'],
    }
    for standard_name, variations in branch_variations.items():
        for variation in variations:
            if variation in area_upper:
                return standard_name
    prefixes = ['AAAA - ', 'aaaa - ', 'BBB - ', 'bbb - ', 'ASIA CRYSTAL COMMODITY LLP - ']
    for prefix in prefixes:
        if area_upper.startswith(prefix.upper()):
            return area[len(prefix):].strip().upper()
    separators = [' - ', '-', ':']
    for sep in separators:
        if sep in area_upper:
            return area_upper.split(sep)[-1].strip()
    return area_upper

def filter_os_qty(os_df, os_area_col, os_qty_col, os_due_date_col, os_exec_col, 
                  selected_branches=None, selected_years=None, till_month=None, selected_executives=None):
    """Filter by due date and aggregate net values by executive, applying filters and using only positive values."""
    required_columns = [os_area_col, os_qty_col, os_due_date_col, os_exec_col]
    for col in required_columns:
        if col not in os_df.columns:
            st.error(f"Column '{col}' not found in OS data.")
            return None, None, None

    os_df = os_df.copy()
    
    # Keep area extraction for filtering purposes
    os_df[os_area_col] = os_df[os_area_col].apply(extract_area_name)
    # Format executive names
    os_df[os_exec_col] = os_df[os_exec_col].apply(lambda x: 'BLANK' if pd.isna(x) or str(x).strip() == '' else str(x).strip().upper())

    try:
        os_df[os_due_date_col] = pd.to_datetime(os_df[os_due_date_col], errors='coerce')
    except Exception as e:
        st.error(f"Error converting '{os_due_date_col}' to datetime: {e}. Ensure dates are in 'YYYY-MM-DD' format.")
        return None, None, None

    start_date, end_date = None, None
    if selected_years and till_month:
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        if till_month == "November Brodha":
            till_month = "November"
            
        till_month_num = month_map.get(till_month)
        if not till_month_num:
            st.error(f"Invalid month selected: {till_month}")
            return None, None, None
        
        selected_years = [int(year) for year in selected_years]
        earliest_year = min(selected_years)
        latest_year = max(selected_years)
        start_date = datetime(earliest_year, 1, 1)
        end_date = (datetime(latest_year, till_month_num, 1) + relativedelta(months=1) - relativedelta(days=1))
        
        os_df = os_df[
            (os_df[os_due_date_col].notna()) &
            (os_df[os_due_date_col] >= start_date) &
            (os_df[os_due_date_col] <= end_date)
        ]
        if os_df.empty:
            st.error(f"No data matches the period from Jan {earliest_year} to {end_date.strftime('%b %Y')}.")
            return None, None, None

    # Apply branch filter if needed
    all_branches = sorted(os_df[os_area_col].dropna().unique())
    if not selected_branches:
        selected_branches = all_branches
    if sorted(selected_branches) != all_branches:
        os_df = os_df[os_df[os_area_col].isin(selected_branches)]
        if os_df.empty:
            st.error("No data matches the selected branches.")
            return None, None, None

    # Apply executive filter if needed
    all_executives = sorted(os_df[os_exec_col].dropna().unique())
    if selected_executives and sorted(selected_executives) != sorted(all_executives):
        os_df = os_df[os_df[os_exec_col].isin(selected_executives)]
        if os_df.empty:
            st.error("No data matches the selected executives.")
            return None, None, None

    # Convert to numeric and filter only positive values
    os_df[os_qty_col] = pd.to_numeric(os_df[os_qty_col], errors='coerce').fillna(0)
    if os_df[os_qty_col].isna().any():
        st.warning(f"Non-numeric values in '{os_qty_col}' replaced with 0.")
    
    # Filter only positive values
    os_df = os_df[os_df[os_qty_col] > 0]
    if os_df.empty:
        st.error("No positive net values found in the filtered data.")
        return None, None, None

    # Group by executive instead of area
    os_grouped_qty = (os_df.groupby(os_exec_col)
                     .agg({os_qty_col: 'sum'})
                     .reset_index()
                     .rename(columns={os_exec_col: 'Executive', os_qty_col: 'TARGET'}))
    os_grouped_qty['TARGET'] = os_grouped_qty['TARGET'] / 100000

    # Prepare the result DataFrame with all executives
    executives_to_display = selected_executives if selected_executives else all_executives

    result_df = pd.DataFrame({'Executive': executives_to_display})
    result_df = pd.merge(result_df, os_grouped_qty, on='Executive', how='left').fillna({'TARGET': 0})

    # Add total row
    total_row = pd.DataFrame([{'Executive': 'TOTAL', 'TARGET': result_df['TARGET'].sum()}])
    result_df = pd.concat([result_df, total_row], ignore_index=True)

    # Round values
    result_df['TARGET'] = result_df['TARGET'].round(2)

    return result_df, start_date, end_date

def create_od_table_image(df, title, columns_to_show=None):
    """Create a table image from the OD Target DataFrame."""
    if columns_to_show is None:
        # Check if DataFrame has Executive column (new structure) or Area column (old structure)
        if 'Executive' in df.columns:
            columns_to_show = ['Executive', 'TARGET (Lakhs)']
        else:
            columns_to_show = ['Area', 'TARGET (Lakhs)']
    
    fig, ax = plt.subplots(figsize=(10, len(df) * 0.5))
    ax.axis('off')
    nrows, ncols = len(df), len(columns_to_show)
    table = Table(ax, bbox=[0, 0, 1, 1])
    
    # Add column headers
    for col_idx, col_name in enumerate(columns_to_show):
        table.add_cell(0, col_idx, 1.0/ncols, 1.0/nrows, text=col_name, loc='center', facecolor='#F2F2F2')
        table[0, col_idx].set_text_props(weight='bold', color='black', fontsize=12)
    
    # Map display column names to actual DataFrame column names
    column_mapping = {
        'Executive': 'Executive',
        'Area': 'Area',
        'TARGET (Lakhs)': 'TARGET'
    }
    
    # Determine if we're using Executive or Area for grouping
    key_column = 'Executive' if 'Executive' in df.columns else 'Area'
    
    # Add data rows
    for row_idx in range(len(df)):
        for col_idx, display_col_name in enumerate(columns_to_show):
            # Get actual column name from mapping
            actual_col_name = column_mapping.get(display_col_name, display_col_name)
            
            # If column not in DataFrame, try to handle it
            if actual_col_name not in df.columns:
                if display_col_name == 'TARGET (Lakhs)' and 'TARGET' in df.columns:
                    actual_col_name = 'TARGET'
                else:
                    st.error(f"Column '{actual_col_name}' not found in DataFrame")
                    continue
            
            # Get value and format
            value = df.iloc[row_idx][actual_col_name]
            text = str(value) if actual_col_name == key_column else f"{float(value):.2f}"
            
            # Set row background color
            facecolor = '#DDEBF7' if row_idx % 2 == 0 else 'white'
            
            # Highlight total row
            if row_idx == len(df) - 1 and df.iloc[row_idx][key_column] == 'TOTAL':
                facecolor = '#D3D3D3'
                table.add_cell(row_idx + 1, col_idx, 1.0/ncols, 1.0/nrows, 
                              text=text, loc='center', facecolor=facecolor).set_text_props(weight='bold', fontsize=12)
            else:
                table.add_cell(row_idx + 1, col_idx, 1.0/ncols, 1.0/nrows, 
                              text=text, loc='center', facecolor=facecolor).set_text_props(fontsize=10)
    
    # Set column widths
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
    """Create a PowerPoint slide with a simple two-column table (Executive and TARGET)."""
    try:
        # Add title
        title_shape = slide.shapes.add_textbox(
            Inches(0.5), Inches(0.5), 
            Inches(12), Inches(0.8)
        )
        title_frame = title_shape.text_frame
        title_para = title_frame.add_paragraph()
        title_para.text = title
        title_para.font.size = Pt(24)
        title_para.font.bold = True
        title_para.alignment = PP_ALIGN.CENTER
        
        # Determine key column (Executive or Area)
        key_column = 'Executive' if 'Executive' in df.columns else 'Area'
        
        # Create a simple 2-column table
        rows, cols = len(df) + 1, 2  # +1 for header
        
        # Table dimensions and position
        table_width = Inches(8)  # Reduced width for a more compact table
        table_height = Inches(len(df) * 0.4 + 0.5)
        
        # Center the table on the slide
        left = Inches(2.0)  # Adjusted to center the table
        top = Inches(1.5)
        
        # Create the table
        table = slide.shapes.add_table(
            rows, cols, 
            left, top, 
            table_width, table_height
        ).table
        
        # Set column headers with blue background (like in your example)
        for i in range(2):
            header_cell = table.cell(0, i)
            header_cell.text = key_column if i == 0 else "TARGET"
            header_cell.text_frame.paragraphs[0].font.bold = True
            header_cell.text_frame.paragraphs[0].font.color.rgb = RGBColor(255, 255, 255)  # White text
            header_cell.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            header_cell.fill.solid()
            header_cell.fill.fore_color.rgb = RGBColor(0, 114, 188)  # Blue background
        
        # Fill data
        for i in range(len(df)):
            # Executive name
            table.cell(i + 1, 0).text = str(df.iloc[i][key_column])
            table.cell(i + 1, 0).text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            
            # TARGET value
            value_text = f"{df.iloc[i]['TARGET']:.2f}"
            table.cell(i + 1, 1).text = value_text
            table.cell(i + 1, 1).text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
            
            # Set row colors - alternating for better readability
            # Light blue for regular rows (like in your example)
            row_color = RGBColor(221, 235, 247) if i % 2 == 0 else RGBColor(255, 255, 255)
            
            # Different color for TOTAL row
            if df.iloc[i][key_column] == 'TOTAL':
                row_color = RGBColor(211, 211, 211)  # Light gray for total row
            
            # Apply colors to cells
            for j in range(2):
                cell = table.cell(i + 1, j)
                cell.fill.solid()
                cell.fill.fore_color.rgb = row_color
            
            # Bold the TOTAL row
            if df.iloc[i][key_column] == 'TOTAL':
                table.cell(i + 1, 0).text_frame.paragraphs[0].font.bold = True
                table.cell(i + 1, 1).text_frame.paragraphs[0].font.bold = True
        
        # Set column widths - equal width for both columns
        table.columns[0].width = Inches(4)  # Executive column
        table.columns[1].width = Inches(4)  # TARGET column
        
    except Exception as e:
        st.error(f"Error creating PPT slide: {e}")
        st.error(traceback.format_exc())
def calculate_od_values(os_jan, os_feb, total_sale, selected_month_str,
                        os_jan_due_date_col, os_jan_ref_date_col, os_jan_net_value_col, os_jan_exec_col, os_jan_sl_code_col,
                        os_feb_due_date_col, os_feb_ref_date_col, os_feb_net_value_col, os_feb_exec_col, os_feb_sl_code_col,
                        sale_bill_date_col, sale_due_date_col, sale_value_col, sale_exec_col, sale_sl_code_col,
                        selected_executives):
    """
    Calculate OD Target vs Collection metrics for a selected month, grouped by executives.
    """
    # Validate numeric columns
    for df, col, file in [
        (os_jan, os_jan_net_value_col, "OS Jan"),
        (os_feb, os_feb_net_value_col, "OS Feb"),
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

    # Preprocess negative values in OS Feb Net Value
    os_feb[os_feb_net_value_col] = os_feb[os_feb_net_value_col].clip(lower=0)

    # Convert date columns to datetime and create SL Code and Executive columns
    os_jan[os_jan_due_date_col] = pd.to_datetime(os_jan[os_jan_due_date_col], errors='coerce')
    os_jan[os_jan_ref_date_col] = pd.to_datetime(os_jan.get(os_jan_ref_date_col), errors='coerce')
    os_jan["SL Code"] = os_jan[os_jan_sl_code_col].astype(str)
    os_jan["Executive"] = os_jan[os_jan_exec_col].astype(str)

    os_feb[os_feb_due_date_col] = pd.to_datetime(os_feb[os_feb_due_date_col], errors='coerce')
    os_feb[os_feb_ref_date_col] = pd.to_datetime(os_feb.get(os_feb_ref_date_col), errors='coerce')
    os_feb["SL Code"] = os_feb[os_feb_sl_code_col].astype(str)
    os_feb["Executive"] = os_feb[os_feb_exec_col].astype(str)

    total_sale[sale_bill_date_col] = pd.to_datetime(total_sale[sale_bill_date_col], errors='coerce')
    total_sale[sale_due_date_col] = pd.to_datetime(total_sale[sale_due_date_col], errors='coerce')
    total_sale["SL Code"] = total_sale[sale_sl_code_col].astype(str)
    total_sale["Executive"] = total_sale[sale_exec_col].astype(str)

    # Apply executive filtering
    if selected_executives:
        os_jan = os_jan[os_jan["Executive"].isin(selected_executives)]
        if os_jan.empty:
            st.error("No data remains after OS Jan executive filtering.")
            return None

        os_feb = os_feb[os_feb["Executive"].isin(selected_executives)]
        if os_feb.empty:
            st.error("No data remains after OS Feb executive filtering.")
            return None

        total_sale = total_sale[total_sale["Executive"].isin(selected_executives)]
        if total_sale.empty:
            st.error("No data remains after Sales executive filtering.")
            return None

    # Define date ranges dynamically from selected_month_str
    specified_date = pd.to_datetime("01-" + selected_month_str, format="%d-%b-%y")
    specified_month_end = specified_date + pd.offsets.MonthEnd(0)

    # Calculate Due Target: Sum of Net Value from OS Jan where Due Date <= specified_month_end, grouped by SL Code
    due_target = os_jan[os_jan[os_jan_due_date_col] <= specified_month_end]
    due_target_sum = due_target.groupby(["SL Code", "Executive"])[os_jan_net_value_col].sum().reset_index()
    due_target_sum = due_target_sum.groupby("Executive")[os_jan_net_value_col].sum().reset_index()
    due_target_sum.columns = ["Executive", "Due Target"]

    # Calculate Collection Achieved: OS Jan (Due Date <= specified_month_end) - OS Feb (Ref Date < specified_date and Due Date <= specified_month_end)
    os_jan_coll = os_jan[os_jan[os_jan_due_date_col] <= specified_month_end]
    os_jan_coll_sum = os_jan_coll.groupby(["SL Code", "Executive"])[os_jan_net_value_col].sum().reset_index()
    os_jan_coll_sum = os_jan_coll_sum.groupby("Executive")[os_jan_net_value_col].sum().reset_index()
    os_jan_coll_sum.columns = ["Executive", "OS Jan Coll"]

    os_feb_coll = os_feb[(os_feb[os_feb_ref_date_col] < specified_date) & (os_feb[os_feb_due_date_col] <= specified_month_end)]
    os_feb_coll_sum = os_feb_coll.groupby(["SL Code", "Executive"])[os_feb_net_value_col].sum().reset_index()
    os_feb_coll_sum = os_feb_coll_sum.groupby("Executive")[os_feb_net_value_col].sum().reset_index()
    os_feb_coll_sum.columns = ["Executive", "OS Feb Coll"]

    collection = os_jan_coll_sum.merge(os_feb_coll_sum, on="Executive", how="outer").fillna(0)
    collection["Collection Achieved"] = collection["OS Jan Coll"] - collection["OS Feb Coll"]
    collection["Overall % Achieved"] = np.where(collection["OS Jan Coll"] > 0, (collection["Collection Achieved"] / collection["OS Jan Coll"]) * 100, 0)

    # Merge with Due Target
    collection = collection.merge(due_target_sum[["Executive", "Due Target"]], on="Executive", how="outer").fillna(0)

    # Calculate Overdue: Sum of Value from Total Sale where Bill Date and Due Date are in the specified month
    overdue = total_sale[(total_sale[sale_bill_date_col].between(specified_date, specified_month_end)) & 
                        (total_sale[sale_due_date_col].between(specified_date, specified_month_end))]
    overdue_sum = overdue.groupby(["SL Code", "Executive"])[sale_value_col].sum().reset_index()
    overdue_sum = overdue_sum.groupby("Executive")[sale_value_col].sum().reset_index()
    overdue_sum.columns = ["Executive", "For the month Overdue"]

    # Calculate Month Collection: Sales Value (specified month) - OS Feb Net Value (Ref Date and Due Date in specified month)
    sale_value = overdue.groupby(["SL Code", "Executive"])[sale_value_col].sum().reset_index()
    sale_value = sale_value.groupby("Executive")[sale_value_col].sum().reset_index()
    sale_value.columns = ["Executive", "Sale Value"]

    os_feb_month = os_feb[(os_feb[os_feb_ref_date_col].between(specified_date, specified_month_end)) & 
                         (os_feb[os_feb_due_date_col].between(specified_date, specified_month_end))]
    os_feb_month_sum = os_feb_month.groupby(["SL Code", "Executive"])[os_feb_net_value_col].sum().reset_index()
    os_feb_month_sum = os_feb_month_sum.groupby("Executive")[os_feb_net_value_col].sum().reset_index()
    os_feb_month_sum.columns = ["Executive", "OS Month Collection"]

    month_collection = sale_value.merge(os_feb_month_sum, on="Executive", how="outer").fillna(0)
    month_collection["For the month Collection"] = month_collection["Sale Value"] - month_collection["OS Month Collection"]
    month_collection_final = month_collection[["Executive", "For the month Collection"]]

    # Combine all metrics
    final = collection.drop(columns=["OS Jan Coll", "OS Feb Coll"]).merge(overdue_sum, on="Executive", how="outer").merge(month_collection_final, on="Executive", how="outer").fillna(0)
    final["% Achieved (Selected Month)"] = np.where(final["For the month Overdue"] > 0, (final["For the month Collection"] / final["For the month Overdue"]) * 100, 0)

    # Final processing: Exclude HO executives, convert to lakhs, round, and sort
    final = final[~final["Executive"].str.upper().isin(["HO", "HEAD OFFICE"])]
    val_cols = ["Due Target", "Collection Achieved", "For the month Overdue", "For the month Collection"]
    final[val_cols] = final[val_cols].div(100000)
    round_cols = val_cols + ["Overall % Achieved", "% Achieved (Selected Month)"]
    final[round_cols] = final[round_cols].round(2)

    final = final[["Executive", "Due Target", "Collection Achieved", "Overall % Achieved", "For the month Overdue", "For the month Collection", "% Achieved (Selected Month)"]]
    final.sort_values("Executive", inplace=True)
    
    # Add TOTAL row
    total_row = {'Executive': 'TOTAL'}
    for col in final.columns[1:]:
        if col in ["Overall % Achieved", "% Achieved (Selected Month)"]:
            # For percentage columns, compute weighted average
            if col == "Overall % Achieved":
                total_row[col] = round((final["Collection Achieved"].sum() / final["Due Target"].sum() * 100) if final["Due Target"].sum() > 0 else 0, 2)
            else:  # "% Achieved (Selected Month)"
                total_row[col] = round((final["For the month Collection"].sum() / final["For the month Overdue"].sum() * 100) if final["For the month Overdue"].sum() > 0 else 0, 2)
        else:
            total_row[col] = round(final[col].sum(), 2)
    
    final = pd.concat([final, pd.DataFrame([total_row])], ignore_index=True)
    
    return final

def get_available_months(os_jan, os_feb, total_sale,
                         os_jan_due_date_col, os_jan_ref_date_col,
                         os_feb_due_date_col, os_feb_ref_date_col,
                         sale_bill_date_col, sale_due_date_col):
    """Get unique months from date columns across all files."""
    months = set()
    for df, date_cols in [
        (os_jan, [os_jan_due_date_col, os_jan_ref_date_col]),
        (os_feb, [os_feb_due_date_col, os_feb_ref_date_col]),
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

#######################################################
# MODULE 4: PRODUCT GROWTH REPORT (From pg2.py)
#######################################################

#######################################################
# MODULE 4: PRODUCT GROWTH REPORT (From pg2.py)
#######################################################

def standardize_name(name):
    """Standardize a name by removing extra spaces, special characters, and converting to title case."""
    if pd.isna(name) or not name:
        return ""
    name = str(name).strip().lower()
    name = ''.join(c for c in name if c.isalnum() or c.isspace())
    name = ' '.join(word.capitalize() for word in name.split())
    general_variants = ['general', 'gen', 'generals', 'general ', 'genral', 'generl']
    if any(variant in name.lower() for variant in general_variants):
        return 'General'
    return name

def create_sl_code_mapping(ly_df, cy_df, budget_df, ly_sl_code_col, cy_sl_code_col, budget_sl_code_col, 
                           ly_company_group_col, cy_company_group_col, budget_company_group_col):
    """Create a mapping of SL Code to standardized company group name from LY, CY, and Budget data."""
    try:
        mappings = []
        for df, sl_code_col, company_group_col in [
            (ly_df, ly_sl_code_col, ly_company_group_col),
            (cy_df, cy_sl_code_col, cy_company_group_col),
            (budget_df, budget_sl_code_col, budget_company_group_col)
        ]:
            if sl_code_col in df.columns and company_group_col in df.columns:
                subset = df[[sl_code_col, company_group_col]].dropna()
                subset = subset[subset[sl_code_col] != ""]
                mappings.append(subset.rename(columns={sl_code_col: 'SL_CODE', company_group_col: 'COMPANY_GROUP'}))
        if not mappings:
            logger.warning("No valid SL Code mappings found in any dataset")
            return {}
        combined = pd.concat(mappings, ignore_index=True)
        combined['COMPANY_GROUP'] = combined['COMPANY_GROUP'].apply(standardize_name)
        mapping_df = combined.groupby('SL_CODE')['COMPANY_GROUP'].agg(lambda x: x.mode()[0] if not x.empty else "").reset_index()
        sl_code_map = dict(zip(mapping_df['SL_CODE'], mapping_df['COMPANY_GROUP']))
        return sl_code_map
    except Exception as e:
        logger.error(f"Error creating SL Code mapping: {e}")
        st.error(f"Error creating SL Code mapping: {e}")
        return {}

def apply_sl_code_mapping(df, sl_code_col, company_group_col, sl_code_map):
    """Apply SL Code mapping to standardize company group names."""
    if sl_code_col not in df.columns or not sl_code_map:
        return df[company_group_col].apply(standardize_name)
    try:
        def map_company(row):
            if pd.isna(row[sl_code_col]) or row[sl_code_col] == "":
                return standardize_name(row[company_group_col])
            sl_code = str(row[sl_code_col]).strip()
            return sl_code_map.get(sl_code, standardize_name(row[company_group_col]))
        return df.apply(map_company, axis=1)
    except Exception as e:
        logger.error(f"Error applying SL Code mapping: {e}")
        st.error(f"Error applying SL Code mapping: {e}")
        return df[company_group_col].apply(standardize_name)

def calculate_product_growth(ly_df, cy_df, budget_df, ly_month, cy_month, ly_date_col, cy_date_col, 
                            ly_qty_col, cy_qty_col, ly_value_col, cy_value_col, 
                            budget_qty_col, budget_value_col, ly_company_group_col, 
                            cy_company_group_col, budget_company_group_col, 
                            ly_product_group_col, cy_product_group_col, budget_product_group_col,
                            ly_sl_code_col, cy_sl_code_col, budget_sl_code_col,
                            ly_exec_col, cy_exec_col, budget_exec_col, 
                            selected_executives=None, selected_company_groups=None, selected_product_groups=None):
    """Calculate Quantity and Value growth by Company Group and Product Group."""
    ly_df = ly_df.copy()
    cy_df = cy_df.copy()
    budget_df = budget_df.copy()

    # Validate columns
    required_cols = [(ly_df, [ly_date_col, ly_qty_col, ly_value_col, ly_company_group_col, ly_product_group_col, ly_exec_col]),
                    (cy_df, [cy_date_col, cy_qty_col, cy_value_col, cy_company_group_col, cy_product_group_col, cy_exec_col]),
                    (budget_df, [budget_qty_col, budget_value_col, budget_company_group_col, budget_product_group_col, budget_exec_col])]
    for df, cols in required_cols:
        missing_cols = [col for col in cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in DataFrame: {missing_cols}")
            st.error(f"Missing columns: {missing_cols}")
            return None

    # Create and apply SL Code mapping
    sl_code_map = create_sl_code_mapping(
        ly_df, cy_df, budget_df, 
        ly_sl_code_col, cy_sl_code_col, budget_sl_code_col,
        ly_company_group_col, cy_company_group_col, budget_company_group_col
    )
    ly_df[ly_company_group_col] = apply_sl_code_mapping(ly_df, ly_sl_code_col, ly_company_group_col, sl_code_map)
    cy_df[cy_company_group_col] = apply_sl_code_mapping(cy_df, cy_sl_code_col, cy_company_group_col, sl_code_map)
    budget_df[budget_company_group_col] = apply_sl_code_mapping(budget_df, budget_sl_code_col, budget_company_group_col, sl_code_map)

    # Standardize Product Group names
    ly_df[ly_product_group_col] = ly_df[ly_product_group_col].apply(standardize_name)
    cy_df[cy_product_group_col] = cy_df[cy_product_group_col].apply(standardize_name)
    budget_df[budget_product_group_col] = budget_df[budget_product_group_col].apply(standardize_name)

    # Apply executive filter
    if selected_executives:
        if ly_exec_col in ly_df.columns:
            ly_df = ly_df[ly_df[ly_exec_col].isin(selected_executives)]
        if cy_exec_col in cy_df.columns:
            cy_df = cy_df[cy_df[cy_exec_col].isin(selected_executives)]
        if budget_exec_col in budget_df.columns:
            budget_df = budget_df[budget_df[budget_exec_col].isin(selected_executives)]

    if ly_df.empty or cy_df.empty or budget_df.empty:
        st.warning("No data remains after executive filtering. Please check executive selections.")
        return None

    # Convert date columns
    ly_df[ly_date_col] = pd.to_datetime(ly_df[ly_date_col], dayfirst=True, errors='coerce', format='mixed')
    cy_df[cy_date_col] = pd.to_datetime(cy_df[cy_date_col], dayfirst=True, errors='coerce', format='mixed')
    available_ly_months = ly_df[ly_date_col].dt.strftime('%b %y').dropna().unique().tolist()
    available_cy_months = cy_df[cy_date_col].dt.strftime('%b %y').dropna().unique().tolist()

    # Check for valid dates
    if not available_ly_months or not available_cy_months:
        st.error("No valid dates found in LY or CY data. Please check date columns.")
        return None

    # Apply month filter
    if ly_month:
        ly_filtered_df = ly_df[ly_df[ly_date_col].dt.strftime('%b %y') == ly_month]
    else:
        latest_ly_month = max(available_ly_months, key=lambda x: pd.to_datetime(f"01 {x}", format="%d %b %y"))
        ly_filtered_df = ly_df[ly_df[ly_date_col].dt.strftime('%b %y') == latest_ly_month]
        ly_month = latest_ly_month

    if cy_month:
        cy_filtered_df = cy_df[cy_df[cy_date_col].dt.strftime('%b %y') == cy_month]
    else:
        latest_cy_month = max(available_cy_months, key=lambda x: pd.to_datetime(f"01 {x}", format="%d %b %y"))
        cy_filtered_df = cy_df[cy_df[cy_date_col].dt.strftime('%b %y') == latest_cy_month]
        cy_month = latest_cy_month

    if ly_filtered_df.empty or cy_filtered_df.empty:
        st.warning(f"No data for selected months (LY: {ly_month}, CY: {cy_month}). Please check month selections.")
        return None

    # Apply company group filter
    company_groups = pd.concat([ly_filtered_df[ly_company_group_col], cy_filtered_df[cy_company_group_col], budget_df[budget_company_group_col]]).dropna().unique().tolist()
    if selected_company_groups:
        selected_company_groups = [standardize_name(g) for g in selected_company_groups]
        valid_groups = set(company_groups)
        invalid_groups = [g for g in selected_company_groups if g not in valid_groups]
        if invalid_groups:
            st.warning(f"The following company groups are not found in the data: {invalid_groups}. Please verify selections.")
            selected_company_groups = [g for g in selected_company_groups if g in valid_groups]
            if not selected_company_groups:
                st.error("No valid company groups selected after validation. Please select valid company groups.")
                return None

        ly_filtered_df = ly_filtered_df[ly_filtered_df[ly_company_group_col].isin(selected_company_groups)]
        cy_filtered_df = cy_filtered_df[cy_filtered_df[cy_company_group_col].isin(selected_company_groups)]
        budget_df = budget_df[budget_df[budget_company_group_col].isin(selected_company_groups)]

        if ly_filtered_df.empty or cy_filtered_df.empty:
            st.warning(f"No data remains after filtering for company groups: {selected_company_groups}. Please check company group selections or data content.")
            return None

    # Apply product group filter
    product_groups = pd.concat([ly_filtered_df[ly_product_group_col], cy_filtered_df[cy_product_group_col], budget_df[budget_product_group_col]]).dropna().unique().tolist()
    if selected_product_groups:
        selected_product_groups = [standardize_name(g) for g in selected_product_groups]
        valid_product_groups = set(product_groups)
        invalid_product_groups = [g for g in selected_product_groups if g not in valid_product_groups]
        if invalid_product_groups:
            st.warning(f"The following product groups are not found in the data: {invalid_product_groups}. Please verify selections.")
            selected_product_groups = [g for g in selected_product_groups if g in valid_product_groups]
            if not selected_product_groups:
                st.error("No valid product groups selected after validation. Please select valid product groups.")
                return None

        ly_filtered_df = ly_filtered_df[ly_filtered_df[ly_product_group_col].isin(selected_product_groups)]
        cy_filtered_df = cy_filtered_df[cy_filtered_df[cy_product_group_col].isin(selected_product_groups)]
        budget_df = budget_df[budget_df[budget_product_group_col].isin(selected_product_groups)]

        if ly_filtered_df.empty or cy_filtered_df.empty:
            st.warning(f"No data remains after filtering for product groups: {selected_product_groups}. Please check product group selections or data content.")
            return None

    # Convert quantity and value columns to numeric
    for df, qty_col, value_col in [(ly_filtered_df, ly_qty_col, ly_value_col), (cy_filtered_df, cy_qty_col, cy_value_col), (budget_df, budget_qty_col, budget_value_col)]:
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)

    # Aggregate by company group and product group
    company_groups = selected_company_groups if selected_company_groups else sorted(set(company_groups))
    if not company_groups:
        st.warning("No valid company groups found in the data. Please check company group columns.")
        return None

    # Initialize result dictionary
    result = {}
    for company in company_groups:
        qty_df = pd.DataFrame(columns=['PRODUCT GROUP', 'LY_QTY', 'BUDGET_QTY', 'CY_QTY', 'ACHIEVEMENT %'])
        value_df = pd.DataFrame(columns=['PRODUCT GROUP', 'LY_VALUE', 'BUDGET_VALUE', 'CY_VALUE', 'ACHIEVEMENT %'])

        # Filter data for the company group
        ly_company_df = ly_filtered_df[ly_filtered_df[ly_company_group_col] == company]
        cy_company_df = cy_filtered_df[cy_filtered_df[cy_company_group_col] == company]
        budget_company_df = budget_df[budget_df[budget_company_group_col] == company]

        if ly_company_df.empty and cy_company_df.empty and budget_company_df.empty:
            continue

        # Get unique product groups for this company group
        company_product_groups = pd.concat([
            ly_company_df[ly_product_group_col],
            cy_company_df[cy_product_group_col],
            budget_company_df[budget_product_group_col]
        ]).dropna().unique().tolist()
        
        if not company_product_groups:
            continue

        # Apply product group filter if specified
        if selected_product_groups:
            company_product_groups = [pg for pg in company_product_groups if pg in selected_product_groups]
            if not company_product_groups:
                continue

        # Aggregate quantities
        ly_qty = ly_company_df.groupby([ly_company_group_col, ly_product_group_col])[ly_qty_col].sum().reset_index()
        ly_qty = ly_qty.rename(columns={ly_product_group_col: 'PRODUCT GROUP', ly_qty_col: 'LY_QTY'})
        
        cy_qty = cy_company_df.groupby([cy_company_group_col, cy_product_group_col])[cy_qty_col].sum().reset_index()
        cy_qty = cy_qty.rename(columns={cy_product_group_col: 'PRODUCT GROUP', cy_qty_col: 'CY_QTY'})
        
        budget_qty = budget_company_df.groupby([budget_company_group_col, budget_product_group_col])[budget_qty_col].sum().reset_index()
        budget_qty = budget_qty.rename(columns={budget_product_group_col: 'PRODUCT GROUP', budget_qty_col: 'BUDGET_QTY'})

        # Aggregate values
        ly_value = ly_company_df.groupby([ly_company_group_col, ly_product_group_col])[ly_value_col].sum().reset_index()
        ly_value = ly_value.rename(columns={ly_product_group_col: 'PRODUCT GROUP', ly_value_col: 'LY_VALUE'})
        
        cy_value = cy_company_df.groupby([cy_company_group_col, cy_product_group_col])[cy_value_col].sum().reset_index()
        cy_value = cy_value.rename(columns={cy_product_group_col: 'PRODUCT GROUP', cy_value_col: 'CY_VALUE'})
        
        budget_value = budget_company_df.groupby([budget_company_group_col, budget_product_group_col])[budget_value_col].sum().reset_index()
        budget_value = budget_value.rename(columns={budget_product_group_col: 'PRODUCT GROUP', budget_value_col: 'BUDGET_VALUE'})

        # Create DataFrames for company-specific product groups
        product_qty_df = pd.DataFrame({'PRODUCT GROUP': company_product_groups})
        product_value_df = pd.DataFrame({'PRODUCT GROUP': company_product_groups})

        # Merge with aggregated data
        qty_df = product_qty_df.merge(ly_qty[['PRODUCT GROUP', 'LY_QTY']], on='PRODUCT GROUP', how='left')\
                               .merge(budget_qty[['PRODUCT GROUP', 'BUDGET_QTY']], on='PRODUCT GROUP', how='left')\
                               .merge(cy_qty[['PRODUCT GROUP', 'CY_QTY']], on='PRODUCT GROUP', how='left').fillna(0)
        value_df = product_value_df.merge(ly_value[['PRODUCT GROUP', 'LY_VALUE']], on='PRODUCT GROUP', how='left')\
                                   .merge(budget_value[['PRODUCT GROUP', 'BUDGET_VALUE']], on='PRODUCT GROUP', how='left')\
                                   .merge(cy_value[['PRODUCT GROUP', 'CY_VALUE']], on='PRODUCT GROUP', how='left').fillna(0)

        # Calculate achievement percentage (LY vs CY only)
        def calc_achievement(row, cy_col, ly_col):
            if pd.isna(row[ly_col]) or row[ly_col] == 0:
                return 0.00 if row[cy_col] == 0 else 100.00
            return round(((row[cy_col] - row[ly_col]) / row[ly_col]) * 100, 2)

        qty_df['ACHIEVEMENT %'] = qty_df.apply(lambda row: calc_achievement(row, 'CY_QTY', 'LY_QTY'), axis=1)
        value_df['ACHIEVEMENT %'] = value_df.apply(lambda row: calc_achievement(row, 'CY_VALUE', 'LY_VALUE'), axis=1)

        # Reorder columns
        qty_df = qty_df[['PRODUCT GROUP', 'LY_QTY', 'BUDGET_QTY', 'CY_QTY', 'ACHIEVEMENT %']]
        value_df = value_df[['PRODUCT GROUP', 'LY_VALUE', 'BUDGET_VALUE', 'CY_VALUE', 'ACHIEVEMENT %']]

        # Add TOTAL row
        qty_totals = pd.DataFrame({
            'PRODUCT GROUP': ['TOTAL'],
            'LY_QTY': [qty_df['LY_QTY'].sum()],
            'BUDGET_QTY': [qty_df['BUDGET_QTY'].sum()],
            'CY_QTY': [qty_df['CY_QTY'].sum()],
            'ACHIEVEMENT %': [calc_achievement({'CY_QTY': qty_df['CY_QTY'].sum(), 'LY_QTY': qty_df['LY_QTY'].sum()}, 'CY_QTY', 'LY_QTY')]
        })
        qty_df = pd.concat([qty_df, qty_totals], ignore_index=True)

        value_totals = pd.DataFrame({
            'PRODUCT GROUP': ['TOTAL'],
            'LY_VALUE': [value_df['LY_VALUE'].sum()],
            'BUDGET_VALUE': [value_df['BUDGET_VALUE'].sum()],
            'CY_VALUE': [value_df['CY_VALUE'].sum()],
            'ACHIEVEMENT %': [calc_achievement({'CY_VALUE': value_df['CY_VALUE'].sum(), 'LY_VALUE': value_df['LY_VALUE'].sum()}, 'CY_VALUE', 'LY_VALUE')]
        })
        value_df = pd.concat([value_df, value_totals], ignore_index=True)

        result[company] = {'qty_df': qty_df, 'value_df': value_df}

    if not result:
        st.warning("No data available after filtering. Please review filters and data.")
        return None

    return result

def create_product_growth_ppt(group_results, month_title, logo_file=None):
    """Create a PPT for Product Growth by Company Group and Product Group."""
    try:
        prs = Presentation()
        prs.slide_width = Inches(13.33)
        prs.slide_height = Inches(7.5)

        # Create title slide
        create_title_slide(prs, f"Product Growth – {month_title}", logo_file)
        
        # Add data slides for each company group
        for company, data in group_results.items():
            qty_df = data['qty_df']
            value_df = data['value_df']
            
            add_table_slide(prs, qty_df, f"{company} - Quantity Growth", percent_cols=[4])
            add_table_slide(prs, value_df, f"{company} - Value Growth", percent_cols=[4])
        
        # Save to buffer
        ppt_buffer = BytesIO()
        prs.save(ppt_buffer)
        ppt_buffer.seek(0)
        return ppt_buffer
    except Exception as e:
        logger.error(f"Error creating Product Growth PPT: {e}")
        st.error(f"Error creating Product Growth PPT: {e}")
        return None
#######################################################
# MAIN APPLICATION
#######################################################

def sidebar_ui():
    """Sidebar UI for file uploads and app info."""
    with st.sidebar:
        st.title("Integrated Reports Dashboard")
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
        st.write("📊 **Dashboard v1.0**")
        st.write("© 2025 Asia Crystal Commodity LLP")

def main():
    """Main application function."""
    sidebar_ui()
    
    st.title("🔄 Integrated Reports Dashboard")
    
    # Check if files are uploaded and show appropriate messages
    required_files = {
        "Sales File": st.session_state.sales_file,
        "Budget File": st.session_state.budget_file,
        "OS-First Excel File": st.session_state.os_jan_file,
        "OS-Second Excel File": st.session_state.os_feb_file
    }
    
    missing_files = [name for name, file in required_files.items() if file is None]
    
    if missing_files:
        st.warning(f"Please upload the following files in the sidebar to access full functionality: {', '.join(missing_files)}")
        
        # Show file status
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Required Files:")
            st.markdown(f"- Sales File: {'✅ Uploaded' if st.session_state.sales_file else '❌ Missing'}")
            st.markdown(f"- Budget File: {'✅ Uploaded' if st.session_state.budget_file else '❌ Missing'}")
        with col2:
            st.markdown("####  ")
            st.markdown(f"- OS-First File: {'✅ Uploaded' if st.session_state.os_jan_file else '❌ Missing'}")
            st.markdown(f"- OS-Second File: {'✅ Uploaded' if st.session_state.os_feb_file else '❌ Missing'}")
    
    # Create tabs for each module
    tabs = st.tabs([
        "📊 Budget vs Billed", 
        "💰 OD Target vs Collection",
        "📈 Product Growth",
        "👥 Number of Billed Customers & OD Target"
        
    ])
    
    # MODULE 1: Budget vs Billed Report Tab
    with tabs[0]:
        st.header("Budget vs Billed Report")
        
        if not st.session_state.sales_file or not st.session_state.budget_file:
            st.warning("⚠️ Please upload both Sales and Budget files to use this tab")
        else:
            try:
                # Get sheet names
                sales_sheets = get_excel_sheets(st.session_state.sales_file)
                budget_sheets = get_excel_sheets(st.session_state.budget_file)
                
                # Sheet selection
                st.subheader("Sheet Selection")
                col1, col2 = st.columns(2)
                with col1:
                    sales_sheet = st.selectbox("Sales Sheet Name", sales_sheets, index=sales_sheets.index('24-25') if '24-25' in sales_sheets else 0, key='budget_sales_sheet')
                    sales_header_row = st.number_input("Sales Header Row (1-based)", min_value=1, value=1, step=1, key='budget_sales_header') - 1
                with col2:
                    budget_sheet = st.selectbox("Budget Sheet Name", budget_sheets, index=budget_sheets.index('Consolidate') if 'Consolidate' in budget_sheets else 0, key='budget_budget_sheet')
                    budget_header_row = st.number_input("Budget Header Row (1-based)", min_value=1, value=1, step=1, key='budget_budget_header') - 1
                
                # Load data
                sales_df = pd.read_excel(st.session_state.sales_file, sheet_name=sales_sheet, header=sales_header_row, dtype={'SL Code': str})
                budget_df = pd.read_excel(st.session_state.budget_file, sheet_name=budget_sheet, header=budget_header_row, dtype={'SL Code': str})
                
                # Get available columns
                sales_columns = sales_df.columns.tolist()
                budget_columns = budget_df.columns.tolist()
                
                # Column mappings
                with st.expander("Column Mappings"):
                    st.subheader("Sales Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        sales_date_col = st.selectbox("Sales Date Column", sales_columns, key='sales_date')
                        sales_area_col = st.selectbox("Sales Area Column", sales_columns, key='sales_area')
                    with col2:
                        sales_value_col = st.selectbox("Sales Value Column", sales_columns, key='sales_value')
                        sales_qty_col = st.selectbox("Sales Quantity Column", sales_columns, key='sales_qty')
                    with col3:
                        sales_product_group_col = st.selectbox("Sales Product Group Column", sales_columns, key='sales_product_group')
                        sales_sl_code_col = st.selectbox("Sales SL Code Column", sales_columns, key='sales_sl_code')
                    sales_exec_col = st.selectbox("Sales Executive Column", sales_columns, key='sales_exec')
                    
                    st.subheader("Budget Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        budget_area_col = st.selectbox("Budget Area Column", budget_columns, key='budget_area')
                        budget_value_col = st.selectbox("Budget Value Column", budget_columns, key='budget_value')
                    with col2:
                        budget_qty_col = st.selectbox("Budget Quantity Column", budget_columns, key='budget_qty')
                        budget_product_group_col = st.selectbox("Budget Product Group Column", budget_columns, key='budget_product_group')
                    with col3:
                        budget_sl_code_col = st.selectbox("Budget SL Code Column", budget_columns, key='budget_sl_code')
                        budget_exec_col = st.selectbox("Budget Executive Column", budget_columns, key='budget_exec')
                
                # Executive selection
                st.subheader("Executive Selection")
                sales_executives = sorted(sales_df[sales_exec_col].dropna().unique().tolist())
                
                # Filter out "ACCLP" 
                sales_executives = [exec for exec in sales_executives if exec != "ACCLP"]
                
                if not 'exec_select_all' in st.session_state:
                    st.session_state.exec_select_all = True
                
                exec_select_all = st.checkbox("Select All Executives", value=st.session_state.exec_select_all, key='exec_select_all')
                selected_executives = []
                
                if exec_select_all:
                    selected_executives = sales_executives
                else:
                    # Use multi-column layout for executives to save space
                    num_cols = 3
                    exec_cols = st.columns(num_cols)
                    for i, exec_name in enumerate(sales_executives):
                        col_idx = i % num_cols
                        with exec_cols[col_idx]:
                            if st.checkbox(exec_name, key=f"exec_{exec_name}"):
                                selected_executives.append(exec_name)
                
                # Month selection
                months = pd.to_datetime(sales_df[sales_date_col], dayfirst=True, errors='coerce').dt.strftime('%b %y').dropna().unique()
                if len(months) == 0:
                    st.error(f"No valid months found in '{sales_date_col}'. Check date format.")
                else:
                    selected_month = st.selectbox("Select Month", months, index=0, key='budget_month')
                    
                    # Calculate button
                    if st.button("Calculate Budget vs Billed", key='budget_calculate'):
                        if not selected_executives:
                            st.error("Please select at least one executive.")
                        else:
                            with st.spinner("Calculating..."):
                                budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df = calculate_budget_values(
                                    sales_df, budget_df, selected_month, selected_executives,
                                    sales_date_col, sales_area_col, sales_value_col, sales_qty_col, sales_product_group_col, sales_sl_code_col, sales_exec_col,
                                    budget_area_col, budget_value_col, budget_qty_col, budget_product_group_col, budget_sl_code_col, budget_exec_col
                                )
                                
                                if all(df is not None for df in [budget_vs_billed_value_df, budget_vs_billed_qty_df, overall_sales_qty_df, overall_sales_value_df]):
                                    st.success("Calculation complete!")
                                    
                                    # Display results in tabs
                                    result_tabs = st.tabs(["Budget vs Billed Qty", "Budget vs Billed Value", "Overall Sales (Qty)", "Overall Sales (Value)"])
                                    
                                    with result_tabs[0]:
                                        st.write("**Budget vs Billed Quantity**")
                                        st.dataframe(budget_vs_billed_qty_df)
                                        img_buffer = create_table_image(budget_vs_billed_qty_df, f"BUDGET AGAINST BILLED (Qty in Mt) - {selected_month}", percent_cols=[3])
                                        st.image(img_buffer, use_column_width=True)                                       
                                    
                                    with result_tabs[1]:
                                        st.write("**Budget vs Billed Value**")
                                        st.dataframe(budget_vs_billed_value_df)
                                        img_buffer = create_table_image(budget_vs_billed_value_df, f"BUDGET AGAINST BILLED (Value in Lakhs) - {selected_month}", percent_cols=[3])
                                        st.image(img_buffer, use_column_width=True)
                                    
                                    with result_tabs[2]:
                                        st.write("**Overall Sales (Qty)**")
                                        st.dataframe(overall_sales_qty_df)
                                        img_buffer = create_table_image(overall_sales_qty_df, f"OVERALL SALES (Qty In Mt) - {selected_month}", percent_cols=[3])
                                        st.image(img_buffer, use_column_width=True)
                                    
                                    with result_tabs[3]:
                                        st.write("**Overall Sales (Value)**")
                                        st.dataframe(overall_sales_value_df)
                                        img_buffer = create_table_image(overall_sales_value_df, f"OVERALL SALES (Value in Lakhs) - {selected_month}", percent_cols=[3])
                                        st.image(img_buffer, use_column_width=True)
                                    
                                    # Create individual PPT
                                    with st.spinner("Creating PPT..."):
                                        ppt_buffer = create_budget_ppt(
                                            budget_vs_billed_value_df, 
                                            budget_vs_billed_qty_df, 
                                            overall_sales_qty_df, 
                                            overall_sales_value_df, 
                                            month_title=selected_month, 
                                            logo_file=st.session_state.logo_file
                                        )
                                        
                                        if ppt_buffer:
                                            st.download_button(
                                                label="Download Budget vs Billed PPT",
                                                data=ppt_buffer,
                                                file_name=f"Budget_vs_Billed_{selected_month}.pptx",
                                                mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                                                key='budget_download'
                                            )
                                    
                                    # Store results in session state
                                    st.session_state.budget_results = [
                                        {'df': budget_vs_billed_qty_df, 'title': f"BUDGET AGAINST BILLED (Qty in Mt) - {selected_month}", 'percent_cols': [3]},
                                        {'df': budget_vs_billed_value_df, 'title': f"BUDGET AGAINST BILLED (Value in Lakhs) - {selected_month}", 'percent_cols': [3]},
                                        {'df': overall_sales_qty_df, 'title': f"OVERALL SALES (Qty In Mt) - {selected_month}", 'percent_cols': [3]},
                                        {'df': overall_sales_value_df, 'title': f"OVERALL SALES (Value in Lakhs) - {selected_month}", 'percent_cols': [3]}
                                    ]
                                else:
                                    st.error("Failed to calculate values. Check your data and selections.")
            except Exception as e:
                st.error(f"Error in Budget vs Billed tab: {e}")
                st.error(traceback.format_exc())
    
    # MODULE 2: Number of Billed Customers and OD Target Tab 
    with tabs[1]:
        st.header("OD Target vs Collection Report")
        
        if not st.session_state.os_jan_file or not st.session_state.os_feb_file or not st.session_state.sales_file:
            st.warning("⚠️ Please upload OS-First, OS-Second, and Sales files to use this tab")
        else:
            try:
                # Get sheet names
                os_jan_sheets = get_excel_sheets(st.session_state.os_jan_file)
                os_feb_sheets = get_excel_sheets(st.session_state.os_feb_file)
                sales_sheets = get_excel_sheets(st.session_state.sales_file)
                
                # Sheet selection
                st.subheader("Sheet Selection")
                col1, col2, col3 = st.columns(3)
                with col1:
                    os_jan_sheet = st.selectbox("OS-First Sheet", os_jan_sheets, key='od_os_jan_sheet')
                    os_jan_header = st.number_input("OS-First Header Row (1-based)", min_value=1, max_value=10, value=1, key='od_os_jan_header') - 1
                with col2:
                    os_feb_sheet = st.selectbox("OS-Second Sheet", os_feb_sheets, key='od_os_feb_sheet')
                    os_feb_header = st.number_input("OS-Second Header Row (1-based)", min_value=1, max_value=10, value=1, key='od_os_feb_header') - 1
                with col3:
                    sales_sheet = st.selectbox("Sales Sheet", sales_sheets, key='od_sales_sheet')
                    sales_header = st.number_input("Sales Header Row (1-based)", min_value=1, max_value=10, value=1, key='od_sales_header') - 1
                
                # Load data
                os_jan = pd.read_excel(st.session_state.os_jan_file, sheet_name=os_jan_sheet, header=os_jan_header)
                os_feb = pd.read_excel(st.session_state.os_feb_file, sheet_name=os_feb_sheet, header=os_feb_header)
                total_sale = pd.read_excel(st.session_state.sales_file, sheet_name=sales_sheet, header=sales_header)
                
                # Column mappings
                with st.expander("Column Mappings"):
                    # OS Jan Column Mapping
                    st.subheader("OS-First Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        os_jan_due_date_col = st.selectbox("Due Date Column", os_jan.columns.tolist(), key='od_os_jan_due_date')
                        os_jan_ref_date_col = st.selectbox("Reference Date Column", os_jan.columns.tolist(), key='od_os_jan_ref_date')
                    with col2:
                        os_jan_net_value_col = st.selectbox("Net Value Column", os_jan.columns.tolist(), key='od_os_jan_net_value')
                        os_jan_sl_code_col = st.selectbox("SL Code Column", os_jan.columns.tolist(), key='od_os_jan_sl_code')
                    with col3:
                        os_jan_exec_col = st.selectbox("Executive Column", os_jan.columns.tolist(), key='od_os_jan_exec')

                    # OS Feb Column Mapping
                    st.subheader("OS-Second Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        os_feb_due_date_col = st.selectbox("Due Date Column", os_feb.columns.tolist(), key='od_os_feb_due_date')
                        os_feb_ref_date_col = st.selectbox("Reference Date Column", os_feb.columns.tolist(), key='od_os_feb_ref_date')
                    with col2:
                        os_feb_net_value_col = st.selectbox("Net Value Column", os_feb.columns.tolist(), key='od_os_feb_net_value')
                        os_feb_sl_code_col = st.selectbox("SL Code Column", os_feb.columns.tolist(), key='od_os_feb_sl_code')
                    with col3:
                        os_feb_exec_col = st.selectbox("Executive Column", os_feb.columns.tolist(), key='od_os_feb_exec')

                    # Sales Column Mapping
                    st.subheader("Total Sale Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        sale_bill_date_col = st.selectbox("Sales Bill Date Column", total_sale.columns.tolist(), key='od_sale_bill_date')
                        sale_due_date_col = st.selectbox("Sales Due Date Column", total_sale.columns.tolist(), key='od_sale_due_date')
                    with col2:
                        sale_value_col = st.selectbox("Sales Value Column", total_sale.columns.tolist(), key='od_sale_value')
                        sale_sl_code_col = st.selectbox("Sales SL Code Column", total_sale.columns.tolist(), key='od_sale_sl_code')
                    with col3:
                        sale_exec_col = st.selectbox("Sales Executive Column", total_sale.columns.tolist(), key='od_sale_exec')
                
                # Get available months
                available_months = get_available_months(
                    os_jan, os_feb, total_sale,
                    os_jan_due_date_col, os_jan_ref_date_col,
                    os_feb_due_date_col, os_feb_ref_date_col,
                    sale_bill_date_col, sale_due_date_col
                )
                
                if not available_months:
                    st.error("No valid months found in the date columns. Please check column selections.")
                else:
                    # Month selection
                    st.subheader("Select Month")
                    selected_month_str = st.selectbox("Month", available_months, key='od_month')
                    
                    # Executive selection
                    st.subheader("Executive Selection")
                    all_executives = set()
                    for df, exec_col in [
                        (os_jan, os_jan_exec_col),
                        (os_feb, os_feb_exec_col),
                        (total_sale, sale_exec_col)
                    ]:
                        if exec_col in df.columns:
                            execs = df[exec_col].dropna().astype(str).unique().tolist()
                            all_executives.update(execs)
                    all_executives = sorted(list(all_executives))
                    
                    exec_select_all = st.checkbox("Select All Executives", value=True, key='od_vs_exec_all')
                    selected_od_executives = []
                    
                    if exec_select_all:
                        selected_od_executives = all_executives
                    else:
                        # Use multi-column layout for executives to save space
                        num_cols = 3
                        exec_cols = st.columns(num_cols)
                        for i, exec_name in enumerate(all_executives):
                            col_idx = i % num_cols
                            with exec_cols[col_idx]:
                                if st.checkbox(exec_name, key=f'od_vs_exec_{exec_name}'):
                                    selected_od_executives.append(exec_name)
                    
                    # Generate report button
                    if st.button("Generate OD Target vs Collection Report", key='od_vs_generate'):
                        if not selected_od_executives:
                            st.error("Please select at least one executive.")
                        else:
                            with st.spinner("Generating report..."):
                                final_df = calculate_od_values(
                                    os_jan, os_feb, total_sale, selected_month_str,
                                    os_jan_due_date_col, os_jan_ref_date_col, os_jan_net_value_col, os_jan_exec_col, os_jan_sl_code_col,
                                    os_feb_due_date_col, os_feb_ref_date_col, os_feb_net_value_col, os_feb_exec_col, os_feb_sl_code_col,
                                    sale_bill_date_col, sale_due_date_col, sale_value_col, sale_exec_col, sale_sl_code_col,
                                    selected_od_executives
                                )
                                
                                if final_df is not None and not final_df.empty:
                                    st.success("Report generation complete!")
                                    st.subheader("OD Target vs Collection Results")
                                    
                                    # Display the results
                                    st.dataframe(final_df)
                                    
                                    # Create table image
                                    img_buffer = create_table_image(final_df, f"OD TARGET VS COLLECTION - {selected_month_str} (Value in Lakhs)", percent_cols=[3, 6])
                                    if img_buffer:
                                        st.image(img_buffer, use_column_width=True)
                                    
                                    # Create PPT
                                    prs = Presentation()
                                    prs.slide_width = Inches(13.33)
                                    prs.slide_height = Inches(7.5)
                                    
                                    create_title_slide(prs, f"OD Target vs Collection - {selected_month_str}", st.session_state.logo_file)
                                    
                                    add_table_slide(prs, final_df, f"Executive-wise Performance - {selected_month_str}", percent_cols=[3, 6])
                                    
                                    ppt_buffer = BytesIO()
                                    prs.save(ppt_buffer)
                                    ppt_buffer.seek(0)
                                    
                                    unique_id = str(uuid.uuid4())[:8]
                                    st.download_button(
                                        label="Download OD Target vs Collection PPT",
                                        data=ppt_buffer,
                                        file_name=f"OD_Target_vs_Collection_{selected_month_str}_{unique_id}.pptx",
                                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                                        key=f"od_vs_download_{unique_id}"
                                    )
                                    
                                    # Store in session state for consolidated report
                                    st.session_state.od_vs_results = [
                                        {'df': final_df, 'title': f"OD TARGET VS COLLECTION - {selected_month_str}", 'percent_cols': [3, 6]}
                                    ]
                                else:
                                    st.error("Failed to generate report. Check your data and selections.")
            except Exception as e:
                st.error(f"Error in OD Target vs Collection tab: {e}")
                st.error(traceback.format_exc())
       
   # MODULE 3: OD TARGET VS COLLECTION (From od.py)
    with tabs[2]:
        st.header("Product Growth Dashboard")
        
        if not st.session_state.sales_file or not st.session_state.budget_file:
            st.warning("⚠️ Please upload both Sales and Budget files to use this tab")
        else:
            try:
                # Get sheet names
                sales_sheets = get_excel_sheets(st.session_state.sales_file)
                budget_sheets = get_excel_sheets(st.session_state.budget_file)
                
                # Sheet selection
                st.subheader("Configure Files")
                col1, col2 = st.columns(2)
                with col1:
                    ly_sheet = st.selectbox("Last Year Sales Sheet", sales_sheets, key='pg_ly_sheet')
                    ly_header = st.number_input("Last Year Header Row (1-based)", min_value=1, max_value=10, value=1, key='pg_ly_header') - 1
                    cy_sheet = st.selectbox("Current Year Sales Sheet", sales_sheets, key='pg_cy_sheet')
                    cy_header = st.number_input("Current Year Header Row (1-based)", min_value=1, max_value=10, value=1, key='pg_cy_header') - 1
                with col2:
                    budget_product_sheet = st.selectbox("Budget Sheet", budget_sheets, key='pg_budget_sheet')
                    budget_product_header = st.number_input("Budget Header Row (1-based)", min_value=1, max_value=10, value=1, key='pg_budget_header') - 1
                
                # Load data
                ly_df = pd.read_excel(st.session_state.sales_file, sheet_name=ly_sheet, header=ly_header)
                cy_df = pd.read_excel(st.session_state.sales_file, sheet_name=cy_sheet, header=cy_header)
                budget_df = pd.read_excel(st.session_state.budget_file, sheet_name=budget_product_sheet, header=budget_product_header)
                
                # Column mappings
                with st.expander("Column Mappings"):
                    st.subheader("Last Year Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        ly_date_col = st.selectbox("LY Date Column", ly_df.columns.tolist(), key='pg_ly_date')
                        ly_qty_col = st.selectbox("LY Quantity Column", ly_df.columns.tolist(), key='pg_ly_qty')
                    with col2:
                        ly_value_col = st.selectbox("LY Value Column", ly_df.columns.tolist(), key='pg_ly_value')
                        ly_company_group_col = st.selectbox("LY Company Group Column", ly_df.columns.tolist(), key='pg_ly_company_group')
                    with col3:
                        ly_product_group_col = st.selectbox("LY Product Group Column", ly_df.columns.tolist(), key='pg_ly_product_group')
                        ly_sl_code_col = st.selectbox("LY SL Code Column", ly_df.columns.tolist(), key='pg_ly_sl_code')
                        ly_exec_col = st.selectbox("LY Executive Column", ly_df.columns.tolist(), key='pg_ly_exec')

                    st.subheader("Current Year Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        cy_date_col = st.selectbox("CY Date Column", cy_df.columns.tolist(), key='pg_cy_date')
                        cy_qty_col = st.selectbox("CY Quantity Column", cy_df.columns.tolist(), key='pg_cy_qty')
                    with col2:
                        cy_value_col = st.selectbox("CY Value Column", cy_df.columns.tolist(), key='pg_cy_value')
                        cy_company_group_col = st.selectbox("CY Company Group Column", cy_df.columns.tolist(), key='pg_cy_company_group')
                    with col3:
                        cy_product_group_col = st.selectbox("CY Product Group Column", cy_df.columns.tolist(), key='pg_cy_product_group')
                        cy_sl_code_col = st.selectbox("CY SL Code Column", cy_df.columns.tolist(), key='pg_cy_sl_code')
                        cy_exec_col = st.selectbox("CY Executive Column", cy_df.columns.tolist(), key='pg_cy_exec')

                    st.subheader("Budget Column Mapping")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        budget_qty_col = st.selectbox("Budget Quantity Column", budget_df.columns.tolist(), key='pg_budget_qty')
                        budget_value_col = st.selectbox("Budget Value Column", budget_df.columns.tolist(), key='pg_budget_value')
                    with col2:
                        budget_company_group_col = st.selectbox("Budget Company Group Column", budget_df.columns.tolist(), key='pg_budget_company_group')
                        budget_sl_code_col = st.selectbox("Budget SL Code Column", budget_df.columns.tolist(), key='pg_budget_sl_code')
                    with col3:
                        budget_product_group_col = st.selectbox("Budget Product Group Column", budget_df.columns.tolist(), key='pg_budget_product_group')
                        budget_exec_col = st.selectbox("Budget Executive Column", budget_df.columns.tolist(), key='pg_budget_exec')
                
                # Remove empty company and product groups
                for df, col in [
                    (ly_df, ly_company_group_col), (cy_df, cy_company_group_col), (budget_df, budget_company_group_col),
                    (ly_df, ly_product_group_col), (cy_df, cy_product_group_col), (budget_df, budget_product_group_col)
                ]:
                    df[col] = df[col].replace("", np.nan)
                    # Don't drop, just standardize
                    df[col] = df[col].fillna("")
                
                # Get unique standardized company groups
                ly_groups = sorted(set(ly_df[ly_company_group_col].apply(standardize_name).unique()))
                cy_groups = sorted(set(cy_df[cy_company_group_col].apply(standardize_name).unique()))
                budget_groups = sorted(set(budget_df[budget_company_group_col].apply(standardize_name).unique()))
                
                all_company_groups = sorted(set([g for g in ly_groups + cy_groups + budget_groups if g]))
                
                # Get unique standardized product groups
                ly_product_groups = sorted(set(ly_df[ly_product_group_col].apply(standardize_name).unique()))
                cy_product_groups = sorted(set(cy_df[cy_product_group_col].apply(standardize_name).unique()))
                budget_product_groups = sorted(set(budget_df[budget_product_group_col].apply(standardize_name).unique()))
                
                all_product_groups = sorted(set([g for g in ly_product_groups + cy_product_groups + budget_product_groups if g]))
                
                # Month selection
                st.subheader("Select Months")
                
                # Get available months
                ly_dates = pd.to_datetime(ly_df[ly_date_col], dayfirst=True, errors='coerce')
                cy_dates = pd.to_datetime(cy_df[cy_date_col], dayfirst=True, errors='coerce')
                
                ly_months = sorted(ly_dates.dt.strftime('%b %y').dropna().unique().tolist())
                cy_months = sorted(cy_dates.dt.strftime('%b %y').dropna().unique().tolist())
                
                col1, col2 = st.columns(2)
                with col1:
                    selected_ly_month = st.selectbox("Last Year Month", options=ly_months, index=0, key='pg_ly_month')
                with col2:
                    selected_cy_month = st.selectbox("Current Year Month", options=cy_months, index=0, key='pg_cy_month')
                
                # Filters for executives, company groups and product groups
                st.subheader("Filter Options")
                filter_tabs = st.tabs(["Executives", "Company Groups", "Product Groups"])
                
                with filter_tabs[0]:
                    # Get all executives from all files
                    all_execs = set()
                    for df, col in [(ly_df, ly_exec_col), (cy_df, cy_exec_col), (budget_df, budget_exec_col)]:
                        execs = df[col].dropna().astype(str).unique().tolist()
                        all_execs.update(execs)
                    all_execs = sorted(all_execs)
                    
                    pg_exec_select_all = st.checkbox("Select All Executives", value=True, key='pg_exec_all')
                    selected_pg_executives = []
                    if pg_exec_select_all:
                        selected_pg_executives = all_execs
                    else:
                        # Multi-column layout for executives
                        num_cols = 3
                        exec_cols = st.columns(num_cols)
                        for i, exec_name in enumerate(all_execs):
                            col_idx = i % num_cols
                            with exec_cols[col_idx]:
                                if st.checkbox(exec_name, key=f'pg_exec_{exec_name}'):
                                    selected_pg_executives.append(exec_name)
                
                with filter_tabs[1]:
                    pg_company_select_all = st.checkbox("Select All Company Groups", value=True, key='pg_company_all')
                    selected_pg_companies = []
                    if pg_company_select_all:
                        selected_pg_companies = all_company_groups
                    else:
                        # Multi-column layout for company groups
                        num_cols = 3
                        company_cols = st.columns(num_cols)
                        for i, group in enumerate(all_company_groups):
                            col_idx = i % num_cols
                            with company_cols[col_idx]:
                                if st.checkbox(group, key=f'pg_company_{group}'):
                                    selected_pg_companies.append(group)
                
                with filter_tabs[2]:
                    pg_product_select_all = st.checkbox("Select All Product Groups", value=True, key='pg_product_all')
                    selected_pg_products = []
                    if pg_product_select_all:
                        selected_pg_products = all_product_groups
                    else:
                        # Multi-column layout for product groups
                        num_cols = 3
                        product_cols = st.columns(num_cols)
                        for i, group in enumerate(all_product_groups):
                            col_idx = i % num_cols
                            with product_cols[col_idx]:
                                if st.checkbox(group, key=f'pg_product_{group}'):
                                    selected_pg_products.append(group)
                
                # Generate report button
                if st.button("Generate Product Growth Report", key='pg_generate'):
                    with st.spinner("Generating report..."):
                        month_title = f"LY: {selected_ly_month} vs CY: {selected_cy_month}"
                        group_results = calculate_product_growth(
                            ly_df, cy_df, budget_df, selected_ly_month, selected_cy_month,
                            ly_date_col, cy_date_col, ly_qty_col, cy_qty_col, ly_value_col, cy_value_col,
                            budget_qty_col, budget_value_col, ly_company_group_col, 
                            cy_company_group_col, budget_company_group_col, 
                            ly_product_group_col, cy_product_group_col, budget_product_group_col,
                            ly_sl_code_col, cy_sl_code_col, budget_sl_code_col,
                            ly_exec_col, cy_exec_col, budget_exec_col,
                            selected_pg_executives, 
                            selected_pg_companies, 
                            selected_pg_products
                        )
                        
                        if group_results:
                            st.success("Report generation complete!")
                            
                            # Display results by company group
                            dfs_info = []
                            
                            for company, data in group_results.items():
                                st.subheader(f"Company Group: {company}")
                                
                                # Apply rounding to all numeric columns for qty_df
                                numeric_cols_qty = ['LY_QTY', 'BUDGET_QTY', 'CY_QTY', 'ACHIEVEMENT %']
                                for col in numeric_cols_qty:
                                    if col in data['qty_df'].columns:
                                        data['qty_df'][col] = data['qty_df'][col].round(2)
                                
                                # Apply rounding to all numeric columns for value_df
                                numeric_cols_value = ['LY_VALUE', 'BUDGET_VALUE', 'CY_VALUE', 'ACHIEVEMENT %']
                                for col in numeric_cols_value:
                                    if col in data['value_df'].columns:
                                        data['value_df'][col] = data['value_df'][col].round(2)
                                
                                # Quantity Growth tab
                                st.write(f"**{company} - Quantity Growth (Qty in Mt)**")
                                st.dataframe(data['qty_df'])
                                
                                # Value Growth tab
                                st.write(f"**{company} - Value Growth (Value in Lakhs)**")
                                st.dataframe(data['value_df'])
                                
                                # Add to dfs_info for consolidated PPT
                                dfs_info.append({'df': data['qty_df'], 'title': f"{company} - Quantity Growth (Qty in Mt)", 'percent_cols': [4]})
                                dfs_info.append({'df': data['value_df'], 'title': f"{company} - Value Growth (Value in Lakhs)", 'percent_cols': [4]})
                            
                            # Store results in session state
                            st.session_state.product_results = dfs_info
                            
                            # Create individual PPT
                            ppt_buffer = create_product_growth_ppt(
                                group_results, 
                                month_title,
                                st.session_state.logo_file
                            )
                            
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
                            st.error("Failed to generate report. Check your data and selections.")
            except Exception as e:
                st.error(f"Error in Product Growth tab: {e}")
                st.error(traceback.format_exc())
        
    
    # MODULE 4: PRODUCT GROWTH REPORT Tab
    with tabs[3]:
        st.header("Number of Billed Customers & OD Target")
        nbc_tab, od_tab = st.tabs(["Number of Billed Customers", "OD Target"])
        
        # Number of Billed Customers subtab
        with nbc_tab:
            if not st.session_state.sales_file:
                st.warning("⚠️ Please upload Sales file to use this tab")
            else:
                try:
                    # Get sheet names
                    sales_sheets = get_excel_sheets(st.session_state.sales_file)
                    
                    # Sheet selection
                    st.subheader("Sheet Selection")
                    sheet_name = st.selectbox("Select Sales Sheet", sales_sheets, key='nbc_sales_sheet')
                    sales_header_row = st.number_input("Sales Header Row (1-based)", min_value=1, max_value=10, value=1, step=1, key='nbc_sales_header') - 1
                    sales_df = pd.read_excel(st.session_state.sales_file, sheet_name=sheet_name, header=sales_header_row)
                    columns = sales_df.columns.tolist()
                    
                    # Column mapping
                    st.subheader("Column Mapping")
                    col1, col2 = st.columns(2)
                    with col1:
                        date_col = st.selectbox(
                            "Date Column",
                            columns,
                            help="This column should contain dates.",
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
                            "SL Code Column",
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
                            # Multi-column layout for branches
                            num_cols = 3
                            branch_cols = st.columns(num_cols)
                            selected_branches = []
                            for i, branch in enumerate(all_nbc_branches):
                                col_idx = i % num_cols
                                with branch_cols[col_idx]:
                                    if st.checkbox(branch, key=f'nbc_branch_{branch}'):
                                        selected_branches.append(branch)
                    
                    with filter_tab2:
                        # Get all executives
                        all_executives = sorted(sales_df[executive_col].dropna().unique().tolist())
                        
                        exec_select_all = st.checkbox("Select All Executives", value=True, key='nbc_exec_all')
                        if exec_select_all:
                            selected_executives = all_executives
                        else:
                            # Multi-column layout for executives
                            num_cols = 3
                            exec_cols = st.columns(num_cols)
                            selected_executives = []
                            for i, exec_name in enumerate(all_executives):
                                col_idx = i % num_cols
                                with exec_cols[col_idx]:
                                    if st.checkbox(exec_name, key=f'nbc_exec_{exec_name}'):
                                        selected_executives.append(exec_name)
                    
                    # Generate report button
                    if st.button("Generate Billed Customers Report", key='nbc_generate'):
                        with st.spinner("Generating report..."):
                            results = create_customer_table(
                                sales_df, date_col, branch_col, customer_id_col, executive_col,
                                selected_branches=selected_branches,
                                selected_executives=selected_executives
                            )
                            
                            if results:
                                st.success("Report generation complete!")
                                st.subheader("Results")
                                for fy, (result_df, sorted_months) in results.items():
                                    st.write(f"**Financial Year: {fy}**")
                                    st.dataframe(result_df)
                                    
                                    # Create table image
                                    title = "NUMBER OF BILLED CUSTOMERS"
                                    img_buffer = create_customer_table_image(result_df, title, sorted_months, fy)
                                    if img_buffer:
                                        st.image(img_buffer, use_column_width=True)
                                    
                                    # Create PPT
                                    prs = Presentation()
                                    prs.slide_width = Inches(13.33)
                                    prs.slide_height = Inches(7.5)
                                    
                                    # Create title slide
                                    create_title_slide(prs, title, st.session_state.logo_file)
                                    
                                    # Add customer slide
                                    slide_layout = prs.slide_layouts[6]  # Blank layout
                                    slide = prs.slides.add_slide(slide_layout)
                                    create_customer_ppt_slide(slide, result_df, title, sorted_months)
                                    
                                    # Save to buffer
                                    ppt_buffer = BytesIO()
                                    prs.save(ppt_buffer)
                                    ppt_buffer.seek(0)
                                    
                                    # Download button
                                    st.download_button(
                                        label="Download Billed Customers PPT",
                                        data=ppt_buffer,
                                        file_name=f"Billed_Customers_{fy}.pptx",
                                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                                        key='nbc_download'
                                    )
                                    
                                    # Store results in session state
                                    st.session_state.customers_results = [
                                        {'df': result_df, 'title': f"NUMBER OF BILLED CUSTOMERS - FY {fy}"}
                                    ]
                            else:
                                st.error("Failed to generate report. Check your data and selections.")
                except Exception as e:
                    st.error(f"Error in Number of Billed Customers tab: {e}")
                    st.error(traceback.format_exc())
        
        # OD Target subtab - from nbc2.py
         # OD Target subtab
        with od_tab:
    # OS file selection option
              os_file_choice = st.radio(
        "Choose OS file for OD Target calculation:", 
        ["OS-First", "OS-Second"],
        key="od_file_choice"
    )

    # Get the chosen file
    chosen_os_file = st.session_state.os_jan_file if os_file_choice == "OS-First" else st.session_state.os_feb_file

    if not chosen_os_file:
        st.warning(f"Selected {os_file_choice} file is not uploaded. Please upload it in the sidebar.")
    else:
        try:
            # Get sheet names
            os_sheets = get_excel_sheets(chosen_os_file)
            
            # Sheet selection
            st.subheader("Sheet Selection")
            os_sheet = st.selectbox("Select OS Sheet", os_sheets, key='od_sheet')
            header_row = st.number_input("Header Row (1-based)", min_value=1, max_value=10, value=1, step=1, key='od_header_row') - 1
            os_df = pd.read_excel(chosen_os_file, sheet_name=os_sheet, header=header_row)
            
            if st.checkbox("Preview Raw OS Data"):
                st.write("Raw OS Data (first 20 rows):")
                st.dataframe(os_df.head(20))
            
            columns = os_df.columns.tolist()
            st.subheader("OS Column Mapping")
            col1, col2 = st.columns(2)
            with col1:
                os_area_col = st.selectbox("Select Area Column", columns, help="Contains branch names", key='os_area_col')
                os_qty_col = st.selectbox("Select Net Value Column", columns, help="Contains net values", key='os_qty_col')
            with col2:
                os_due_date_col = st.selectbox("Select Due Date Column", columns, help="Contains due dates", key='os_due_date_col')
                os_exec_col = st.selectbox("Select Executive Column", columns, help="Contains executive names", key='os_exec_col')
            
            # Due Date Filter
            st.subheader("Due Date Filter")
            try:
                os_df[os_due_date_col] = pd.to_datetime(os_df[os_due_date_col], errors='coerce')
                years = sorted(os_df[os_due_date_col].dt.year.dropna().astype(int).unique())
            except Exception as e:
                st.error(f"Error processing due dates: {e}. Ensure valid date format.")
                years = []
            
            if not years:
                st.warning("No valid years found in due date column.")
            else:
                selected_years = st.multiselect(
                    "Select years for filtering",
                    options=[str(year) for year in years],
                    default=[str(year) for year in years],
                    key='year_multiselect'
                )
                if not selected_years:
                    st.error("Please select at least one year.")
                else:
                    month_options = ['January', 'February', 'March', 'April', 'May', 'June',
                                   'July', 'August', 'September', 'October', 'November', 'December']
                    till_month = st.selectbox("Select end month", month_options, key='till_month')
            
            # Branch and Executive selection
            st.subheader("Filter Options")
            filter_tabs = st.tabs(["Branches", "Executives"])
            
            with filter_tabs[0]:
                os_branches = sorted(set([b for b in os_df[os_area_col].dropna().apply(extract_area_name) if b]))
                if not os_branches:
                    st.error("No valid branches found in OS data. Check area column.")
                else:
                    os_branch_select_all = st.checkbox("Select All OS Branches", value=True, key='od_branch_all_check')
                    selected_os_branches = []
                    if os_branch_select_all:
                        selected_os_branches = os_branches
                    else:
                        num_cols = 3
                        branch_cols = st.columns(num_cols)
                        for i, branch in enumerate(os_branches):
                            col_idx = i % num_cols
                            with branch_cols[col_idx]:
                                if st.checkbox(branch, key=f'od_branch_{branch}'):
                                    selected_os_branches.append(branch)
            
            with filter_tabs[1]:
                os_executives = sorted(set([e for e in os_df[os_exec_col].apply(extract_executive_name) if e]))
                os_exec_select_all = st.checkbox("Select All OS Executives", value=True, key='od_exec_all')
                selected_os_executives = []
                if os_exec_select_all:
                    selected_os_executives = os_executives
                else:
                    num_cols = 3
                    exec_cols = st.columns(num_cols)
                    for i, exec_name in enumerate(os_executives):
                        col_idx = i % num_cols
                        with exec_cols[col_idx]:
                            if st.checkbox(exec_name, key=f'od_exec_{exec_name}'):
                                selected_os_executives.append(exec_name)
            
            # Generate OD Target report button
            if st.button("Generate OD Target Report", key='od_generate'):
                if selected_years and till_month:
                    od_target_df, start_date, end_date = filter_os_qty(
                        os_df, os_area_col, os_qty_col, os_due_date_col, os_exec_col,
                        selected_branches=selected_os_branches,
                        selected_years=selected_years,
                        till_month=till_month,
                        selected_executives=selected_os_executives
                    )
                    
                    if od_target_df is not None:
                        start_str = start_date.strftime('%b %Y') if start_date else 'All Periods'
                        end_str = end_date.strftime('%b %Y') if end_date else 'All Periods'
                        
                        od_title = f"OD Target-{end_str} (Value in Lakhs)"
                            
                        st.subheader(od_title)
                        st.dataframe(od_target_df)
                        
                        img_buffer = create_od_table_image(od_target_df, od_title)
                        if img_buffer:
                            st.image(img_buffer, use_column_width=True)
                        
                        prs = Presentation()
                        prs.slide_width = Inches(13.33)
                        prs.slide_height = Inches(7.5)
                        
                        create_title_slide(prs, od_title, st.session_state.logo_file)
                        
                        slide_layout = prs.slide_layouts[6]
                        slide = prs.slides.add_slide(slide_layout)
                        
                        create_od_ppt_slide(slide, od_target_df, od_title)
                        
                        ppt_buffer = BytesIO()
                        prs.save(ppt_buffer)
                        ppt_buffer.seek(0)
                        
                        st.download_button(
                            label="Download OD Target PPT",
                            data=ppt_buffer,
                            file_name=f"OD_Target_by_Executive_{end_str}.pptx",
                            mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                            key='od_target_download'
                        )
                        
                        if 'od_results' not in st.session_state:
                            st.session_state.od_results = []
                        st.session_state.od_results = [{'df': od_target_df, 'title': od_title}]
                        
                else:
                    st.error("Please select at least one year and an end month.")
        except Exception as e:
            st.error(f"Error in OD Target tab: {e}")
            st.error(traceback.format_exc())

# Consolidated report section
st.divider()
st.header("🔄 Consolidated Report Generator")

all_dfs_info = []
collected_sections = []

if hasattr(st.session_state, 'budget_results') and st.session_state.budget_results:
    all_dfs_info.extend(st.session_state.budget_results)
    collected_sections.append(f"Budget: {len(st.session_state.budget_results)} reports")

if hasattr(st.session_state, 'od_vs_results') and st.session_state.od_vs_results:
    all_dfs_info.extend(st.session_state.od_vs_results)
    collected_sections.append(f"OD vs Collection: {len(st.session_state.od_vs_results)} reports")

if hasattr(st.session_state, 'product_results') and st.session_state.product_results:
    all_dfs_info.extend(st.session_state.product_results)
    collected_sections.append(f"Product: {len(st.session_state.product_results)} reports")

if hasattr(st.session_state, 'customers_results') and st.session_state.customers_results:
    all_dfs_info.extend(st.session_state.customers_results)
    collected_sections.append(f"Customers: {len(st.session_state.customers_results)} reports")

if hasattr(st.session_state, 'od_results') and st.session_state.od_results:
    all_dfs_info.extend(st.session_state.od_results)
    collected_sections.append(f"OD Target: {len(st.session_state.od_results)} reports")

if all_dfs_info:
    st.info(f"Reports collected: {', '.join(collected_sections)}")
    title = st.text_input("Enter Consolidated Report Title", "ACCLLP Integrated Report")

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
