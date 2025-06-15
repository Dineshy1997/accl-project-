import pandas as pd
import streamlit as st
import re
from io import BytesIO
from pptx import Presentation
from pptx.util import Inches
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Set Streamlit page configuration for wide layout
st.set_page_config(layout='wide')
st.title("📊 Excel Dashboard - Data Table & Visualizations")

# Define exclusion terms for branches
BRANCH_EXCLUDE_TERMS = ['Total', 'TOTAL', 'Grand', 'GRAND', 'CHN Total', 'ERD SALES', 'North Total', 'WEST SALES', 'GROUP COMPANIES']

# Utility function to safely convert values to JSON-serializable types
def safe_convert_value(x):
    """Ultra-safe value conversion that handles all pandas types."""
    try:
        if x is None or (hasattr(x, 'isna') and pd.isna(x)) or pd.isna(x):
            return None
        str_val = str(x)
        if str_val.lower() in ['nan', 'none', 'nat', '', 'null']:
            return None
        return str_val.strip()
    except:
        return None

# Convert DataFrame to JSON-serializable types while preserving numerics
def make_jsonly_serializable(df):
    """Convert DataFrame columns to JSON-serializable types while preserving numerics."""
    if df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        try:
            if pd.api.types.is_numeric_dtype(df[col]):
                if pd.api.types.is_integer_dtype(df[col]):
                    df[col] = df[col].astype('Int64')
                else:
                    df[col] = df[col].astype(float)
            else:
                df[col] = [safe_convert_value(val) for val in df[col]]
        except Exception as e:
            st.warning(f"Error processing column '{col}': {e}")
            df[col] = [str(val) if val is not None else None for val in df[col]]
    return df.reset_index(drop=True)

# Find table end by detecting TOTAL SALES or GRAND TOTAL rows
def find_table_end(df, start_idx):
    """Find where table ends by looking for TOTAL SALES or GRAND TOTAL rows."""
    for i in range(start_idx, len(df)):
        row_text = ' '.join(str(cell) for cell in df.iloc[i].values if pd.notna(cell)).upper()
        if any(term in row_text for term in ['TOTAL SALES', 'GRAND TOTAL', 'OVERALL TOTAL']):
            return i + 1  # Include the total row
    return len(df)

# Create PowerPoint slide with chart image
def create_ppt_with_chart(title, chart_data, x_col, y_col, chart_type='bar'):
    """Creates PowerPoint slide with chart image."""
    ppt = Presentation()
    slide = ppt.slides.add_slide(ppt.slide_layouts[5])
    
    # Add title to slide
    txBox = slide.shapes.add_textbox(Inches(1), Inches(0.5), Inches(8), Inches(1))
    tf = txBox.text_frame
    tf.text = title
    
    # Check if y_col exists and contains numeric data
    if y_col not in chart_data.columns:
        st.error(f"Error: Column {y_col} not found in data.")
        txBox = slide.shapes.add_textbox(Inches(1), Inches(1.5), Inches(8), Inches(1))
        tf = txBox.text_frame
        tf.text = f"Error: Column {y_col} not found"
        ppt_bytes = BytesIO()
        ppt.save(ppt_bytes)
        ppt_bytes.seek(0)
        return ppt_bytes
    
    if not pd.api.types.is_numeric_dtype(chart_data[y_col]):
        st.error(f"Error: Column {y_col} is not numeric. Cannot create chart.")
        txBox = slide.shapes.add_textbox(Inches(1), Inches(1.5), Inches(8), Inches(1))
        tf = txBox.text_frame
        tf.text = f"Error: No numeric data available for {y_col}"
        ppt_bytes = BytesIO()
        ppt.save(ppt_bytes)
        ppt_bytes.seek(0)
        return ppt_bytes
    
    # Create chart
    fig, ax = plt.subplots(figsize=(12, 6))
    if chart_type == 'bar':
        chart_data.plot.bar(x=x_col, y=y_col, ax=ax, color='#2ca02c')
    elif chart_type == 'line':
        chart_data.plot.line(x=x_col, y=y_col, ax=ax, marker='o', color='#2ca02c')
    elif chart_type == 'pie':
        pie_data = chart_data[chart_data[y_col] > 0]
        if not pie_data.empty:
            pie_data.plot.pie(y=y_col, labels=pie_data[x_col], autopct='%1.1f%%', ax=ax)
        else:
            ax.text(0.5, 0.5, "No positive values to display", ha='center', va='center')
            ax.set_title(title + " (No positive data)")
    ax.set_ylabel(y_col)
    plt.xticks(rotation=0, ha='center')
    plt.tight_layout()
    
    # Save chart to buffer
    img_buffer = BytesIO()
    fig.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
    plt.close()
    img_buffer.seek(0)
    
    # Add chart to slide
    slide.shapes.add_picture(img_buffer, Inches(1), Inches(1.5), width=Inches(8))
    
    ppt_bytes = BytesIO()
    ppt.save(ppt_bytes)
    ppt_bytes.seek(0)
    return ppt_bytes

# Ensure y-axis column contains numeric data
def ensure_numeric_data(data, y_col):
    """Ensure the y-axis column contains numeric data."""
    if y_col not in data.columns:
        return False
    try:
        data[y_col] = pd.to_numeric(data[y_col].astype(str).str.replace(',', ''), errors='coerce')
        data.dropna(subset=[y_col], inplace=True)
    except Exception as e:
        st.warning(f"Failed to convert {y_col} to numeric: {e}")
        return False
    return not data.empty

# Helper function to safely get chart data for master PPT
def get_chart_data_for_ppt(data, label, first_col, visual_type):
    """Safely extract chart data for PPT generation with proper column handling."""
    if data is None or (isinstance(data, pd.DataFrame) and data.empty):
        return None, None, None
    
    try:
        if label == "Budget vs Actual":
            if 'Month' in data.columns and 'Value' in data.columns and 'Metric' in data.columns:
                return data, "Month", "Value"
            else:
                return None, None, None
                
        elif label in ["Branch Performance", "Product Performance"]:
            if len(data.columns) >= 2:
                x_col = data.columns[0]  # First column (Branch/Product name)
                y_col = data.columns[1]  # Second column (YTD Act values)
                return data, x_col, y_col
            else:
                return None, None, None
                
        elif label in ["Branch Monthwise", "Product Monthwise"]:
            if 'Month' in data.columns and 'Value' in data.columns:
                return data, "Month", "Value"
            else:
                return None, None, None
                
        else:  # Budget, LY, Act, Gr, Ach, YTD variations
            if "Month" in data.columns:
                # For monthly data
                label_clean = label.replace(",", "").replace(" ", "")
                if label_clean in data.columns:
                    return data, "Month", label_clean
                elif "Value" in data.columns:
                    return data, "Month", "Value"
            elif "Period" in data.columns:
                # For YTD data
                label_clean = label.replace(",", "").replace(" ", "")
                if label_clean in data.columns:
                    return data, "Period", label_clean
                elif "Value" in data.columns:
                    return data, "Period", "Value"
            
        return None, None, None
        
    except Exception as e:
        st.warning(f"Error processing chart data for {label}: {e}")
        return None, None, None

# Helper function to extract clean month-year from column names
def extract_month_year(col_name):
    """Extract clean month-year format from column names, removing Gr/Ach prefixes."""
    col_str = str(col_name).strip()
    
    # Remove common prefixes that we don't want in the x-axis labels
    col_str = re.sub(r'^(Gr[-\s]*|Ach[-\s]*|Act[-\s]*)', '', col_str, flags=re.IGNORECASE)
    
    # Extract month-year pattern
    month_year_match = re.search(r'(\w{3,})[-–\s]*(\d{2})', col_str, re.IGNORECASE)
    if month_year_match:
        month, year = month_year_match.groups()
        return f"{month.capitalize()}-{year}"
    
    return col_str

# File uploader
uploaded_file = st.sidebar.file_uploader("📂 Upload Excel File", type=["xlsx"])

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names
        selected_sheet = st.sidebar.selectbox("📄 Select a Sheet", sheet_names)
        df_sheet = pd.read_excel(xls, sheet_name=selected_sheet, header=None)
        
        # Try alternative reading method if data structure is suboptimal
        if df_sheet.shape[1] < 10 and df_sheet.iloc[:, 0].astype(str).str.len().max() > 200:
            try:
                df_sheet_alt = pd.read_excel(xls, sheet_name=selected_sheet, header=None, engine='openpyxl')
                if df_sheet_alt.shape[1] > df_sheet.shape[1]:
                    df_sheet = df_sheet_alt
                    st.info("✅ Improved data structure using alternative reading method")
            except:
                pass
                
        # Dynamic processing of long header rows
        if df_sheet.shape[1] < 20:
            new_data = []
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            metrics = ['Budget', 'LY', 'Act', 'Gr', 'Ach', 'YTD']
            year_pattern = r'\d{2,4}(?:[-–]\d{2,4})?'
            
            for idx, row in df_sheet.iterrows():
                if pd.notna(row.iloc[0]):
                    row_text = str(row.iloc[0]).strip()
                    if any(metric in row_text for metric in metrics) or re.search(r'SALES\s*(in\s*(MT|Value|Ton[n]?age))?', row_text, re.IGNORECASE):
                        patterns = []
                        patterns.append(r'SALES\s*in\s*(MT|Value|Ton[n]?age)', re.IGNORECASE)
                        for metric in metrics:
                            for month in months:
                                patterns.append(rf'{metric}[-–\s]*{month}[-–\s]*{year_pattern}', re.IGNORECASE)
                            patterns.append(rf'{metric}[-–\s]*YTD[-–\s]*{year_pattern}\s*\([^)]*\)', re.IGNORECASE)
                            patterns.append(rf'YTD[-–\s]*{year_pattern}\s*\([^)]*\)\s*{metric}', re.IGNORECASE)
                        
                        positions = []
                        for pattern in patterns:
                            for match in re.finditer(pattern, row_text):
                                positions.append((match.start(), match.group()))
                        positions.sort()
                        parts = [item[1].strip() for item in positions]
                        
                        if len(parts) < 5:
                            parts = [part.strip() for part in row_text.split() if part.strip()]
                        
                        new_data.append(parts)
                    else:
                        new_data.append(row_text.split())
                else:
                    new_data.append([])
            
            if new_data:
                max_cols = max(len(row) for row in new_data)
                for row in new_data:
                    while len(row) < max_cols:
                        row.append(None)
                df_sheet = pd.DataFrame(new_data)
        
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        st.stop()

    # Determine sheet type and index
    sheet_index = sheet_names.index(selected_sheet)
    is_first_sheet = sheet_index == 0
    is_sales_monthwise = 'sales analysis month wise' in selected_sheet.lower() or ('sales' in selected_sheet.lower() and 'month' in selected_sheet.lower())

    # Initialize table_name variable
    table_name = ""

    if is_first_sheet:
        st.subheader("📋 First Sheet - Table Detection")
        
        table1_start = None
        table2_start = None
        for i in range(len(df_sheet)):
            row_text = ' '.join(str(cell) for cell in df_sheet.iloc[i].values if pd.notna(cell))
            if re.search(r'\bsales\s*in\s*mt\b', row_text, re.IGNORECASE) and table1_start is None:
                table1_start = i
            elif re.search(r'\bsales\s*in\s*(value|tonnage|tonage)\b', row_text, re.IGNORECASE) and table1_start is not None and table2_start is None:
                table2_start = i
        
        table_options = []
        if table1_start is not None:
            table_options.append("Table 1: SALES IN MT")
        if table2_start is not None:
            table_options.append("Table 2: SALES IN VALUE")
        
        if table_options:
            table_choice = st.sidebar.radio("📌 Select Table", table_options, key="first_sheet_table_select")
            table_name = table_choice  # Set table name for visualizations
            
            if table_choice == "Table 1: SALES IN MT" and table1_start is not None:
                st.write("### Table 1: SALES IN MT")
                table1_end = table2_start if table2_start is not None else len(df_sheet)
                table1 = df_sheet.iloc[table1_start:table1_end].dropna(how='all').reset_index(drop=True)
                
                if not table1.empty:
                    header_row_idx = None
                    for i in range(min(3, len(table1))):
                        row_text = ' '.join(str(cell) for cell in table1.iloc[i].values if pd.notna(cell))
                        if re.search(r'\b(budget|ly|act|gr|ach)\b', row_text, re.IGNORECASE):
                            header_row_idx = i
                            break
                    
                    if header_row_idx is not None:
                        header_row = table1.iloc[header_row_idx]
                        new_columns = []
                        for i, val in enumerate(header_row):
                            if pd.notna(val) and str(val).strip():
                                col_name = str(val).strip()
                                if len(col_name) > 100:
                                    parts = re.split(r'\s+(?=Budget|LY|Act|Gr|Ach|YTD)', col_name)
                                    if len(parts) > 1:
                                        col_name = parts[0]
                                new_columns.append(col_name)
                            else:
                                new_columns.append(f'Unnamed_{i}')
                        
                        if len(new_columns) < len(table1.columns):
                            while len(new_columns) < len(table1.columns):
                                new_columns.append(f'Unnamed_{len(new_columns)}')
                        elif len(new_columns) > len(table1.columns):
                            new_columns = new_columns[:len(table1.columns)]
                        
                        if pd.notna(header_row.iloc[0]) and len(str(header_row.iloc[0])) > 100:
                            header_text = str(header_row.iloc[0])
                            split_headers = re.split(r'\s+(?=Budget-|LY-|Act-|Gr-|Ach-|YTD-)', header_text)
                            split_headers = [h.strip() for h in split_headers if h.strip()]
                            if len(split_headers) > 5:
                                new_columns = split_headers
                                while len(new_columns) < len(table1.columns):
                                    new_columns.append(f'Unnamed_{len(new_columns)}')
                                new_columns = new_columns[:len(table1.columns)]
                        
                        table1.columns = new_columns
                        table1 = table1.iloc[header_row_idx + 1:].reset_index(drop=True)
                        
                        if not table1.empty:
                            table1 = make_jsonly_serializable(table1)
                            st.dataframe(table1, use_container_width=True)
                            csv1 = table1.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "⬇️ Download Table 1 as CSV", 
                                csv1, 
                                "sales_in_mt.csv", 
                                "text/csv",
                                key="download_table1_csv"
                            )
                        else:
                            st.warning("No data available for Table 1 after processing.")
                    else:
                        st.error("Could not find column headers for Table 1.")
                        st.dataframe(table1)
            
            elif table_choice == "Table 2: SALES IN VALUE" and table2_start is not None:
                st.write("### Table 2: SALES IN VALUE")
                table2 = df_sheet.iloc[table2_start:].dropna(how='all').reset_index(drop=True)
                
                if not table2.empty:
                    header_row_idx = None
                    for i in range(min(5, len(table2))):
                        row_text = ' '.join(str(cell) for cell in table2.iloc[i].values if pd.notna(cell))
                        if re.search(r'\b(budget|ly|act|gr|ach)\b', row_text, re.IGNORECASE):
                            header_row_idx = i
                            break
                    
                    if header_row_idx is not None:
                        header_row = table2.iloc[header_row_idx]
                        new_columns = []
                        for i, val in enumerate(header_row):
                            if pd.notna(val) and str(val).strip():
                                col_name = str(val).strip()
                                if len(col_name) > 100:
                                    parts = re.split(r'\s+(?=Budget|LY|Act|Gr|Ach|YTD)', col_name)
                                    if len(parts) > 1:
                                        col_name = parts[0]
                                new_columns.append(col_name)
                            else:
                                new_columns.append(f'Unnamed_{i}')
                        
                        if len(new_columns) < len(table2.columns):
                            while len(new_columns) < len(table2.columns):
                                new_columns.append(f'Unnamed_{len(new_columns)}')
                        elif len(new_columns) > len(table2.columns):
                            new_columns = new_columns[:len(table2.columns)]
                        
                        if pd.notna(header_row.iloc[0]) and len(str(header_row.iloc[0])) > 100:
                            header_text = str(header_row.iloc[0])
                            split_headers = re.split(r'\s+(?=Budget-|LY-|Act-|Gr-|Ach-|YTD-)', header_text)
                            split_headers = [h.strip() for h in split_headers if h.strip()]
                            if len(split_headers) > 5:
                                new_columns = split_headers
                                while len(new_columns) < len(table2.columns):
                                    new_columns.append(f'Unnamed_{len(new_columns)}')
                                new_columns = new_columns[:len(table2.columns)]
                        
                        table2.columns = new_columns
                        table2 = table2.iloc[header_row_idx + 1:].reset_index(drop=True)
                        
                        if not table2.empty:
                            table2 = make_jsonly_serializable(table2)
                            st.dataframe(table2, use_container_width=True)
                            csv2 = table2.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "⬇️ Download Table 2 as CSV", 
                                csv2, 
                                "sales_in_value_tonage.csv", 
                                "text/csv",
                                key="download_table2_csv"
                            )
                        else:
                            st.warning("No data available for Table 2 after processing.")
                    else:
                        st.error("Could not find column headers for Table 2.")
                        st.dataframe(table2)
                else:
                    st.warning("Table 2 is empty or contains no valid data.")
        else:
            st.warning("No tables ('SALES IN MT' or 'SALES IN VALUE/TONAGE') found in the first sheet.")
            df_sheet_clean = make_jsonly_serializable(df_sheet)
            st.dataframe(df_sheet_clean, use_container_width=True)
            csv = df_sheet_clean.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️ Download Raw Data as CSV", 
                csv, 
                "raw_data.csv", 
                "text/csv",
                key="download_raw_data_csv"
            )
    
    elif is_sales_monthwise:
        st.subheader(f"📋 {selected_sheet} - Sales Month Wise Analysis")
        
        table1_start = None
        table2_start = None
        for i in range(len(df_sheet)):
            row_text = ' '.join(str(cell) for cell in df_sheet.iloc[i].values if pd.notna(cell))
            if re.search(r'\bsales\s*in\s*mt\b', row_text, re.IGNORECASE) and table1_start is None:
                table1_start = i
            elif re.search(r'\bsales\s*in\s*(tonnage|tonage)\b', row_text, re.IGNORECASE) and table1_start is not None and table2_start is None:
                table2_start = i
        
        table_options = []
        if table1_start is not None:
            table_options.append("Table 1: SALES IN MT")
        if table2_start is not None:
            table_options.append("Table 2: SALES IN TONAGE")
        
        if table_options:
            table_choice = st.sidebar.radio("📌 Select Table", table_options, key="sales_monthwise_table_select")
            table_name = table_choice  # Set table name for visualizations
            
            if table_choice == "Table 1: SALES IN MT" and table1_start is not None:
                if sheet_index >= 1 and sheet_index <= 4:
                    table1_end = find_table_end(df_sheet, table1_start)
                else:
                    table1_end = table2_start if table2_start is not None else len(df_sheet)
                
                table1 = df_sheet.iloc[table1_start:table1_end].dropna(how='all').reset_index(drop=True)
                
                if not table1.empty:
                    header_row_idx = None
                    for i in range(min(5, len(table1))):
                        row_text = ' '.join(str(cell) for cell in table1.iloc[i].values if pd.notna(cell))
                        if re.search(r'\b(budget|ly|act|gr|ach)\b', row_text, re.IGNORECASE):
                            header_row_idx = i
                            break
                    
                    if header_row_idx is not None:
                        header_row = table1.iloc[header_row_idx]
                        new_columns = [str(val).strip() if pd.notna(val) else f'Unnamed_{i}' 
                                      for i, val in enumerate(header_row)]
                        table1.columns = new_columns
                        table1 = table1.iloc[header_row_idx + 1:].reset_index(drop=True)
                        
                        if 2 <= sheet_index <= 4:
                            if not table1.empty:
                                table1 = table1.drop(index=0).reset_index(drop=True)
                            else:
                                st.warning("Table 1 is empty after processing, cannot delete first row.")
                        
                        if sheet_index == 1 and not table1.empty:
                            table1 = table1[~table1[table1.columns[0]].str.contains('REGIONS', case=False, na=False)].reset_index(drop=True)
                        
                        if not table1.empty:
                            table1 = make_jsonly_serializable(table1)
                            st.dataframe(table1, use_container_width=True)
                            csv1 = table1.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "⬇️ Download Table 1 as CSV", 
                                csv1, 
                                "sales_in_mt.csv", 
                                "text/csv",
                                key="download_table1_csv"
                            )
                        else:
                            st.warning("No data available for Table 1 after processing.")
                    else:
                        st.error("Could not find column headers for Table 1.")
                        st.dataframe(table1)
            
            elif table_choice == "Table 2: SALES IN TONAGE" and table2_start is not None:
                if sheet_index >= 1 and sheet_index <= 4:
                    table2_end = find_table_end(df_sheet, table2_start)
                    table2 = df_sheet.iloc[table2_start:table2_end].dropna(how='all').reset_index(drop=True)
                else:
                    table2 = df_sheet.iloc[table2_start:].dropna(how='all').reset_index(drop=True)
                
                if not table2.empty:
                    header_row_idx = None
                    for i in range(min(5, len(table2))):
                        row_text = ' '.join(str(cell) for cell in table2.iloc[i].values if pd.notna(cell))
                        if re.search(r'\b(budget|ly|act|gr|ach)\b', row_text, re.IGNORECASE):
                            header_row_idx = i
                            break
                    
                    if header_row_idx is not None:
                        header_row = table2.iloc[header_row_idx]
                        new_columns = [str(val).strip() if pd.notna(val) else f'Unnamed_{i}' 
                                      for i, val in enumerate(header_row)]
                        table2.columns = new_columns
                        table2 = table2.iloc[header_row_idx + 1:].reset_index(drop=True)
                        
                        if 2 <= sheet_index <= 4:
                            if not table2.empty:
                                table2 = table2.drop(index=0).reset_index(drop=True)
                            else:
                                st.warning("Table 2 is empty after processing, cannot delete first row.")
                        
                        if sheet_index == 1 and not table2.empty:
                            table2 = table2[~table2[table2.columns[0]].str.contains('REGIONS', case=False, na=False)].reset_index(drop=True)
                        
                        if not table2.empty:
                            table2 = make_jsonly_serializable(table2)
                            st.dataframe(table2, use_container_width=True)
                            csv2 = table2.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "⬇️ Download Table 2 as CSV", 
                                csv2, 
                                "sales_in_tonage.csv", 
                                "text/csv",
                                key="download_table2_csv"
                            )
                        else:
                            st.warning("No data available for Table 2 after processing.")
                    else:
                        st.error("Could not find column headers for Table 2.")
                        st.dataframe(table2)
                else:
                    st.warning("Table 2 is empty or contains no valid data.")
        else:
            st.warning("No tables ('SALES IN MT' or 'SALES IN TONAGE') found in the sheet.")
            df_sheet_clean = make_jsonly_serializable(df_sheet)
            st.dataframe(df_sheet_clean, use_container_width=True)
            csv = df_sheet_clean.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️ Download Raw Data as CSV", 
                csv, 
                "raw_data.csv", 
                "text/csv",
                key="download_raw_data_csv"
            )
    
    else:
        is_product_analysis = ('product' in selected_sheet.lower() or 
                             'ts-pw' in selected_sheet.lower() or 
                             'ero-pw' in selected_sheet.lower())
        is_branch_analysis = 'region wise analysis' in selected_sheet.lower()

        if is_branch_analysis:
            table1_header = "Sales in MT"
            table2_header = "Sales in Value"
        else:
            table1_header = "Sales in Tonage"
            table2_header = "Sales in Value"

        def extract_tables(df, table1_header, table2_header):
            table1_idx, table2_idx = None, None
            for i in range(len(df)):
                row_text = ' '.join(df.iloc[i].astype(str).str.lower().tolist())
                if table1_idx is None and table1_header.lower() in row_text:
                    table1_idx = i
                elif table2_idx is None and table2_header.lower() in row_text and i > (table1_idx or 0):
                    table2_idx = i
            return table1_idx, table2_idx

        idx1, idx2 = extract_tables(df_sheet, table1_header, table2_header)

        if idx1 is None:
            st.error(f"❌ Could not locate '{table1_header}' header in the sheet.")
        else:
            if sheet_index >= 1 and sheet_index <= 4:
                table1_end = find_table_end(df_sheet, idx1 + 1)
                if idx2 is not None:
                    table1_end = min(table1_end, idx2)
            else:
                table1_end = idx2 if idx2 is not None else len(df_sheet)
            
            table1 = df_sheet.iloc[idx1+1:table1_end].dropna(how='all').reset_index(drop=True)
            table1.columns = df_sheet.iloc[idx1].apply(lambda x: str(x).strip() if pd.notna(x) else '')
            
            if not table1.empty:
                first_row = table1.iloc[0]
                first_cell = str(first_row[0]).strip().lower() if pd.notna(first_row[0]) else ""
                is_subheader = (first_cell == "" or 
                               first_cell.startswith("unnamed") or 
                               any(term.lower() in first_cell for term in BRANCH_EXCLUDE_TERMS))
                if is_subheader:
                    table1 = table1.drop(index=0).reset_index(drop=True)

            if idx2 is not None:
                if sheet_index >= 1 and sheet_index <= 4:
                    table2_end = find_table_end(df_sheet, idx2 + 1)
                    table2 = df_sheet.iloc[idx2+1:table2_end].dropna(how='all').reset_index(drop=True)
                else:
                    table2 = df_sheet.iloc[idx2+1:].dropna(how='all').reset_index(drop=True)
                
                table2.columns = df_sheet.iloc[idx2].apply(lambda x: str(x).strip() if pd.notna(x) else '')
                
                if not table2.empty:
                    first_row = table2.iloc[0]
                    first_cell = str(first_row[0]).strip().lower() if pd.notna(first_row[0]) else ""
                    is_subheader = (first_cell == "" or 
                                   first_cell.startswith("unnamed") or 
                                   any(term.lower() in first_cell for term in BRANCH_EXCLUDE_TERMS))
                    if is_subheader:
                        table2 = table2.drop(index=0).reset_index(drop=True)
            else:
                table2 = None

            table_options = [f"Table 1: {table1_header.upper()}"]
            if table2 is not None:
                table_options.append(f"Table 2: {table2_header.upper()}")
            table_choice = st.sidebar.radio("📌 Select Table", table_options)
            table_name = table_choice  # Set table name for visualizations
            table_df = table1 if table_choice == table_options[0] else table2

            table_df = make_jsonly_serializable(table_df)
            table_df.columns = table_df.columns.map(str)

            def rename_columns(columns):
                renamed = []
                ytd_base = None
                prev_month = None
                prev_year = None
                for col in columns:
                    col_clean = col.strip().replace(",", "").replace("–", "-")
                    ytd_act_match = re.search(r'(YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\))\s*Act', col_clean, re.IGNORECASE)
                    if ytd_act_match:
                        ytd_base = ytd_act_match.group(1).replace("–", "-")
                        renamed.append(f"Act-{ytd_base}")
                        continue
                    ytd_gr_match = re.search(r'(Gr-YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\))', col_clean, re.IGNORECASE)
                    if ytd_gr_match:
                        gr_ytd = ytd_gr_match.group(1).replace("–", "-")
                        renamed.append(f"Gr-{gr_ytd.split('Gr-')[1]}")
                        continue
                    ytd_ach_match = re.search(r'(Ach-YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\))', col_clean, re.IGNORECASE)
                    ytd_ach_alt_match = re.search(r'(YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\))\s*Ach', col_clean, re.IGNORECASE)
                    if ytd_ach_match:
                        ach_ytd = ytd_ach_match.group(1).replace("–", "-")
                        renamed.append(ach_ytd)
                        continue
                    elif ytd_ach_alt_match:
                        ytd_part = ytd_ach_alt_match.group(1).replace("–", "-")
                        renamed.append(f"Ach-{ytd_part}")
                        continue
                    monthly_match = re.search(r'(\b\w{3,})[\s-]*(\d{2})', col_clean)
                    if monthly_match:
                        prev_month, prev_year = monthly_match.groups()
                        prev_month = prev_month.capitalize()
                    if col_clean.lower().startswith("gr") and prev_month and prev_year:
                        renamed.append(f"Gr - {prev_month}-{prev_year}")
                    elif col_clean.lower().startswith("ach") and prev_month and prev_year:
                        renamed.append(f"Ach - {prev_month}-{prev_year}")
                    else:
                        renamed.append(col)
                return renamed

            table_df.columns = rename_columns(table_df.columns)

            if table_df.columns.duplicated().any():
                table_df = table_df.loc[:, ~table_df.columns.duplicated()]
                st.warning("⚠️ Duplicate columns detected and removed.")

            branch_list = []
            product_list = []

            def extract_unique_values(df, first_col, exclude_terms=None):
                if exclude_terms is None:
                    exclude_terms = ['Total', 'TOTAL', 'Grand', 'GRAND', 'CHN Total', 'ERD SALES']
                
                valid_rows = df[~df[first_col].str.contains('|'.join(exclude_terms), na=False, case=False)]
                valid_rows = valid_rows[valid_rows[first_col].notna()]
                
                unique_values = valid_rows[first_col].astype(str).str.strip().unique()
                return sorted(unique_values)

            if is_branch_analysis:
                branch_list = extract_unique_values(table_df, table_df.columns[0])
            elif is_product_analysis:
                product_list = extract_unique_values(table_df, table_df.columns[0])

            months = sorted(set(re.findall(r'\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b', 
                       ' '.join(table_df.columns), flags=re.IGNORECASE)))
            years = sorted(set(re.findall(r'[-–](\d{2})\b', ' '.join(table_df.columns))))

            # Add "Select All" option to filters
            months_options = ["Select All"] + months
            years_options = ["Select All"] + years
            branches_options = ["Select All"] + branch_list if is_branch_analysis else []
            products_options = ["Select All"] + product_list if is_product_analysis else []

            selected_month = st.sidebar.selectbox("📅 Filter by Month", months_options, index=0)
            selected_year = st.sidebar.selectbox("📆 Filter by Year", years_options, index=0)
            selected_branch = st.sidebar.selectbox("🌍 Filter by Branch", branches_options, index=0) if is_branch_analysis else None
            selected_product = st.sidebar.selectbox("📦 Filter by Product", products_options, index=0) if is_product_analysis else None

            # Handle "Select All" logic
            selected_months = months if selected_month == "Select All" else [selected_month] if selected_month else []
            selected_years = years if selected_year == "Select All" else [selected_year] if selected_year else []
            selected_branches = branch_list if selected_branch == "Select All" else [selected_branch] if selected_branch else []
            selected_products = product_list if selected_product == "Select All" else [selected_product] if selected_product else []

            filtered_df = table_df.copy()
            first_col = filtered_df.columns[0]

            if selected_branches and is_branch_analysis:
                filtered_df = filtered_df[filtered_df[first_col].astype(str).isin(selected_branches)]
            if selected_products and is_product_analysis:
                filtered_df = filtered_df[filtered_df[first_col].astype(str).isin(selected_products)]

            def column_filter(col):
                col_str = str(col).lower().replace(",", "").replace("–", "-")
                if "ytd" in col_str:
                    return any(f"-{y}" in col_str for y in selected_years) if selected_years else True
                month_match = any(m.lower() in col_str for m in selected_months)
                year_match = any(f"-{y}" in col_str for y in selected_years) if selected_years else True
                return month_match and year_match

            visual_cols = [col for col in table_df.columns if column_filter(col)]
            display_df = filtered_df[[first_col] + visual_cols] if visual_cols else filtered_df[[first_col]]

            display_df = make_jsonly_serializable(display_df)

            def convert_to_numeric(series):
                try:
                    return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce')
                except:
                    return series

            formatted_df = display_df.copy()
            numeric_cols = []
            for col in formatted_df.columns:
                if col == formatted_df.columns[0]:
                    continue
                formatted_df[col] = convert_to_numeric(formatted_df[col])
                if pd.api.types.is_numeric_dtype(formatted_df[col]):
                    numeric_cols.append(col)
                    formatted_df[col] = formatted_df[col].round(2)

            style_dict = {col: "{:.2f}" for col in numeric_cols}
            st.subheader("📋 Filtered Table View")
            st.dataframe(formatted_df.style.format(style_dict, na_rep="-"), use_container_width=True)

            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️ Download Filtered Data as CSV", 
                csv, 
                "filtered_data.csv",
                "text/csv",
                key="download_filtered_data_csv"
            )

            st.sidebar.markdown("---")
            st.sidebar.subheader("📊 Visualization Options")
            
            visual_type = st.sidebar.selectbox(
                "Select Visualization Type",
                ["Bar Chart", "Pie Chart", "Line Chart"],
                index=0,
                key="visualization_type_select"
            )

            tabs = st.tabs([
                "📊 Budget vs Actual", "📊 Budget", "📊 LY", "📊 Act", "📊 Gr", "📊 Ach", 
                "📈 YTD Budget", "📈 YTD LY", "📈 YTD Act", "📈 YTD Gr", "📈 YTD Ach", 
                "🌍 Branch Performance", "🌍 Branch Monthwise", 
                "📦 Product Performance", "📦 Product Monthwise"
            ])
            tab_names = [
                "Budget vs Actual", "Budget", "LY", "Act", "Gr", "Ach",
                "YTD Budget", "YTD LY", "YTD Act", "YTD Gr", "YTD Ach",
                "Branch Performance", "Branch Monthwise",
                "Product Performance", "Product Monthwise"
            ]
            tabs_dict = dict(zip(tab_names, tabs))

            def plot_budget_vs_actual(tab, visual_type):
                with tab:
                    budget_cols = [col for col in table_df.columns 
                                  if str(col).lower().startswith('budget') and 'ytd' not in str(col).lower()
                                  and column_filter(col)]
                    act_cols = [col for col in table_df.columns 
                               if str(col).lower().startswith('act') and 'ytd' not in str(col).lower()
                               and column_filter(col)]
                
                    if not (budget_cols and act_cols):
                        st.info("No matching Budget or Act columns found for comparison")
                        return None
                
                    budget_months = [re.search(r'(\w{3,})[-–](\d{2})', str(col), re.IGNORECASE) for col in budget_cols]
                    act_months = [re.search(r'(\w{3,})[-–](\d{2})', str(col), re.IGNORECASE) for col in act_cols]
                    common_months = set((m.group(1), m.group(2)) for m in budget_months if m) & \
                                    set((m.group(1), m.group(2)) for m in act_months if m)
                
                    if not common_months:
                        st.info("No common months found for Budget vs Actual comparison")
                        return None
                
                    selected_budget_cols = []
                    selected_act_cols = []
                    for month, year in common_months:
                        for col in budget_cols:
                            if re.search(rf'\b{month}[-–]{year}\b', str(col), re.IGNORECASE):
                                selected_budget_cols.append(col)
                        for col in act_cols:
                            if re.search(rf'\b{month}[-–]{year}\b', str(col), re.IGNORECASE):
                                selected_act_cols.append(col)
                
                    chart_data = filtered_df[[first_col] + selected_budget_cols + selected_act_cols].copy()
                
                    for col in selected_budget_cols + selected_act_cols:
                        chart_data[col] = pd.to_numeric(chart_data[col].astype(str).str.replace(',', ''), 
                                                     errors='coerce')
                
                    chart_data = chart_data.dropna()
                
                    if chart_data.empty:
                        st.warning("No valid numeric data available for Budget vs Act comparison")
                        return None
                
                    chart_data_melt = chart_data.melt(id_vars=first_col, 
                                                   var_name="Month_Metric", 
                                                   value_name="Value")
                    chart_data_melt['Metric'] = chart_data_melt['Month_Metric'].apply(
                        lambda x: 'Budget' if 'budget' in str(x).lower() else 'Act'
                    )
                    chart_data_melt['Month'] = chart_data_melt['Month_Metric'].apply(
                        lambda x: re.search(r'(\w{3,})[-–](\d{2})', str(x), re.IGNORECASE).group(0) 
                                  if re.search(r'(\w{3,})[-–](\d{2})', str(x), re.IGNORECASE) else x
                    )
                
                    chart_data_melt = make_jsonly_serializable(chart_data_melt)
                
                    # Aggregate data and verify output
                    chart_data_agg = chart_data_melt.groupby(['Month', 'Metric'])['Value'].sum().reset_index()
                
                    # Check if aggregation produced valid data
                    if chart_data_agg.empty or 'Value' not in chart_data_agg.columns:
                        st.warning("Aggregation failed: No valid data for Budget vs Actual comparison")
                        return None
                
                    # Ensure 'Value' column is numeric
                    chart_data_agg['Value'] = pd.to_numeric(chart_data_agg['Value'], errors='coerce')
                    if chart_data_agg['Value'].isna().all():
                        st.warning("No numeric values available in aggregated data for Budget vs Actual")
                        return None
                
                    if not ensure_numeric_data(chart_data_agg, 'Value'):
                        st.warning("No numeric data available for Budget vs Actual comparison")
                        return None
                
                    st.markdown(f"### Budget vs Actual Comparison - {table_name}")
                
                    if visual_type == "Bar Chart":
                        try:
                            import plotly.express as px
                            fig = px.bar(chart_data_agg, x='Month', y='Value', color='Metric',
                                       barmode='group', title=f"Budget vs Actual Comparison - {table_name}",
                                       height=400, color_discrete_sequence=['#ff7f0e', '#2ca02c'])
                            fig.update_layout(xaxis_tickangle=0)
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(12, 6))
                            budget_data = chart_data_agg[chart_data_agg['Metric'] == 'Budget']
                            act_data = chart_data_agg[chart_data_agg['Metric'] == 'Act']
                            bar_width = 0.35
                            index = np.arange(len(budget_data))
                            ax.bar(index - bar_width/2, budget_data['Value'], bar_width, label='Budget', color='#ff7f0e')
                            ax.bar(index + bar_width/2, act_data['Value'], bar_width, label='Act', color='#2ca02c')
                            ax.set_xticks(index)
                            ax.set_xticklabels(budget_data['Month'], rotation=0)
                            ax.set_title(f"Budget vs Actual Comparison - {table_name}")
                            ax.set_xlabel("Month")
                            ax.set_ylabel("Value")
                            ax.legend()
                            plt.tight_layout()
                            st.pyplot(fig)
                
                    elif visual_type == "Line Chart":
                        try:
                            import plotly.express as px
                            fig = px.line(chart_data_agg, x='Month', y='Value', color='Metric',
                                        title=f"Budget vs Actual Comparison - {table_name}", height=400,
                                        markers=True, color_discrete_sequence=['#ff7f0e', '#2ca02c'])
                            fig.update_layout(xaxis_tickangle=0)
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(12, 6))
                            for metric in ['Budget', 'Act']:
                                metric_data = chart_data_agg[chart_data_agg['Metric'] == metric]
                                ax.plot(metric_data['Month'], metric_data['Value'], marker='o', 
                                       label=metric, color='#ff7f0e' if metric == 'Budget' else '#2ca02c')
                            ax.set_title(f"Budget vs Actual Comparison - {table_name}")
                            ax.set_xlabel("Month")
                            ax.set_ylabel("Value")
                            plt.xticks(rotation=0)
                            ax.legend()
                            plt.tight_layout()
                            st.pyplot(fig)
                
                    else:
                        st.markdown("Pie charts not suitable for Budget vs Actual comparison. Showing bar chart instead.")
                        try:
                            import plotly.express as px
                            fig = px.bar(chart_data_agg, x='Month', y='Value', color='Metric',
                                       barmode='group', title=f"Budget vs Actual Comparison - {table_name}",
                                       height=400, color_discrete_sequence=['#ff7f0e', '#2ca02c'])
                            fig.update_layout(xaxis_tickangle=0)
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(12, 6))
                            budget_data = chart_data_agg[chart_data_agg['Metric'] == 'Budget']
                            act_data = chart_data_agg[chart_data_agg['Metric'] == 'Act']
                            bar_width = 0.35
                            index = np.arange(len(budget_data))
                            ax.bar(index - bar_width/2, budget_data['Value'], bar_width, label='Budget', color='#ff7f0e')
                            ax.bar(index + bar_width/2, act_data['Value'], bar_width, label='Act', color='#2ca02c')
                            ax.set_xticks(index)
                            ax.set_xticklabels(budget_data['Month'], rotation=0)
                            ax.set_title(f"Budget vs Actual Comparison - {table_name}")
                            ax.set_xlabel("Month")
                            ax.set_ylabel("Value")
                            ax.legend()
                            plt.tight_layout()
                            st.pyplot(fig)
                
                    with st.expander("📈 View Comparison Data"):
                        st.dataframe(chart_data_melt, use_container_width=True)
                
                    ppt_type = 'bar' if visual_type == 'Bar Chart' else 'line' if visual_type == 'Line Chart' else 'pie'
                    ppt_bytes = create_ppt_with_chart(
                        title=f"Budget vs Actual - {table_name} - {selected_sheet}",
                        chart_data=chart_data_agg,
                        x_col="Month",
                        y_col="Value",
                        chart_type=ppt_type
                    )
                
                    st.download_button(
                        "⬇️ Download Budget vs Actual PPT",
                        ppt_bytes,
                        "budget_vs_actual.pptx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"download_budget_vs_actual_ppt_{selected_sheet}_{sheet_index}"
                    )
                    return chart_data_agg

            def display_visualization(tab, label, data, x_col, y_col, visual_type):
                with tab:
                    if data is None or data.empty:
                        st.warning(f"No data available for {label}")
                        return
                    
                    if not ensure_numeric_data(data, y_col):
                        st.warning(f"No numeric data available for {label}")
                        return None
                    
                    st.markdown(f"### {label} - {table_name}")
                    
                    if visual_type == "Bar Chart":
                        try:
                            import plotly.express as px
                            fig = px.bar(data, x=x_col, y=y_col, 
                                       title=f"{label} - {table_name}",
                                       height=500,
                                       color_discrete_sequence=['#2ca02c'])
                            fig.update_layout(xaxis_title=x_col, yaxis_title=y_col, xaxis_tickangle=0)
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(12, 6))
                            data.plot.bar(x=x_col, y=y_col, ax=ax, color='#2ca02c')
                            ax.set_title(f"{label} - {table_name}")
                            ax.set_xlabel(x_col)
                            ax.set_ylabel(y_col)
                            plt.xticks(rotation=0)
                            plt.tight_layout()
                            st.pyplot(fig)
                    
                    elif visual_type == "Line Chart":
                        try:
                            import plotly.express as px
                            fig = px.line(data, x=x_col, y=y_col, 
                                        title=f"{label} - {table_name}",
                                        height=400,
                                        markers=True,
                                        color_discrete_sequence=['#2ca02c'])
                            fig.update_layout(xaxis_title=x_col, yaxis_title=y_col, xaxis_tickangle=0)
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(12, 6))
                            data.plot.line(x=x_col, y=y_col, ax=ax, marker='o', color='#2ca02c')
                            ax.set_title(f"{label} - {table_name}")
                            ax.set_xlabel(x_col)
                            ax.set_ylabel(y_col)
                            plt.xticks(rotation=0)
                            plt.tight_layout()
                            st.pyplot(fig)
                    
                    elif visual_type == "Pie Chart":
                        try:
                            import plotly.express as px
                            pie_data = data[data[y_col] > 0]
                            if not pie_data.empty:
                                fig = px.pie(pie_data, values=y_col, names=x_col, 
                                           title=f"{label} Distribution - {table_name}",
                                           height=400)
                                fig.update_traces(textposition='inside', textinfo='percent+label')
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("No positive values to display in pie chart")
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(8, 6))
                            pie_data = data[data[y_col] > 0]
                            if not pie_data.empty:
                                pie_data.groupby(x_col)[y_col].sum().plot.pie(autopct='%1.1f%%', ax=ax)
                                ax.set_title(f"{label} Distribution - {table_name}")
                            else:
                                ax.text(0.5, 0.5, "No positive values to display", 
                                       ha='center', va='center')
                                ax.set_title(f"{label} (No positive data) - {table_name}")
                            st.pyplot(fig)
                    
                    with st.expander("📊 View Data Table"):
                        st.dataframe(data, use_container_width=True)
                    
                    ppt_type = 'bar' if visual_type == "Bar Chart" else 'line' if visual_type == "Line Chart" else 'pie'
                    ppt_bytes = create_ppt_with_chart(
                        f"{label} Analysis - {table_name} - {selected_sheet}",
                        data,
                        x_col,
                        y_col,
                        ppt_type
                    )
                    
                    st.download_button(
                        f"⬇️ Download {label} PPT",
                        ppt_bytes,
                        f"{label.lower().replace(' ', '_')}_analysis.pptx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"download_{label.lower().replace(' ', '_')}_ppt_{selected_sheet}_{sheet_index}"
                    )

            def plot_monthly_comparison(tab, label, visual_type):
                with tab:
                    normalized_label = label.replace(",", "")
                    plot_cols = [col for col in table_df.columns 
                               if str(col).lower().replace(",", "").startswith(normalized_label.lower()) 
                               and 'ytd' not in str(col).lower()
                               and column_filter(col)]
                
                    if not plot_cols:
                        st.info(f"No matching columns found for '{label}'")
                        return None
                
                    chart_data = filtered_df[[first_col] + plot_cols].copy()
                
                    for col in plot_cols:
                        chart_data[col] = pd.to_numeric(chart_data[col].astype(str).str.replace(',', ''), 
                                                       errors='coerce')
                
                    chart_data = chart_data.melt(id_vars=first_col, 
                                              var_name="Month", 
                                              value_name=label)
                    chart_data = chart_data.dropna()
                
                    chart_data[label] = pd.to_numeric(chart_data[label], errors='coerce')
                
                    if chart_data.empty or not ensure_numeric_data(chart_data, label):
                        st.warning(f"No valid numeric data available for '{label}' after conversion.")
                        return None
                
                    # Clean month names by removing Gr/Ach prefixes using the helper function
                    chart_data['Month'] = chart_data['Month'].apply(extract_month_year)
                
                    month_order = {'Apr': 1, 'May': 2, 'Jun': 3, 'Jul': 4, 'Aug': 5, 'Sep': 6,
                                   'Oct': 7, 'Nov': 8, 'Dec': 9, 'Jan': 10, 'Feb': 11, 'Mar': 12}
                
                    def get_sort_key(month_str):
                        month_match = re.search(r'(\w{3,})[-–](\d{2})', month_str, re.IGNORECASE)
                        if month_match:
                            month, year = month_match.groups()
                            month_idx = month_order.get(month.capitalize(), 99)
                            year_int = int(year)
                            if month_idx >= 10:
                                fiscal_year = year_int - 1
                            else:
                                fiscal_year = year_int
                            return (fiscal_year, month_idx)
                        return (0, 99)
                
                    chart_data = chart_data.sort_values(by='Month', key=lambda x: x.map(get_sort_key))
                    chart_data = make_jsonly_serializable(chart_data)
                
                    display_visualization(tab, f"{label} by Month", chart_data, "Month", label, visual_type)
                    return chart_data

            def plot_ytd_comparison(tab, pattern, label, visual_type):
                with tab:
                    ytd_cols = []
                    normalized_label = label.replace(",", "").lower()
                    
                    for col in table_df.columns:
                        col_str = str(col).lower().replace(",", "").replace("–", "-")
                        if normalized_label == 'gr':
                            if (re.search(r'gr-ytd[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\)', col_str, re.IGNORECASE) and
                                column_filter(col)):
                                ytd_cols.append(col)
                        elif normalized_label == 'ach':
                            if (re.search(r'ach-ytd[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\)', col_str, re.IGNORECASE) and
                                column_filter(col)):
                                ytd_cols.append(col)
                            elif (re.search(r'(ytd[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\).*ach|ach.*ytd[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\))', col_str, re.IGNORECASE) and
                                  column_filter(col)):
                                ytd_cols.append(col)
                        else:
                            if (re.search(r'ytd.*\b' + normalized_label + r'\b|' + normalized_label + r'.*ytd', col_str, re.IGNORECASE) or
                                re.search(r'ytd[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\)\s*' + normalized_label, col_str, re.IGNORECASE)) and \
                               column_filter(col):
                                ytd_cols.append(col)
                    
                    if not ytd_cols:
                        st.warning(f"No YTD {label} columns found. Expected columns like '{label}-YTD-25-26-(Apr to Jun)'.")
                        return None
                    
                    clean_labels = []
                    for col in ytd_cols:
                        col_str = str(col)
                        year_match = re.search(r'(\d{2,4})\s*[-–]\s*(\d{2,4})\s*\((.*?)\)', col_str, re.IGNORECASE)
                        if year_match:
                            start_year, end_year, month_range = year_match.groups()
                            start_year = start_year[-2:] if len(start_year) > 2 else start_year
                            end_year = end_year[-2:] if len(end_year) > 2 else end_year
                            fiscal_year = f"{start_year}-{end_year}"
                            month_range_clean = re.sub(r'\s*to\s*', ' - ', month_range, flags=re.IGNORECASE)
                            clean_label = f"{label}-{fiscal_year} ({month_range_clean})"
                        else:
                            fiscal_year = "Unknown"
                            month_range_clean = "Apr - Jun"
                            clean_label = f"{label}-{fiscal_year} ({month_range_clean})"
                            st.warning(f"Could not parse year or month range in column '{col}'. Using default '{clean_label}'.")
                        clean_labels.append(clean_label)
                    
                    month_order = {'Apr':1, 'May':2, 'Jun':3, 'Jul':4, 'Aug':5, 'Sep':6,
                                   'Oct':7, 'Nov':8, 'Dec':9, 'Jan':10, 'Feb':11, 'Mar':12}
                    
                    def get_sort_key(col_name):
                        month_match = re.search(r'\((\w{3})', col_name, re.IGNORECASE)
                        return month_order.get(month_match.group(1).capitalize(), 0) if month_match else 0
                    
                    sorted_cols = [first_col] + sorted(clean_labels, key=get_sort_key)
                    comparison_data = filtered_df[[first_col] + ytd_cols].copy()
                    comparison_data.columns = [first_col] + clean_labels
                    comparison_data = comparison_data[sorted_cols]
                    
                    for col in clean_labels:
                        comparison_data[col] = pd.to_numeric(comparison_data[col].astype(str).str.replace(',', ''), errors='coerce')
                    
                    chart_data = comparison_data.melt(id_vars=first_col, 
                                                     var_name="Period", 
                                                     value_name=label)
                    chart_data = chart_data.dropna()
                    
                    if not ensure_numeric_data(chart_data, label):
                        st.warning(f"No numeric data available for YTD {label} comparisons")
                        return None
                    
                    chart_data = make_jsonly_serializable(chart_data)
                    
                    st.markdown(f"### {label} YTD Comparisons - {table_name}")
                    
                    if visual_type == "Bar Chart":
                        try:
                            import plotly.express as px
                            fig = px.bar(chart_data, x="Period", y=label, 
                                         title=f"{label} YTD Comparisons - {table_name}",
                                         height=500, color_discrete_sequence=['#2ca02c'])
                            fig.update_layout(xaxis_title="Period", yaxis_title=label, xaxis_tickangle=0)
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(12, 6))
                            chart_data.plot.bar(x="Period", y=label, ax=ax, color='#2ca02c')
                            ax.set_title(f"{label} YTD Comparisons - {table_name}")
                            ax.set_xlabel("Period")
                            ax.set_ylabel(label)
                            plt.xticks(rotation=0)
                            plt.tight_layout()
                            st.pyplot(fig)
                    
                    elif visual_type == "Line Chart":
                        try:
                            import plotly.express as px
                            fig = px.line(chart_data, x="Period", y=label, 
                                          title=f"{label} YTD Comparisons - {table_name}",
                                          height=400, markers=True, color_discrete_sequence=['#2ca02c'])
                            fig.update_layout(xaxis_title="Period", yaxis_title=label, xaxis_tickangle=0)
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(12, 6))
                            chart_data.plot.line(x="Period", y=label, ax=ax, marker='o', color='#2ca02c')
                            ax.set_title(f"{label} YTD Comparisons - {table_name}")
                            ax.set_xlabel("Period")
                            ax.set_ylabel(label)
                            plt.xticks(rotation=0)
                            plt.tight_layout()
                            st.pyplot(fig)
                    
                    elif visual_type == "Pie Chart":
                        try:
                            import plotly.express as px
                            pie_data = chart_data[chart_data[label] > 0]
                            if not pie_data.empty:
                                fig = px.pie(pie_data, values=label, names="Period", 
                                             title=f"{label} YTD Distribution - {table_name}",
                                             height=400)
                                fig.update_traces(textposition='inside', textinfo='percent+label')
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("No positive values to display in pie chart")
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(8, 6))
                            pie_data = chart_data[chart_data[label] > 0]
                            if not pie_data.empty:
                                pie_data.groupby("Period")[label].sum().plot.pie(autopct='%1.1f%%', ax=ax)
                                ax.set_title(f"{label} YTD Distribution - {table_name}")
                            else:
                                ax.text(0.5, 0.5, "No positive values to display", 
                                        ha='center', va='center')
                                ax.set_title(f"{label} YTD (No positive data) - {table_name}")
                            st.pyplot(fig)
                    
                    with st.expander("📊 View Data Table"):
                        st.dataframe(chart_data, use_container_width=True)
                    
                    ppt_type = 'bar' if visual_type == "Bar Chart" else 'line' if visual_type == "Line Chart" else 'pie'
                    ppt_bytes = create_ppt_with_chart(
                        f"{label} YTD Analysis - {table_name} - {selected_sheet}",
                        chart_data,
                        "Period",
                        label,
                        ppt_type
                    )
                    
                    st.download_button(
                        f"⬇️ Download {label} YTD PPT",
                        ppt_bytes,
                        f"{label.lower().replace(' ', '_')}_ytd_analysis.pptx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"download_{label.lower().replace(' ', '_')}_ytd_ppt_{selected_sheet}_{sheet_index}"
                    )
                    return chart_data

            def plot_branch_performance(tab, visual_type):
                with tab:
                    if not is_branch_analysis:
                        st.info("This visualization is only available for region analysis sheets")
                        return
                
                    ytd_act_col = None
                    for col in table_df.columns:
                        col_str = str(col).strip()
                        if col_str == "Act-YTD-25-26 (Apr to Mar)" or \
                           re.search(r'YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\)\s*Act', col_str, re.IGNORECASE):
                            ytd_act_col = col
                            break
                
                    if ytd_act_col is None:
                        st.warning("Could not find YTD Act column for region performance analysis")
                        return
                
                    first_col = table_df.columns[0]
                    regions_df = table_df[~table_df[first_col].str.contains('|'.join(BRANCH_EXCLUDE_TERMS), na=False, case=False)].copy()
                    regions_df = regions_df.dropna(subset=[first_col, ytd_act_col])
                
                    if regions_df.empty:
                        st.warning("No branch data available after filtering")
                        return
                
                    regions_df[ytd_act_col] = pd.to_numeric(regions_df[ytd_act_col].astype(str).str.replace(',', ''), errors='coerce')
                    regions_df = regions_df.dropna(subset=[ytd_act_col])
                
                    if not ensure_numeric_data(regions_df, ytd_act_col):
                        st.warning("No numeric data available for region performance")
                        return
                
                    regions_df = regions_df.sort_values(by=ytd_act_col, ascending=False)
                    
                    st.markdown(f"### Branch Performance Analysis - {table_name}")
                    
                    if visual_type == "Bar Chart":
                        chart_data = regions_df.set_index(first_col)[ytd_act_col]
                        st.bar_chart(chart_data, height=500)
                        
                    elif visual_type == "Line Chart":
                        chart_data = regions_df.set_index(first_col)[ytd_act_col]
                        st.line_chart(chart_data, height=500)
                        
                    elif visual_type == "Pie Chart":
                        try:
                            import plotly.express as px
                            positive_regions = regions_df[regions_df[ytd_act_col] > 0]
                            if not positive_regions.empty:
                                fig = px.pie(positive_regions, values=ytd_act_col, names=first_col,
                                           title=f'Branch Performance Distribution by {ytd_act_col} - {table_name}',
                                           height=500)
                                fig.update_traces(textposition='inside', textinfo='percent+label')
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("No positive values to display in pie chart")
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(10, 8))
                            positive_regions = regions_df[regions_df[ytd_act_col] > 0]
                            if not positive_regions.empty:
                                ax.pie(positive_regions[ytd_act_col], 
                                      labels=positive_regions[first_col],
                                      autopct='%1.1f%%',
                                      startangle=90)
                                ax.set_title(f'Branch Performance by {ytd_act_col} - {table_name}')
                            else:
                                ax.text(0.5, 0.5, "No positive values", ha='center', va='center')
                            st.pyplot(fig)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        top_region = regions_df.iloc[0]
                        st.metric("Top Performer", top_region[first_col], f"{top_region[ytd_act_col]:,.0f}")
                    with col2:
                        total_performance = regions_df[ytd_act_col].sum()
                        st.metric("Total Performance", f"{total_performance:,.0f}")
                    with col3:
                        avg_performance = regions_df[ytd_act_col].mean()
                        st.metric("Average Performance", f"{avg_performance:,.0f}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("#### 🏆 Top 5 Regions")
                        top_5 = regions_df.head(5)[[first_col, ytd_act_col]]
                        st.dataframe(top_5, use_container_width=True, hide_index=True)
                    
                    with col2:
                        st.markdown("#### 📉 Bottom 5 Regions")
                        bottom_5 = regions_df.tail(5)[[first_col, ytd_act_col]]
                        st.dataframe(bottom_5, use_container_width=True, hide_index=True)
                    
                    with st.expander("📊 View All Region Data"):
                        st.dataframe(regions_df[[first_col, ytd_act_col]], use_container_width=True, hide_index=True)
                
                    regions_df = make_jsonly_serializable(regions_df)
                    ppt_type = 'bar' if visual_type == "Bar Chart" else 'line' if visual_type == "Line Chart" else 'pie'
                    ppt_bytes = create_ppt_with_chart(
                        f"Branch Performance - {table_name} - {selected_sheet}",
                        regions_df,
                        first_col,
                        ytd_act_col,
                        ppt_type
                    )
                
                    st.download_button(
                        "⬇️ Download Region Performance PPT",
                        ppt_bytes,
                        "region_performance.pptx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"download_region_performance_ppt_{selected_sheet}_{sheet_index}"
                    )

            def plot_branch_monthwise(tab, visual_type):
                with tab:
                    if not is_branch_analysis:
                        st.info("This visualization is only available for region analysis sheets")
                        return
                
                    regions_df = filtered_df[~filtered_df[first_col].str.contains('|'.join(BRANCH_EXCLUDE_TERMS), na=False, case=False)].copy()
                
                    act_cols = []
                    for col in table_df.columns:
                        col_str = str(col).lower()
                        for year in years:
                            if (re.search(rf'\bact\b.*(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)[-\s]*{year}', col_str, re.IGNORECASE) and 'ytd' not in col_str):
                                act_cols.append(col)
                
                    if not act_cols:
                        st.warning(f"Could not find monthly Act columns for the selected years ({', '.join(years)})")
                        return
                
                    month_order = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
                
                    def get_sort_key(col_name):
                        col_name = str(col_name).lower()
                        month_match = re.search(r'\b(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)\b', col_name, re.IGNORECASE)
                        year_match = re.search(r'[-–](\d{2})\b', col_name)
                        month_idx = month_order.index(month_match.group(1).capitalize()) if month_match and month_match.group(1).capitalize() in month_order else 99
                        year = int(year_match.group(1)) if year_match else 0
                        return (year, month_idx)
                
                    act_cols_sorted = sorted(act_cols, key=get_sort_key)
                
                    monthwise_data = regions_df[[first_col] + act_cols_sorted].copy()
                
                    clean_col_names = []
                    for col in act_cols_sorted:
                        month_match = re.search(r'\b(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)\b', str(col), re.IGNORECASE)
                        year_match = re.search(r'[-–](\d{2})\b', str(col))
                        if month_match and year_match:
                            clean_col_names.append(f"{month_match.group(1).capitalize()}-{year_match.group(1)}")
                        else:
                            clean_col_names.append(str(col))
                
                    monthwise_data.columns = [first_col] + clean_col_names
                
                    for col in clean_col_names:
                        monthwise_data[col] = pd.to_numeric(monthwise_data[col].astype(str).str.replace(',', ''), errors='coerce')
                    
                    monthwise_data = monthwise_data.dropna()
                    
                    if monthwise_data.empty:
                        st.warning("No numeric data available for region monthwise performance after filtering")
                        return
                
                    st.markdown(f"### Branch Monthwise Performance ({', '.join(selected_years if selected_years else years)})")
                
                    if visual_type == "Bar Chart":
                        chart_data = monthwise_data.set_index(first_col)
                        st.bar_chart(chart_data, height=500)
                        
                    elif visual_type == "Line Chart":
                        chart_data = monthwise_data.set_index(first_col)
                        st.line_chart(chart_data, height=500)
                        
                    else:
                        st.info("Pie charts not ideal for time series. Showing bar chart instead.")
                        chart_data = monthwise_data.set_index(first_col)
                        st.bar_chart(chart_data, height=500)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        total_by_month = monthwise_data[clean_col_names].sum()
                        best_month = total_by_month.idxmax()
                        st.metric("Best Month", best_month, f"{total_by_month[best_month]:,.0f}")
                    with col2:
                        avg_monthly = total_by_month.mean()
                        st.metric("Monthly Average", f"{avg_monthly:,.0f}")
                    with col3:
                        total_performance = total_by_month.sum()
                        st.metric("Total Performance", f"{total_performance:,.0f}")
                    
                    with st.expander("📊 View Detailed Monthly Data"):
                        st.dataframe(monthwise_data, use_container_width=True, hide_index=True)
                
                    chart_data = monthwise_data.melt(id_vars=first_col, 
                                                  var_name="Month", 
                                                  value_name="Value")
                    chart_data = make_jsonly_serializable(chart_data)
                
                    ppt_type = 'bar' if visual_type == "Bar Chart" else 'line'
                    ppt_bytes = create_ppt_with_chart(
                        f"Region Monthwise Performance - {selected_sheet}",
                        chart_data,
                        "Month",
                        "Value",
                        ppt_type
                    )
                
                    st.download_button(
                        "⬇️ Download Region Monthwise PPT",
                        ppt_bytes,
                        "region_monthwise.pptx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"download_region_monthwise_ppt_{selected_sheet}_{sheet_index}"
                    )

            def plot_product_performance(tab, visual_type):
                with tab:
                    if not is_product_analysis:
                        st.info("This visualization is only available for product analysis sheets")
                        return
                
                    ytd_act_col = None
                    for col in table_df.columns:
                        col_str = str(col).strip()
                        if col_str == "Act-YTD-25-26 (Apr to Mar)" or \
                           re.search(r'YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\)\s*Act', col_str, re.IGNORECASE):
                            ytd_act_col = col
                            break
                
                    if ytd_act_col is None:
                        st.warning("Could not find YTD Act column for product performance analysis")
                        return
                
                    first_col = table_df.columns[0]
                    exclude_terms = ['Total', 'TOTAL', 'Grand', 'GRAND', 'Total Sales']
                    products_df = table_df[~table_df[first_col].str.contains('|'.join(exclude_terms), na=False, case=False)].copy()
                    products_df = products_df.dropna(subset=[first_col, ytd_act_col])
                
                    if products_df.empty:
                        st.warning("No product data available after filtering")
                        return
                
                    products_df[ytd_act_col] = pd.to_numeric(products_df[ytd_act_col].astype(str).str.replace(',', ''), errors='coerce')
                    products_df = products_df.dropna(subset=[ytd_act_col])
                
                    if not ensure_numeric_data(products_df, ytd_act_col):
                        st.warning("No numeric data available for product performance")
                        return
                
                    products_df = products_df.sort_values(by=ytd_act_col, ascending=False)
                    
                    st.markdown("### Product Performance Analysis")
                    
                    if visual_type == "Bar Chart":
                        chart_data = products_df.set_index(first_col)[ytd_act_col]
                        st.bar_chart(chart_data, height=500)
                        
                    elif visual_type == "Line Chart":
                        chart_data = products_df.set_index(first_col)[ytd_act_col]
                        st.line_chart(chart_data, height=500)
                        
                    elif visual_type == "Pie Chart":
                        try:
                            import plotly.express as px
                            positive_products = products_df[products_df[ytd_act_col] > 0]
                            if not positive_products.empty:
                                fig = px.pie(positive_products, values=ytd_act_col, names=first_col,
                                           title=f'Product Performance Distribution by {ytd_act_col}',
                                           height=500)
                                fig.update_traces(textposition='inside', textinfo='percent+label')
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("No positive values to display in pie chart")
                        except ImportError:
                            fig, ax = plt.subplots(figsize=(10, 8))
                            positive_products = products_df[products_df[ytd_act_col] > 0]
                            if not positive_products.empty:
                                ax.pie(positive_products[ytd_act_col], 
                                      labels=positive_products[first_col],
                                      autopct='%1.1f%%',
                                      startangle=90)
                                ax.set_title(f'Product Performance by {ytd_act_col}')
                            else:
                                ax.text(0.5, 0.5, "No positive values", ha='center', va='center')
                            st.pyplot(fig)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        top_product = products_df.iloc[0]
                        st.metric("Top Performer", top_product[first_col], f"{top_product[ytd_act_col]:,.0f}")
                    with col2:
                        total_performance = products_df[ytd_act_col].sum()
                        st.metric("Total Performance", f"{total_performance:,.0f}")
                    with col3:
                        avg_performance = products_df[ytd_act_col].mean()
                        st.metric("Average Performance", f"{avg_performance:,.0f}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("#### 🏆 Top 5 Products")
                        top_5 = products_df.head(5)[[first_col, ytd_act_col]]
                        st.dataframe(top_5, use_container_width=True, hide_index=True)
                    
                    with col2:
                        st.markdown("#### 📉 Bottom 5 Products")
                        bottom_5 = products_df.tail(5)[[first_col, ytd_act_col]]
                        st.dataframe(bottom_5, use_container_width=True, hide_index=True)
                    
                    with st.expander("📊 View All Product Data"):
                        st.dataframe(products_df[[first_col, ytd_act_col]], use_container_width=True, hide_index=True)
                
                    products_df = make_jsonly_serializable(products_df)
                    ppt_type = 'bar' if visual_type == "Bar Chart" else 'line' if visual_type == "Line Chart" else 'pie'
                    ppt_bytes = create_ppt_with_chart(
                        f"Product Performance - {selected_sheet}",
                        products_df,
                        first_col,
                        ytd_act_col,
                        ppt_type
                    )
                
                    st.download_button(
                        "⬇️ Download Product Performance PPT",
                        ppt_bytes,
                        "product_performance.pptx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"download_product_performance_ppt_{selected_sheet}_{sheet_index}"
                    )
            

            def plot_product_monthwise(tab, visual_type):
                with tab:
                    if not is_product_analysis:
                        st.info("This visualization is only available for product analysis sheets")
                        return
                
                    act_cols = []
                    for col in table_df.columns:
                        col_str = str(col).lower()
                        for year in years:
                            if (re.search(rf'\bact\b.*(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)[-\s]*{year}', col_str, re.IGNORECASE) and 'ytd' not in col_str):
                                act_cols.append(col)
                
                    if not act_cols:
                        st.warning(f"Could not find monthly Act columns for the selected years ({', '.join(years)})")
                        return
                
                    month_order = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
                
                    def get_sort_key(col_name):
                        col_name = str(col_name).lower()
                        month_match = re.search(r'\b(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)\b', col_name, re.IGNORECASE)
                        year_match = re.search(r'[-–](\d{2})\b', col_name)
                        month_idx = month_order.index(month_match.group(1).capitalize()) if month_match and month_match.group(1).capitalize() in month_order else 99
                        year = int(year_match.group(1)) if year_match else 0
                        return (year, month_idx)
                
                    act_cols_sorted = sorted(act_cols, key=get_sort_key)
                
                    first_col = table_df.columns[0]
                    exclude_terms = ['Total', 'TOTAL', 'Grand', 'GRAND', 'Total Sales']
                    products_df = table_df[~table_df[first_col].str.contains('|'.join(exclude_terms), na=False, case=False)].copy()
                    monthwise_data = products_df[[first_col] + act_cols_sorted].copy()
                    
                    clean_col_names = []
                    for col in act_cols_sorted:
                        month_match = re.search(r'\b(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)\b', str(col), re.IGNORECASE)
                        year_match = re.search(r'[-–](\d{2})\b', str(col))
                        if month_match and year_match:
                            clean_col_names.append(f"{month_match.group(1).capitalize()}-{year_match.group(1)}")
                        else:
                            clean_col_names.append(str(col))
                    
                    monthwise_data.columns = [first_col] + clean_col_names
                    
                    for col in clean_col_names:
                        monthwise_data[col] = pd.to_numeric(monthwise_data[col].astype(str).str.replace(',', ''), errors='coerce')
                    
                    monthwise_data = monthwise_data.dropna()
                    
                    if monthwise_data.empty:
                        st.warning("No numeric data available for product monthwise performance after filtering")
                        return
                
                    st.write(f"### Product Monthwise Performance ({', '.join(selected_years if selected_years else years)})")
                
                    if visual_type == "Bar Chart":
                        chart_data = monthwise_data.set_index(first_col)
                        st.bar_chart(chart_data, height=500)
                        
                    elif visual_type == "Line Chart":
                        chart_data = monthwise_data.set_index(first_col)
                        st.line_chart(chart_data                        , height=500)
                    else:
                        st.info("Pie charts not ideal for time series. Showing bar chart instead.")
                        chart_data = monthwise_data.set_index(first_col)
                        st.bar_chart(chart_data, height=500)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        total_by_month = monthwise_data[clean_col_names].sum()
                        best_month = total_by_month.idxmax()
                        st.metric("Best Month", best_month, f"{total_by_month[best_month]:,.0f}")
                    with col2:
                        avg_monthly = total_by_month.mean()
                        st.metric("Monthly Average", f"{avg_monthly:,.0f}")
                    with col3:
                        total_performance = total_by_month.sum()
                        st.metric("Total Performance", f"{total_performance:,.0f}")
                    
                    with st.expander("📊 View Detailed Monthly Data"):
                        st.dataframe(monthwise_data, use_container_width=True, hide_index=True)
                
                    chart_data = monthwise_data.melt(id_vars=first_col, 
                                                  var_name="Month", 
                                                  value_name="Value")
                    chart_data = make_jsonly_serializable(chart_data)
                
                    ppt_type = 'bar' if visual_type == "Bar Chart" else 'line'
                    ppt_bytes = create_ppt_with_chart(
                        f"Product Monthwise Performance - {selected_sheet}",
                        chart_data,
                        "Month",
                        "Value",
                        ppt_type
                    )
                
                    st.download_button(
                        "⬇️ Download Product Monthwise PPT",
                        ppt_bytes,
                        "product_monthwise.pptx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"download_product_monthwise_ppt_{selected_sheet}_{sheet_index}"
                    )

            # Plot visualizations in respective tabs
            budget_vs_actual_data = plot_budget_vs_actual(tabs_dict["Budget vs Actual"], visual_type)
            budget_data = plot_monthly_comparison(tabs_dict["Budget"], "Budget", visual_type)
            ly_data = plot_monthly_comparison(tabs_dict["LY"], "LY", visual_type)
            act_data = plot_monthly_comparison(tabs_dict["Act"], "Act", visual_type)
            gr_data = plot_monthly_comparison(tabs_dict["Gr"], "Gr", visual_type)
            ach_data = plot_monthly_comparison(tabs_dict["Ach"], "Ach", visual_type)
            ytd_budget_data = plot_ytd_comparison(tabs_dict["YTD Budget"], r'\bBudget\b.*YTD', "Budget", visual_type)
            ytd_ly_data = plot_ytd_comparison(tabs_dict["YTD LY"], r'\bLY\b.*YTD', "LY", visual_type)
            ytd_act_data = plot_ytd_comparison(tabs_dict["YTD Act"], r'\bAct\b.*YTD', "Act", visual_type)
            ytd_gr_data = plot_ytd_comparison(tabs_dict["YTD Gr"], r'\bGr\b.*YTD', "Gr", visual_type)
            ytd_ach_data = plot_ytd_comparison(tabs_dict["YTD Ach"], r'\bAch\b.*YTD', "Ach", visual_type)
            plot_branch_performance(tabs_dict["Branch Performance"], visual_type)
            plot_branch_monthwise(tabs_dict["Branch Monthwise"], visual_type)
            plot_product_performance(tabs_dict["Product Performance"], visual_type)
            plot_product_monthwise(tabs_dict["Product Monthwise"], visual_type)

            # Generate master PPT for all visualizations
            all_data = [
                ("Budget vs Actual", budget_vs_actual_data),
                ("Budget", budget_data),
                ("LY", ly_data),
                ("Act", act_data),
                ("Gr", gr_data),
                ("Ach", ach_data),
                ("YTD Budget", ytd_budget_data),
                ("YTD LY", ytd_ly_data),
                ("YTD Act", ytd_act_data),
                ("YTD Gr", ytd_gr_data),
                ("YTD Ach", ytd_ach_data),
                ("Branch Performance", None),  # Will be populated below
                ("Branch Monthwise", None),   # Will be populated below
                ("Product Performance", None),  # Will be populated below
                ("Product Monthwise", None)    # Will be populated below
            ]

            # Prepare data for Region Performance
            if is_branch_analysis:
                ytd_act_col = None
                for col in table_df.columns:
                    col_str = str(col).strip()
                    if col_str == "Act-YTD-25-26 (Apr to Mar)" or \
                       re.search(r'YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\)\s*Act', col_str, re.IGNORECASE):
                        ytd_act_col = col
                        break
                if ytd_act_col:
                    regions_df = table_df[~table_df[first_col].str.contains('|'.join(BRANCH_EXCLUDE_TERMS), na=False, case=False)].copy()
                    regions_df = regions_df.dropna(subset=[first_col, ytd_act_col])
                    regions_df[ytd_act_col] = pd.to_numeric(regions_df[ytd_act_col].astype(str).str.replace(',', ''), errors='coerce')
                    regions_df = regions_df.dropna(subset=[ytd_act_col])
                    regions_df = regions_df.sort_values(by=ytd_act_col, ascending=False)
                    regions_df = make_jsonly_serializable(regions_df)
                    all_data[11] = ("Branch Performance", regions_df[[first_col, ytd_act_col]])

            # Prepare data for Region Monthwise
            if is_branch_analysis:
                act_cols = []
                for col in table_df.columns:
                    col_str = str(col).lower()
                    for year in years:
                        # Only match Act columns that contain month-year, not Gr or Ach
                        if (re.search(rf'\bact\b.*(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)[-\s]*{year}', col_str, re.IGNORECASE) 
                            and 'ytd' not in col_str 
                            and not re.search(r'\b(gr|ach)\b', col_str, re.IGNORECASE)):
                            act_cols.append(col)
                if act_cols:
                    month_order = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
                    def get_sort_key(col_name):
                        col_name = str(col_name).lower()
                        month_match = re.search(r'\b(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)\b', col_name, re.IGNORECASE)
                        year_match = re.search(r'[-–](\d{2})\b', col_name)
                        month_idx = month_order.index(month_match.group(1).capitalize()) if month_match and month_match.group(1).capitalize() in month_order else 99
                        year = int(year_match.group(1)) if year_match else 0
                        return (year, month_idx)
                    act_cols_sorted = sorted(act_cols, key=get_sort_key)
                    regions_df = filtered_df[~filtered_df[first_col].str.contains('|'.join(BRANCH_EXCLUDE_TERMS), na=False, case=False)].copy()
                    monthwise_data = regions_df[[first_col] + act_cols_sorted].copy()
                    clean_col_names=[]
                    for col in act_cols_sorted:
                        clean_col_names.append(extract_month_year(col))
                    monthwise_data.columns = [first_col] + clean_col_names
                    for col in clean_col_names:
                        monthwise_data[col] = pd.to_numeric(monthwise_data[col].astype(str).str.replace(',', ''), errors='coerce')
                    monthwise_data = monthwise_data.dropna()
                    chart_data = monthwise_data.melt(id_vars=first_col, var_name="Month", value_name="Value")
                    chart_data = make_jsonly_serializable(chart_data)
                    all_data[12] = ("Branch Monthwise", chart_data)

            # Prepare data for Product Performance
            if is_product_analysis:
                ytd_act_col = None
                for col in table_df.columns:
                    col_str = str(col).strip()
                    if col_str == "Act-YTD-25-26 (Apr to Mar)" or \
                       re.search(r'YTD[-–\s]*\d{2}[-–\s]*\d{2}\s*\([^)]*\)\s*Act', col_str, re.IGNORECASE):
                        ytd_act_col = col
                        break
                if ytd_act_col:
                    exclude_terms = ['Total', 'TOTAL', 'Grand', 'GRAND', 'Total Sales']
                    products_df = table_df[~table_df[first_col].str.contains('|'.join(exclude_terms), na=False, case=False)].copy()
                    products_df = products_df.dropna(subset=[first_col, ytd_act_col])
                    products_df[ytd_act_col] = pd.to_numeric(products_df[ytd_act_col].astype(str).str.replace(',', ''), errors='coerce')
                    products_df = products_df.dropna(subset=[ytd_act_col])
                    products_df = products_df.sort_values(by=ytd_act_col, ascending=False)
                    products_df = make_jsonly_serializable(products_df)
                    all_data[13] = ("Product Performance", products_df[[first_col, ytd_act_col]])

            # Prepare data for Product Monthwise
            if is_product_analysis:
                act_cols = []
                for col in table_df.columns:
                    col_str = str(col).lower()
                    for year in years:
                        # Only match Act columns that contain month-year, not Gr or Ach
                        if (re.search(rf'\bact\b.*(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)[-\s]*{year}', col_str, re.IGNORECASE) 
                            and 'ytd' not in col_str 
                            and not re.search(r'\b(gr|ach)\b', col_str, re.IGNORECASE)):
                            act_cols.append(col)
                if act_cols:
                    month_order = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
                    def get_sort_key(col_name):
                        col_name = str(col_name).lower()
                        month_match = re.search(r'\b(apr|may|jun|jul|aug|sep|oct|nov|dec|jan|feb|mar)\b', col_name, re.IGNORECASE)
                        year_match = re.search(r'[-–](\d{2})\b', col_name)
                        month_idx = month_order.index(month_match.group(1).capitalize()) if month_match and month_match.group(1).capitalize() in month_order else 99
                        year = int(year_match.group(1)) if year_match else 0
                        return (year, month_idx)
                    act_cols_sorted = sorted(act_cols, key=get_sort_key)
                    exclude_terms = ['Total', 'TOTAL', 'Grand', 'GRAND', 'Total Sales']
                    products_df = table_df[~table_df[first_col].str.contains('|'.join(exclude_terms), na=False, case=False)].copy()
                    monthwise_data = products_df[[first_col] + act_cols_sorted].copy()
                    clean_col_names = []
                    for col in act_cols_sorted:
                        clean_col_names.append(extract_month_year(col))
                    monthwise_data.columns = [first_col] + clean_col_names
                    for col in clean_col_names:
                        monthwise_data[col] = pd.to_numeric(monthwise_data[col].astype(str).str.replace(',', ''), errors='coerce')
                    monthwise_data = monthwise_data.dropna()
                    chart_data = monthwise_data.melt(id_vars=first_col, var_name="Month", value_name="Value")
                    chart_data = make_jsonly_serializable(chart_data)
                    all_data[14] = ("Product Monthwise", chart_data)

            # Master PPT generation with fixed column handling
            if any(data is not None for _, data in all_data):
                st.sidebar.markdown("---")
                st.sidebar.subheader("📊 Download All Visuals")
                
                master_ppt = Presentation()
                
                for label, data in all_data:
                    if data is not None and (not isinstance(data, pd.DataFrame) or not data.empty):
                        try:
                            chart_data, x_col, y_col = get_chart_data_for_ppt(data, label, first_col, visual_type)
                            
                            if chart_data is None or x_col is None or y_col is None:
                                continue
                                
                            slide = master_ppt.slides.add_slide(master_ppt.slide_layouts[5])
                            
                            fig, ax = plt.subplots(figsize=(10, 6))
                            
                            if label == "Budget vs Actual":
                                budget_data_ppt = chart_data[chart_data['Metric'] == 'Budget']
                                act_data_ppt = chart_data[chart_data['Metric'] == 'Act']
                                if not budget_data_ppt.empty and not act_data_ppt.empty:
                                    bar_width = 0.35
                                    index = np.arange(len(budget_data_ppt))
                                    ax.bar(index - bar_width/2, budget_data_ppt[y_col], bar_width, label='Budget', color='#ff7f0e')
                                    ax.bar(index + bar_width/2, act_data_ppt[y_col], bar_width, label='Act', color='#2ca02c')
                                    ax.set_xticks(index)
                                    ax.set_xticklabels(budget_data_ppt[x_col], rotation=0, ha='center')
                                    ax.set_ylabel('Value')
                                    ax.set_title(f"{label} Analysis - {table_name} - {selected_sheet}")
                                    ax.legend()
                                else:
                                    ax.text(0.5, 0.5, "Insufficient data for comparison", ha='center', va='center')
                                    ax.set_title(f"{label} - No Data - {table_name}")
                                    
                            elif label in ["Branch Performance", "Product Performance"]:
                                if visual_type == "Pie Chart":
                                    positive_data = chart_data[chart_data[y_col] > 0]
                                    if not positive_data.empty:
                                        ax.pie(positive_data[y_col], labels=positive_data[x_col], autopct='%1.1f%%', startangle=90)
                                        ax.set_title(f"{label} Analysis - {table_name} - {selected_sheet}")
                                    else:
                                        ax.text(0.5, 0.5, "No positive values", ha='center', va='center')
                                        ax.set_title(f"{label} - No Data - {table_name}")
                                else:
                                    chart_data.plot.bar(x=x_col, y=y_col, ax=ax, color='#2ca02c')
                                    ax.set_title(f"{label} Analysis - {table_name} - {selected_sheet}")
                                    ax.set_xlabel(x_col)
                                    ax.set_ylabel(y_col)
                                    plt.xticks(rotation=0, ha='center')
                                    
                            elif label in ["Branch Monthwise", "Product Monthwise"]:
                                if visual_type == "Line Chart":
                                    data_pivot = chart_data.pivot_table(index=x_col, values=y_col, aggfunc='sum').reset_index()
                                    ax.plot(data_pivot[x_col], data_pivot[y_col], marker='o', color='#2ca02c')
                                    ax.set_title(f"{label} Analysis - {table_name} - {selected_sheet}")
                                    ax.set_xlabel(x_col)
                                    ax.set_ylabel(y_col)
                                    plt.xticks(rotation=0, ha='center')
                                else:
                                    data_pivot = chart_data.pivot_table(index=x_col, values=y_col, aggfunc='sum').reset_index()
                                    data_pivot.plot.bar(x=x_col, y=y_col, ax=ax, color='#2ca02c')
                                    ax.set_title(f"{label} Analysis - {table_name} - {selected_sheet}")
                                    ax.set_xlabel(x_col)
                                    ax.set_ylabel(y_col)
                                    plt.xticks(rotation=0, ha='center')
                            else:
                                # Handle monthly and YTD data
                                if visual_type == "Line Chart":
                                    chart_data.plot.line(x=x_col, y=y_col, ax=ax, marker='o', color='#2ca02c')
                                else:
                                    chart_data.plot.bar(x=x_col, y=y_col, ax=ax, color='#2ca02c')
                                ax.set_title(f"{label} Analysis - {table_name} - {selected_sheet}")
                                ax.set_xlabel(x_col)
                                ax.set_ylabel(y_col)
                                plt.xticks(rotation=0, ha='center')
                            
                            plt.tight_layout()
                            
                            img_buffer = BytesIO()
                            fig.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
                            plt.close()
                            img_buffer.seek(0)
                            
                            txBox = slide.shapes.add_textbox(Inches(1), Inches(0.5), Inches(8), Inches(1))
                            tf = txBox.text_frame
                            tf.text = f"{label} Analysis - {table_name} - {selected_sheet}"
                            
                            slide.shapes.add_picture(img_buffer, Inches(1), Inches(1.5), width=Inches(8))
                            
                        except Exception as e:
                            st.warning(f"Error creating chart for {label}: {e}")
                            continue
                
                master_ppt_bytes = BytesIO()
                master_ppt.save(master_ppt_bytes)
                master_ppt_bytes.seek(0)
                
                st.sidebar.download_button(
                    "⬇️ Download All Charts as PPT",
                    master_ppt_bytes,
                    "all_visuals.pptx",
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                    key=f"download_all_charts_ppt_{selected_sheet}_{sheet_index}"
                )

else:
    st.info("Please upload an Excel file to begin analysis.")
