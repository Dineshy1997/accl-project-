import streamlit as st
import pandas as pd
import re
import numpy as np
from datetime import datetime
from io import BytesIO
import warnings
from fuzzywuzzy import process
import traceback

# Configuration
warnings.filterwarnings('ignore')
pd.set_option("styler.render.max_elements", 500000)
st.set_page_config(layout='wide')

# Memory-efficient merge functions
def safe_merge_dataframes(left_df, right_df, on_column, how='left', max_rows_threshold=100000):
    """
    Safely merge dataframes with memory checks and deduplication to prevent memory explosion.
    
    Args:
        left_df: Left dataframe to merge
        right_df: Right dataframe to merge  
        on_column: Column to merge on
        how: Type of merge ('left', 'right', 'inner', 'outer')
        max_rows_threshold: Maximum expected rows after merge
    
    Returns:
        Merged dataframe or raises informative error
    """
    if left_df is None or right_df is None:
        st.error("One of the dataframes is None")
        return left_df if right_df is None else right_df
    
    if left_df.empty or right_df.empty:
        st.warning("One of the dataframes is empty")
        return left_df if right_df.empty else right_df
    
    if on_column not in left_df.columns:
        st.error(f"Column '{on_column}' not found in left dataframe")
        return left_df
    
    if on_column not in right_df.columns:
        st.error(f"Column '{on_column}' not found in right dataframe")
        return left_df

    
    
    # Clean and deduplicate the merge column
    left_df = left_df.copy()
    right_df = right_df.copy()
    
    # Ensure merge column is string and cleaned
    left_df[on_column] = left_df[on_column].astype(str).str.strip().str.upper()
    right_df[on_column] = right_df[on_column].astype(str).str.strip().str.upper()
    
    # Remove empty/invalid keys
    left_df = left_df[~left_df[on_column].isin(['', 'NAN', 'NONE', 'NULL'])]
    right_df = right_df[~right_df[on_column].isin(['', 'NAN', 'NONE', 'NULL'])]
    
    # Check for duplicates in merge column and warn
    left_dups = left_df[on_column].duplicated().sum()
    right_dups = right_df[on_column].duplicated().sum()
    
    if left_dups > 0:
        
        # Keep only the first occurrence to prevent explosion
        left_df = left_df.drop_duplicates(subset=[on_column], keep='first')
    
    
    if right_dups > 0:
        
        # For right dataframe, aggregate numeric columns by sum to prevent loss
        numeric_cols = right_df.select_dtypes(include=[np.number]).columns
        non_numeric_cols = [col for col in right_df.columns if col not in numeric_cols and col != on_column]
        
        if len(numeric_cols) > 0:
            # Aggregate numeric columns by sum, keep first value for non-numeric
            agg_dict = {col: 'sum' for col in numeric_cols}
            if non_numeric_cols:
                agg_dict.update({col: 'first' for col in non_numeric_cols})
            
            right_df = right_df.groupby(on_column).agg(agg_dict).reset_index()
            
        else:
            right_df = right_df.drop_duplicates(subset=[on_column], keep='first')
            
    
    # Estimate merge result size
    estimated_rows = len(left_df) * right_df.groupby(on_column).size().max() if how == 'left' else len(left_df) + len(right_df)

    estimated_cols = len(left_df.columns) + len(right_df.columns) - 1  # -1 for merge column overlap
    
    
    
    if estimated_rows > max_rows_threshold:
        st.error(f"Estimated merge size ({estimated_rows} rows) exceeds threshold ({max_rows_threshold}). Aborting to prevent memory issues.")
        return left_df
    
    # Perform the merge with error handling
    try:
        # Use categorical data type for merge column to save memory
        if left_df[on_column].nunique() < len(left_df) * 0.5:  # If less than 50% unique values
            left_df[on_column] = left_df[on_column].astype('category')
            right_df[on_column] = right_df[on_column].astype('category')
        
        merged_df = left_df.merge(right_df, on=on_column, how=how, suffixes=('', '_right'))
        
        # Remove duplicate columns that may have been created
        duplicate_cols = [col for col in merged_df.columns if col.endswith('_right')]
        if duplicate_cols:
        
            merged_df = merged_df.drop(columns=duplicate_cols)
        
    
        return merged_df
        
    except MemoryError as e:
        st.error(f"Memory error during merge: {str(e)}")
        st.info("Attempting chunk-based merge...")
        
        # Fallback: chunk-based merge for large datasets
        return chunk_based_merge(left_df, right_df, on_column, how)
        
    except Exception as e:
        st.error(f"Error during merge: {str(e)}")
        return left_df

def chunk_based_merge(left_df, right_df, on_column, how='left', chunk_size=10000):
    """
    Perform merge in chunks to handle large datasets with limited memory.
    """
    try:
        st.info(f"Performing chunk-based merge with chunk size: {chunk_size}")
        
        merged_chunks = []
        total_chunks = (len(left_df) // chunk_size) + 1
        
        for i in range(0, len(left_df), chunk_size):
            chunk = left_df.iloc[i:i+chunk_size]
            merged_chunk = chunk.merge(right_df, on=on_column, how=how, suffixes=('', '_right'))
            
            # Remove duplicate columns
            duplicate_cols = [col for col in merged_chunk.columns if col.endswith('_right')]
            if duplicate_cols:
                merged_chunk = merged_chunk.drop(columns=duplicate_cols)
            
            merged_chunks.append(merged_chunk)
            
            if (i // chunk_size + 1) % 5 == 0:  # Progress update every 5 chunks
                st.info(f"Processed chunk {i//chunk_size + 1}/{total_chunks}")
        
        # Combine all chunks
        final_merged = pd.concat(merged_chunks, ignore_index=True)
        st.success(f"Chunk-based merge completed! Final shape: {final_merged.shape}")
        return final_merged
        
    except Exception as e:
        st.error(f"Chunk-based merge also failed: {str(e)}")
        st.info("Returning left dataframe as fallback.")
        return left_df

def optimize_dataframe_memory(df):
    """
    Optimize dataframe memory usage by converting data types.
    """
    if df is None or df.empty:
        return df
    
    original_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
    
    # Optimize numeric columns
    for col in df.select_dtypes(include=['int64']).columns:
        if df[col].min() >= -32768 and df[col].max() <= 32767:
            df[col] = df[col].astype('int16')
        elif df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
            df[col] = df[col].astype('int32')
    
    for col in df.select_dtypes(include=['float64']).columns:
        if df[col].min() >= -3.4e38 and df[col].max() <= 3.4e38:
            df[col] = df[col].astype('float32')
    
    # Optimize string columns with low cardinality
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
            df[col] = df[col].astype('category')
    
    new_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
    
    if original_memory > 0:
        memory_reduction = (1 - new_memory/original_memory) * 100
        st.info(f"Memory optimization: {original_memory:.1f}MB → {new_memory:.1f}MB ({memory_reduction:.1f}% reduction)")
    
    return df

# Initialize session state
if 'uploaded_file_auditor' not in st.session_state:
    st.session_state.uploaded_file_auditor = None
if 'uploaded_file_sales' not in st.session_state:
    st.session_state.uploaded_file_sales = None
if 'uploaded_file_budget' not in st.session_state:
    st.session_state.uploaded_file_budget = None
if 'uploaded_file_last_year' not in st.session_state:
    st.session_state.uploaded_file_last_year = None
if 'region_analysis_data' not in st.session_state:
    st.session_state.region_analysis_data = None
if 'region_value_data' not in st.session_state:
    st.session_state.region_value_data = None
if 'budget_data' not in st.session_state:
    st.session_state.budget_data = None
if 'last_year_data' not in st.session_state:
    st.session_state.last_year_data = None
if 'auditor_mt_table' not in st.session_state:
    st.session_state.auditor_mt_table = None
if 'auditor_value_table' not in st.session_state:
    st.session_state.auditor_value_table = None
if 'product_analysis_data' not in st.session_state:
    st.session_state.product_analysis_data = None
if 'product_mt_data' not in st.session_state:
    st.session_state.product_mt_data = None
if 'product_value_data' not in st.session_state:
    st.session_state.product_value_data = None
if 'product_budget_data' not in st.session_state:
    st.session_state.product_budget_data = None
if 'auditor_product_mt_table' not in st.session_state:
    st.session_state.auditor_product_mt_table = None
if 'auditor_product_value_table' not in st.session_state:
    st.session_state.auditor_product_value_table = None
if 'ts_pw_analysis_data' not in st.session_state:
    st.session_state.ts_pw_analysis_data = None
if 'ts_pw_value_data' not in st.session_state:
    st.session_state.ts_pw_value_data = None
if 'ts_pw_budget_data' not in st.session_state:
    st.session_state.ts_pw_budget_data = None
if 'ero_pw_analysis_data' not in st.session_state:
    st.session_state.ero_pw_analysis_data = None
if 'ero_pw_value_data' not in st.session_state:
    st.session_state.ero_pw_value_data = None
if 'ero_pw_budget_data' not in st.session_state:
    st.session_state.ero_pw_budget_data = None

# Helper Functions
def handle_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            f"{dup}_{i}" if i != 0 else dup 
            for i in range(sum(cols == dup))
        ]
    df.columns = cols
    return df

def clean_header_row(header_row):
    return [
        str(col).strip() if pd.notna(col) and str(col).strip() != '' else None
        for col in header_row
    ]

def extract_tables(df, possible_headers, is_product_analysis=False):
    for i in range(len(df)):
        row_text = ' '.join(df.iloc[i].astype(str).str.lower().tolist())
        for header in possible_headers:
            if header.lower() in row_text:
                # First check for budget/actual style headers
                potential_header = df.iloc[i]
                if any(str(col).strip().lower().startswith(('budget-', 'act-', 'ly-', 'gr.', 'ach.')) for col in potential_header[1:]):
                    data_start = i + 2 if i + 2 < len(df) else i + 1
                    if data_start < len(df):
                        first_col = str(df.iloc[data_start, 0]).strip().upper()
                        identifier_cols = ['REGIONS', 'REGION', 'BRANCH', 'ORGANIZATION', 'ORGANIZATION NAME'] if not is_product_analysis else ['PRODUCT', 'PRODUCT GROUP', 'PRODUCT NAME', 'ACETIC ACID', 'AUXILARIES', 'CSF', 'TOTAL']
                        if any(r in first_col for r in identifier_cols) or first_col in ['ACCLLP', 'TOTAL SALES']:
                            return i, data_start
                    
                    data_start = i + 1
                    if data_start < len(df):
                        first_col = str(df.iloc[data_start, 0]).strip().upper()
                        if any(r in first_col for r in identifier_cols) or first_col in ['ACCLLP', 'TOTAL SALES']:
                            return i, data_start
                
                # Check next row for budget/actual style headers
                if i + 1 < len(df):
                    potential_header = df.iloc[i + 1]
                    if any(str(col).strip().lower().startswith(('budget-', 'act-', 'ly-', 'gr.', 'ach.')) for col in potential_header[1:]):
                        data_start = i + 2 if i + 2 < len(df) else i + 1
                        if data_start < len(df):
                            first_col = str(df.iloc[data_start, 0]).strip().upper()
                            identifier_cols = ['REGIONS', 'REGION', 'BRANCH', 'ORGANIZATION', 'ORGANIZATION NAME'] if not is_product_analysis else ['PRODUCT', 'PRODUCT GROUP', 'PRODUCT NAME', 'ACETIC ACID', 'AUXILARIES', 'CSF', 'TOTAL']
                            if any(r in first_col for r in identifier_cols) or first_col in ['ACCLLP', 'TOTAL SALES']:
                                return i + 1, data_start
                
                # If budget/actual style not found, try the first approach
                for j in range(i + 1, min(i + 5, len(df))):
                    row = df.iloc[j]
                    first_col = str(row.iloc[0]).strip().upper()
                    identifier_cols = ['REGIONS', 'REGION', 'BRANCH'] if not is_product_analysis else ['PRODUCT', 'PRODUCT GROUP', 'PRODUCT NAME', 'ACETIC ACID', 'AUXILARIES', 'CSF', 'TOTAL']
                    if any(r in first_col for r in identifier_cols):
                        header_row = j - 1 if j > 0 else j
                        potential_header = df.iloc[header_row]
                        if not all(str(col).strip().upper() in ['MT', 'RS', ''] for col in potential_header[1:]):
                            return header_row, j
                        else:
                            st.warning(f"Header row {header_row} contains only 'MT' or 'Rs', looking for a better header.")
                            header_row = j - 2 if j > 1 else j
                            potential_header = df.iloc[header_row]
                            if not all(str(col).strip().upper() in ['MT', 'RS', ''] for col in potential_header[1:]):
                                return header_row, j
                            else:
                                st.error(f"No proper header row found after table header: {header}")
                                return None, None
                
                st.error(f"No proper header row found after table header: {header}")
                return None, None
    
    st.error(f"Could not locate table header. Tried: {', '.join(possible_headers)}")
    return None, None

def find_column(df, possible_names, case_sensitive=False, threshold=80):
    """Enhanced fuzzy matching for column names"""
    if isinstance(possible_names, str):
        possible_names = [possible_names]
    
    for name in possible_names:
        if case_sensitive:
            if name in df.columns:
                return name
        else:
            for col in df.columns:
                if col.lower() == name.lower():
                    return col
    
    # If exact match not found, try fuzzy matching
    for name in possible_names:
        matches = process.extractOne(name, df.columns, score_cutoff=threshold)
        if matches:
            return matches[0]
    
    return None

def standardize_column_names(df, is_auditor=False):
    df = df.copy()
    original_columns = df.columns.tolist()
    new_columns = []
    last_act_column = None
    column_mapping = {}

    for i, col in enumerate(df.columns):
        col = str(col).upper().strip()
        if is_auditor:
            column_mapping[col] = original_columns[i]

        if any(x in col for x in ['PRODUCT', 'REGION', 'BRANCH']):
            new_columns.append('PRODUCT NAME' if 'PRODUCT' in col else 'REGIONS')
            continue

        if col in ['GR.', 'ACH.']:
            if last_act_column:
                prefix = 'Gr' if col == 'GR.' else 'Ach'
                if 'YTD' in last_act_column:
                    ytd_base = last_act_column.replace('ACT-', '').replace('ACT.', '').strip()
                    new_columns.append(f'{prefix}-{ytd_base}')
                else:
                    month_year = last_act_column.replace('ACT-', '')
                    new_columns.append(f'{prefix}-{month_year}')
            else:
                new_columns.append(col)
            continue

        col_clean = col.replace('BUDGET', '').replace('ACTUAL', '').replace('ACT', '').replace('LY', '').strip()
        month_year = None
        for fmt in ['%b-%y', '%B-%y', '%b-%Y', '%B-%Y']:
            try:
                parsed_date = datetime.strptime(col_clean, fmt)
                month_year = parsed_date.strftime('%b-%y')
                break
            except ValueError:
                continue

        if month_year:
            if 'BUDGET' in col:
                new_columns.append(f'Budget-{month_year}')
            elif 'ACT' in col or 'ACTUAL' in col:
                new_columns.append(f'Act-{month_year}')
                last_act_column = f'Act-{month_year}'
            elif 'LY' in col:
                new_columns.append(f'LY-{month_year}')
            else:
                new_columns.append(col)
        else:
            ytd_match = re.match(r'(?:ACT[- ]*)?YTD[-–\s]*(\d{2})[-–\s]*(\d{2})\s*\((.*?)\)', col, re.IGNORECASE)
            if ytd_match:
                start_year, end_year, period = ytd_match.groups()
                period = period.replace('June', 'Jun').replace('July', 'Jul').replace('August', 'Aug').replace('September', 'Sep').replace('October', 'Oct').replace('November', 'Nov').replace('December', 'Dec')
                ytd_base = f'YTD-{start_year}-{end_year} ({period})'
                if 'ACT' in col:
                    new_columns.append(f'Act-{ytd_base}')
                    last_act_column = f'Act-{ytd_base}'
                elif 'L,Y' in col:
                    new_columns.append(f'{ytd_base}L,Y')
                else:
                    new_columns.append(ytd_base)
            else:
                new_columns.append(col)

    df.columns = new_columns
    return df, column_mapping

def rename_columns(columns):
    renamed = []
    last_act_col = None
    ytd_bases = {}
    original_cols = list(columns)

    # First pass to collect YTD bases from Act-YTD columns
    for col in columns:
        col_clean = str(col).strip()
        if col_clean.startswith('Act-'):
            if 'YTD' in col_clean:
                ytd_match = re.match(
                    r'Act\s*[-]*YTD[-–\s]*(\d{2})[-–\s]*(\d{2})\s*\((.*?)\)',
                    col_clean, 
                    re.IGNORECASE
                )
                if ytd_match:
                    start_year, end_year, period = ytd_match.groups()
                    ytd_base = f"YTD-{start_year}-{end_year} ({period.strip()})"
                    ytd_bases[start_year] = ytd_base

    # Second pass to rename all columns
    for col in columns:
        col_clean = str(col).strip()

        # 1. Handle MT/RS columns
        if col_clean.upper() in ['MT', 'RS']:
            renamed.append(col_clean)
            continue

        # 2. Handle Budget columns with month-year
        budget_match = re.match(
            r'Budget[- ]*(?:Qty|Value)?(\w{3,})[\-\' ]*(\d{2,4})',
            col_clean, 
            re.IGNORECASE
        )
        if budget_match:
            month, year = budget_match.groups()
            month = month[:3].capitalize()
            year = year[-2:] if len(year) > 2 else year
            renamed.append(f"Budget-{month}-{year}")
            continue

        # 3. Handle YTD columns with Budget suffix
        ytd_budget_match = re.match(
            r'YTD[-–\s]*(\d{2})[-–\s]*(\d{2})\s*\((.*?)\)\s*(Budget)',
            col_clean, 
            re.IGNORECASE
        )
        if ytd_budget_match:
            start_year, end_year, period, suffix = ytd_budget_match.groups()
            period = period.replace('to ', 'to ').replace('To ', 'to ').replace('June', 'Jun').replace('July', 'Jul').replace('August', 'Aug').replace('September', 'Sep').replace('October', 'Oct').replace('November', 'Nov').replace('December', 'Dec').strip()
            renamed.append(f"YTD-{start_year}-{end_year} ({period})Budget")
            continue

        # 4. Handle regular YTD columns
        ytd_match = re.match(
            r'YTD[-–\s]*(\d{2})[-–\s]*(\d{2})\s*\((.*?)\)\s*(Act\.|L,Y|LY)?',
            col_clean, 
            re.IGNORECASE
        )
        if ytd_match:
            start_year, end_year, period, suffix = ytd_match.groups()
            period = period.replace('to ', 'to ').replace('To ', 'to ').replace('June', 'Jun').replace('July', 'Jul').replace('August', 'Aug').replace('September', 'Sep').replace('October', 'Oct').replace('November', 'Nov').replace('December', 'Dec').strip()
            ytd_base = f"YTD-{start_year}-{end_year} ({period})"
            
            if suffix:
                suffix = suffix.strip().lower()
                if suffix == 'act.':
                    renamed.append(f"Act-{ytd_base}")
                    last_act_col = renamed[-1]
                elif suffix in ['l,y', 'ly']:
                    renamed.append(f"{ytd_base}LY")
                else:
                    renamed.append(ytd_base)
            else:
                renamed.append(ytd_base)
            continue

        # 5. Handle Act-month-year columns
        act_match = re.match(
            r'Act\s*[-]*\s*(\w{3,})\s*[-]*\s*(\d{2})',
            col_clean, 
            re.IGNORECASE
        )
        if act_match:
            month, year = act_match.groups()
            month = month[:3].capitalize()
            year = year[-2:]
            renamed.append(f"Act-{month}-{year}")
            last_act_col = renamed[-1]
            continue

        # 6. Handle Gr/Ach columns
        if col_clean.strip() == "Gr." and last_act_col:
            renamed.append(last_act_col.replace("Act-", "Gr-"))
            continue
        elif col_clean.strip() == "Ach." and last_act_col:
            renamed.append(last_act_col.replace("Act-", "Ach-"))
            continue

        # 7. Handle LY columns
        ly_match = re.match(
            r'LY\s*[-]*\s*(\w{3,})\s*[-]*\s*(\d{2,4})',
            col_clean, 
            re.IGNORECASE
        )
        if ly_match:
            month, year = ly_match.groups()
            month = month[:3].capitalize()
            year = year[-2:] if len(year) > 2 else year
            renamed.append(f"LY-{month}-{year}")
            continue

        # 8. Handle Gr/Ach-YTD columns
        gr_ytd_match = re.match(r'Gr[- ]*Ytd[- ]*(\d{2})', col_clean, re.IGNORECASE)
        ach_ytd_match = re.match(r'Ach[- ]*Ytd[- ]*(\d{2})', col_clean, re.IGNORECASE)
        if gr_ytd_match or ach_ytd_match:
            year = gr_ytd_match.group(1) if gr_ytd_match else ach_ytd_match.group(1)
            if year in ytd_bases:
                ytd_base = ytd_bases[year]
                prefix = "Gr-" if gr_ytd_match else "Ach-"
                renamed.append(f"{prefix}{ytd_base}")
            else:
                renamed.append(col_clean)
            continue

        # 9. Default case
        renamed.append(col_clean)

    # Validation
    if len(renamed) != len(original_cols):
        error_msg = f"Column count mismatch! Original: {len(original_cols)}, Renamed: {len(renamed)}"
        try:
            import streamlit as st
            st.error(error_msg)
        except ImportError:
            pass
        raise ValueError(error_msg)

    return renamed

def normalize_month_year(col):
    try:
        col = str(col).strip()
        if '-' in col:
            parts = col.split('-')
            if len(parts) == 2:
                month = parts[0][:3].capitalize()
                year = parts[1][-2:] if len(parts[1]) >= 2 else parts[1]
                return f"{month}-{year}"
        elif '/' in col:
            parts = col.split('/')
            if len(parts) >= 2:
                month = datetime.strptime(parts[0], '%m').strftime('%b') if parts[0].isdigit() else parts[0][:3].capitalize()
                year = parts[1][-2:] if len(parts[1]) >= 2 else parts[1]
                return f"{month}-{year}"
        elif ' ' in col:
            parts = col.split()
            if len(parts) == 2:
                month = parts[0][:3].capitalize()
                year = parts[1][-2:] if len(parts[1]) >= 2 else parts[1]
                return f"{month}-{year}"
        try:
            dt = datetime.strptime(col, '%B-%y')
            return dt.strftime('%b-%y')
        except:
            pass
        return col
    except:
        return col

def clean_and_convert_numeric(df):
    """
    Clean and convert DataFrame columns to appropriate data types.
    Handles mixed data types that cause serialization issues.
    """
    if df is None or df.empty:
        return df
        
    df = df.copy()
    
    # Get the first column (identifier column)
    identifier_col = df.columns[0]
    
    # Process each column
    for col in df.columns:
        if col == identifier_col:
            # Keep identifier column as string, ensure no numeric conversion
            df[col] = df[col].astype(str).str.strip()
            # Replace common problematic values
            df[col] = df[col].replace({'nan': '', 'NaN': '', 'None': '', 'null': ''})
            # Don't convert to numeric for identifier columns
            continue
            
        # Try to convert to numeric for other columns
        try:
            # First, convert to string and clean
            series_str = df[col].astype(str).str.strip()
            
            # Replace common non-numeric values
            series_str = series_str.replace({
                'nan': '0',
                'NaN': '0', 
                'None': '0',
                '': '0',
                '-': '0',
                'null': '0'
            })
            
            # Try to convert to numeric
            df[col] = pd.to_numeric(series_str, errors='coerce')
            
            # Fill any remaining NaN values with 0
            df[col] = df[col].fillna(0.0)
            
            # Ensure it's float64 for consistency
            df[col] = df[col].astype('float64')
            
        except Exception as e:
            # If conversion fails, keep as string but clean it
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'nan': '', 'NaN': '', 'None': '', 'null': ''})
            st.warning(f"Could not convert column '{col}' to numeric: {str(e)}")
    
    return df

def safe_format_dataframe(df):
    """
    Safely format a dataframe for display, removing duplicate columns and rounding numeric columns to 2 decimal places.
    
    Args:
        df (pd.DataFrame): Input dataframe to format.
    
    Returns:
        pd.DataFrame: Formatted dataframe with numeric columns rounded to 2 decimal places.
    """
    try:
        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Identify numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        # Create a copy of the dataframe for styling
        styled_df = df.copy()
        
        # Convert to numeric and round to 2 decimal places
        for col in numeric_cols:
            styled_df[col] = pd.to_numeric(styled_df[col], errors='ignore').round(2)
        
        return styled_df
    except Exception as e:
        st.warning(f"Error in safe_format_dataframe: {str(e)}")
        return df

def display_dataframe_safely(df, title="", download_name="table"):
    """
    Safely display a DataFrame with proper formatting and error handling.
    """
    try:
        if df is None or df.empty:
            st.warning(f"No data available for {title}")
            return
        
        if title:
            st.subheader(title)
        
        # Clean and format the dataframe
        clean_df = safe_format_dataframe(df)
        
        # Display the dataframe
        st.dataframe(clean_df, use_container_width=True)
        
        # Add download button
        csv_data = clean_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇️ Download Table as CSV",
            csv_data,
            file_name=f"{download_name}.csv",
            mime="text/csv",
            key=f"download_{download_name}_{hash(title)}"
        )
        
    except Exception as e:
        st.error(f"Error displaying dataframe: {str(e)}")
        # Fallback: display raw dataframe
        try:
            st.dataframe(df, use_container_width=True)
        except:
            st.error("Could not display the dataframe at all.")

def display_raw_dataframe_preview(df, title="", download_name="table"):
    """
    Display DataFrame for preview tabs without complex formatting.
    Converts all data to strings to avoid Arrow serialization issues.
    """
    try:
        if df is None or df.empty:
            st.warning(f"No data available for {title}")
            return
        
        if title:
            st.subheader(title)
        
        # Convert all data to strings for safe display in preview tabs
        display_df = df.copy()
        for col in display_df.columns:
            display_df[col] = display_df[col].astype(str)
        
        # Display the dataframe without styling
        st.dataframe(display_df, use_container_width=True)
        
        # Add download button for original data
        csv_data = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇️ Download Table as CSV",
            csv_data,
            file_name=f"{download_name}.csv",
            mime="text/csv",
            key=f"download_{download_name}_{hash(title)}"
        )
        
    except Exception as e:
        st.error(f"Error displaying dataframe: {str(e)}")
        # Fallback: show basic info about the dataframe
        st.write(f"DataFrame shape: {df.shape}")
        st.write(f"Columns: {list(df.columns)}")

def validate_dataframe(df, name, required_cols=['REGIONS', 'PRODUCT NAME']):
    if df is None or df.empty:
        st.warning(f"{name} is None or empty.")
        return False
    present_cols = [col for col in required_cols if col in df.columns]
    if not present_cols:
        st.error(f"None of the required columns {', '.join(required_cols)} found in {name}.")
        return False
    identifier_col = present_cols[0]
    if df[identifier_col].isna().all():
        st.error(f"All '{identifier_col}' values in {name} are NaN.")
        return False
    return True

def is_effectively_empty(series):
    series_clean = series.astype(str).str.strip()
    return all(s in ['nan', '', '0', '0.0', 'MT', 'TON'] for s in series_clean)

def process_budget_data(budget_df, group_type='region'):
    budget_df = handle_duplicate_columns(budget_df.copy())
    budget_df.columns = budget_df.columns.str.strip()
    
    identifier_col = None
    identifier_names = ['Branch', 'Region', 'REGIONS'] if group_type == 'region' else ['Product', 'Product Group', 'PRODUCT NAME']
    for col in identifier_names:
        if col in budget_df.columns:
            identifier_col = col
            break
    
    if not identifier_col:
        identifier_col = find_column(budget_df, identifier_names[0], threshold=80)
        if not identifier_col:
            st.error(f"Could not find {group_type.capitalize()} column in budget dataset.")
            return None
    
    budget_cols = {'Qty': [], 'Value': []}
    detailed_pattern = r'(Qty|Value)\s*[-]\s*(\w{3,})\'?(\d{2,4})'
    range_pattern = r'(Qty|Value)\s*(\w{3,})\'?(\d{2,4})[-]\s*(\w{3,})\'?(\d{2,4})'
    
    for col in budget_df.columns:
        col_clean = col.lower().replace("'", "").replace(" ", "").replace("-", "")
        
        detailed_match = re.match(detailed_pattern, col, re.IGNORECASE)
        if detailed_match:
            qty_or_value, month, year = detailed_match.groups()
            month = month.capitalize()
            year = year[-2:] if len(year) > 2 else year
            month_year = f"{month}-{year}"
            if qty_or_value.lower() == 'qty':
                budget_cols['Qty'].append((col, month_year))
            elif qty_or_value.lower() == 'value':
                budget_cols['Value'].append((col, month_year))
            continue
        
        range_match = re.match(range_pattern, col, re.IGNORECASE)
        if range_match:
            qty_or_value, start_month, start_year, end_month, end_year = range_match.groups()
            start_month = start_month.capitalize()
            start_year = start_year[-2:] if len(start_year) > 2 else start_year
            end_year = end_year[-2:] if len(end_year) > 2 else end_year
            month_year = f"{start_month}{start_year}{end_month.lower()}-{end_year}"
            if qty_or_value.lower() == 'qty':
                budget_cols['Qty'].append((col, month_year))
            elif qty_or_value.lower() == 'value':
                budget_cols['Value'].append((col, month_year))
    
    if not budget_cols['Qty'] and not budget_cols['Value']:
        st.error(f"No budget quantity or value columns found: Qty={budget_cols['Qty']}, Value={budget_cols['Value']}")
        return None
    
    for col, _ in budget_cols['Qty'] + budget_cols['Value']:
        budget_df[col] = pd.to_numeric(budget_df[col], errors='coerce')
    
    group_cols = [col for col, _ in budget_cols['Qty'] + budget_cols['Value']]
    budget_data = budget_df.groupby(identifier_col)[group_cols].sum().reset_index()
    
    rename_dict = {identifier_col: 'REGIONS' if group_type == 'region' else 'PRODUCT NAME'}
    for col, month_year in budget_cols['Qty']:
        rename_dict[col] = f'Budget-{month_year}_MT'
    for col, month_year in budget_cols['Value']:
        rename_dict[col] = f'Budget-{month_year}_Value'
    
    budget_data = budget_data.rename(columns=rename_dict)
    budget_data[rename_dict[identifier_col]] = budget_data[rename_dict[identifier_col]].str.strip().str.upper()
    
    return budget_data

def process_budget_data_product_region(budget_df, group_type='product_region'):
    budget_df = handle_duplicate_columns(budget_df.copy())
    budget_df.columns = budget_df.columns.str.strip()

    product_col = None
    region_col = None
    product_names = ['Product', 'Product Group', 'PRODUCT NAME']
    region_names = ['Region', 'Branch', 'REGIONS']

    for col in product_names:
        if col in budget_df.columns:
            product_col = col
            break
    if not product_col:
        product_col = find_column(budget_df, product_names[0], threshold=80)

    for col in region_names:
        if col in budget_df.columns:
            region_col = col
            break
    if not region_col:
        region_col = find_column(budget_df, region_names[0], threshold=80)

    if not product_col or not region_col:
        st.error(f"Could not find Product Group or Region column in budget dataset.")
        return None

    budget_cols = {'Qty': [], 'Value': []}
    detailed_pattern = r'(Qty|Value)\s*[-]\s*(\w{3,})\'?(\d{2,4})'
    range_pattern = r'(Qty|Value)\s*(\w{3,})\'?(\d{2,4})[-]\s*(\w{3,})\'?(\d{2,4})'

    for col in budget_df.columns:
        col_clean = col.lower().replace("'", "").replace(" ", "").replace("-", "")
        detailed_match = re.match(detailed_pattern, col, re.IGNORECASE)
        if detailed_match:
            qty_or_value, month, year = detailed_match.groups()
            month = month.capitalize()
            year = year[-2:] if len(year) > 2 else year
            month_year = f"{month}-{year}"
            if qty_or_value.lower() == 'qty':
                budget_cols['Qty'].append((col, month_year))
            elif qty_or_value.lower() == 'value':
                budget_cols['Value'].append((col, month_year))
            continue

        range_match = re.match(range_pattern, col, re.IGNORECASE)
        if range_match:
            qty_or_value, start_month, start_year, end_month, end_year = range_match.groups()
            start_month = start_month.capitalize()
            start_year = start_year[-2:] if len(start_year) > 2 else start_year
            end_year = end_year[-2:] if len(end_year) > 2 else end_year
            month_year = f"{start_month}{start_year}{end_month.lower()}-{end_year}"
            if qty_or_value.lower() == 'qty':
                budget_cols['Qty'].append((col, month_year))
            elif qty_or_value.lower() == 'value':
                budget_cols['Value'].append((col, month_year))

    if not budget_cols['Qty'] and not budget_cols['Value']:
        st.error(f"No budget quantity or value columns found.")
        return None

    for col, _ in budget_cols['Qty'] + budget_cols['Value']:
        budget_df[col] = pd.to_numeric(budget_df[col], errors='coerce')

    group_cols = [col for col, _ in budget_cols['Qty'] + budget_cols['Value']]
    budget_data = budget_df.groupby([product_col, region_col])[group_cols].sum().reset_index()

    rename_dict = {
        product_col: 'PRODUCT NAME',
        region_col: 'Region'
    }
    for col, month_year in budget_cols['Qty']:
        rename_dict[col] = f'Budget-{month_year}_MT'
    for col, month_year in budget_cols['Value']:
        rename_dict[col] = f'Budget-{month_year}_Value'

    budget_data = budget_data.rename(columns=rename_dict)
    budget_data['PRODUCT NAME'] = budget_data['PRODUCT NAME'].str.strip().str.upper()
    budget_data['Region'] = budget_data['Region'].str.strip().str.upper()

    return budget_data

def process_last_year_data(last_year_df, group_type='region'):
    last_year_df = handle_duplicate_columns(last_year_df.copy())
    last_year_df.columns = last_year_df.columns.str.strip()
    
    identifier_col = None
    identifier_names = ['Branch', 'Region', 'REGIONS'] if group_type == 'region' else ['Product', 'Product Group', 'PRODUCT NAME']
    for col in identifier_names:
        if col in last_year_df.columns:
            identifier_col = col
            break
    
    if not identifier_col:
        identifier_col = find_column(last_year_df, identifier_names[0], threshold=80)
        if not identifier_col:
            st.error(f"Could not find {group_type.capitalize()} column in last year dataset.")
            return None
    
    ly_cols = {'Qty': [], 'Value': []}
    detailed_pattern = r'(Qty|Value)\s*[-]\s*(\w{3,})\'?(\d{2,4})'
    range_pattern = r'(Qty|Value)\s*(\w{3,})\'?(\d{2,4})[-]\s*(\w{3,})\'?(\d{2,4})'
    
    for col in last_year_df.columns:
        col_clean = col.lower().replace("'", "").replace(" ", "").replace("-", "")
        
        detailed_match = re.match(detailed_pattern, col, re.IGNORECASE)
        if detailed_match:
            qty_or_value, month, year = detailed_match.groups()
            month = month.capitalize()
            year = year[-2:] if len(year) > 2 else year
            month_year = f"{month}-{year}"
            if qty_or_value.lower() == 'qty':
                ly_cols['Qty'].append((col, month_year))
            elif qty_or_value.lower() == 'value':
                ly_cols['Value'].append((col, month_year))
            continue
        
        range_match = re.match(range_pattern, col, re.IGNORECASE)
        if range_match:
            qty_or_value, start_month, start_year, end_month, end_year = range_match.groups()
            start_month = start_month.capitalize()
            start_year = start_year[-2:] if len(start_year) > 2 else start_year
            end_year = end_year[-2:] if len(end_year) > 2 else end_year
            month_year = f"{start_month}{start_year}{end_month.lower()}-{end_year}"
            if qty_or_value.lower() == 'qty':
                ly_cols['Qty'].append((col, month_year))
            elif qty_or_value.lower() == 'value':
                ly_cols['Value'].append((col, month_year))
    
    if not ly_cols['Qty'] and not ly_cols['Value']:
        st.error(f"No last year quantity or value columns found: Qty={ly_cols['Qty']}, Value={ly_cols['Value']}")
        return None
    
    for col, _ in ly_cols['Qty'] + ly_cols['Value']:
        last_year_df[col] = pd.to_numeric(last_year_df[col], errors='coerce')
    
    group_cols = [col for col, _ in ly_cols['Qty'] + ly_cols['Value']]
    last_year_data = last_year_df.groupby(identifier_col)[group_cols].sum().reset_index()
    
    rename_dict = {identifier_col: 'REGIONS' if group_type == 'region' else 'PRODUCT NAME'}
    for col, month_year in ly_cols['Qty']:
        rename_dict[col] = f'LY-{month_year}_MT'
    for col, month_year in ly_cols['Value']:
        rename_dict[col] = f'LY-{month_year}_Value'
    
    last_year_data = last_year_data.rename(columns=rename_dict)
    last_year_data[rename_dict[identifier_col]] = last_year_data[rename_dict[identifier_col]].str.strip().str.upper()
    
    return last_year_data

with st.sidebar:
    st.header("📁 File Uploads")
    st.session_state.uploaded_file_budget = st.file_uploader(
        "Upload Budget Dataset", 
        type=["xlsx"],
        key="budget_uploader"
    )
    st.session_state.uploaded_file_sales = st.file_uploader(
        "Upload Sales Dataset", 
        type=["xlsx"],
        key="sales_uploader"
    )
    
    st.session_state.uploaded_file_last_year = st.file_uploader(
        "Upload Total Sales Dataset", 
        type=["xlsx"],
        key="last_year_uploader"
    )
    st.session_state.uploaded_file_auditor = st.file_uploader(
        "Upload Auditor Format File", 
        type=["xlsx"],
        key="auditor_uploader"
    )

# Budget Sheet Selection Sidebar
with st.sidebar:
    st.header("📄 Budget Sheet Selection")
    if st.session_state.uploaded_file_budget:
        xls_budget = pd.ExcelFile(st.session_state.uploaded_file_budget)
        sheet_names_budget = xls_budget.sheet_names
        selected_sheet_budget = st.selectbox(
            "Select Budget Sheet", 
            sheet_names_budget,
            key="budget_sheet"
        )

#  Sales Sheet Selection Sidebar
with st.sidebar:
    st.header("📄 Sales Sheet Selection")
    if st.session_state.uploaded_file_sales:
        xls_sales = pd.ExcelFile(st.session_state.uploaded_file_sales)
        sheet_names_sales = xls_sales.sheet_names
        selected_sheets_sales = st.multiselect(
            "Select Sales Sheets ", 
            sheet_names_sales,
            key="sales_sheets",
            default=[sheet_names_sales[0]] if sheet_names_sales else None
        )
        st.session_state.selected_sheets_sales = selected_sheets_sales

# Last Year Sheet Selection Sidebar
with st.sidebar:
    st.header("📄 Total Sales Selection")
    if st.session_state.uploaded_file_last_year:
        xls_last_year = pd.ExcelFile(st.session_state.uploaded_file_last_year)
        sheet_names_last_year = xls_last_year.sheet_names
        selected_sheet_last_year = st.selectbox(
            "Select Last Year Sheet", 
            sheet_names_last_year,
            key="last_year_sheet"
        )

# Auditor Sheet and Table Selection Sidebar
with st.sidebar:
    st.header("📄 Auditor Sheet Selection")
    if st.session_state.uploaded_file_auditor:
        xls_auditor = pd.ExcelFile(st.session_state.uploaded_file_auditor)
        sheet_names_auditor = xls_auditor.sheet_names
        selected_sheet_auditor = st.selectbox(
            "Select Auditor Sheet", 
            sheet_names_auditor,
            key="auditor_sheet"
        )
        
        # Determine analysis type based on sheet name
        is_region_analysis = 'region' in selected_sheet_auditor.lower()
        is_product_analysis = 'product' in selected_sheet_auditor.lower() or 'ts-pw' in selected_sheet_auditor.lower() or 'ero-pw' in selected_sheet_auditor.lower()
        is_sales_analysis_month_wise = re.search(r'sales\s*analysis\s*month\s*wise', selected_sheet_auditor.lower(), re.IGNORECASE) is not None
        
        if is_region_analysis or is_product_analysis or is_sales_analysis_month_wise:
            table_label = "Region Analysis" if is_region_analysis else "Product Analysis"

# Main tabs
st.title("📊 ACL Extraction")
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(["Auditor Format", " Sales and Budget Dataset", "Region Month-wise Analysis", "Product-wise Analysis", "TS-PW Analysis", "ERO-PW Analysis", "Sales Analysis", "Combined Data"])

# Tab 1: Auditor Format
with tab1:
    st.header("📊 Auditor Format - Data Tables")
    
    if st.session_state.get('uploaded_file_auditor') and 'selected_sheet_auditor' in locals():
        try:
            xls = pd.ExcelFile(st.session_state.uploaded_file_auditor)
            df_sheet = pd.read_excel(xls, sheet_name=selected_sheet_auditor, header=None)
            
            is_region_analysis = 'region' in selected_sheet_auditor.lower()
            is_product_analysis = 'product' in selected_sheet_auditor.lower() or 'ts-pw' in selected_sheet_auditor.lower() or 'ero-pw' in selected_sheet_auditor.lower()
            is_sales_analysis_month_wise = re.search(r'sales\s*analysis\s*month\s*wise', selected_sheet_auditor.lower(), re.IGNORECASE) is not None
            
            if is_region_analysis or is_product_analysis or is_sales_analysis_month_wise:
                table1_possible_headers = [
                    "SALES in MT", "SALES IN MT", "Sales in MT", "SALES IN TONNAGE", "SALES IN TON",
                    "Tonnage", "TONNAGE", "Tonnage Sales", "Sales Tonnage", "Metric Tons", "MT Sales"
                ]
                table2_possible_headers = [
                    "SALES in Value", "SALES IN VALUE", "Sales in Value", "SALES IN RS", "VALUE SALES",
                    "Value", "VALUE", "Sales Value"
                ]
                
                idx1, data_start1 = extract_tables(df_sheet, table1_possible_headers, is_product_analysis=is_product_analysis)
                idx2, data_start2 = extract_tables(df_sheet, table2_possible_headers, is_product_analysis=is_product_analysis)
                
                if idx1 is None:
                    st.error(f"❌ Could not locate SALES in Tonnage/MT table header in the sheet. Tried: {', '.join(table1_possible_headers)}")
                    st.dataframe(df_sheet.head(10))
                else:
                    table1_end = idx2 if idx2 is not None and idx2 > idx1 else len(df_sheet)
                    table1 = df_sheet.iloc[data_start1:table1_end].dropna(how='all')
                    table1.columns = df_sheet.iloc[idx1]
                    table1.columns = table1.columns.map(str)
                    table1.columns = rename_columns(table1.columns)
                    
                    if idx2 is not None and idx2 > idx1:
                        table2 = df_sheet.iloc[data_start2:].dropna(how='all')
                        table2.columns = df_sheet.iloc[idx2]
                        table2.columns = table2.columns.map(str)
                        table2.columns = rename_columns(table2.columns)
                    else:
                        table2 = None
                    
                    # Sidebar for table selection
                    st.sidebar.subheader("Select Table to Display")
                    table_choice_auditor = st.sidebar.radio(
                        "Choose table:",
                        ["Table 1: SALES in MT/Tonnage", "Table 2: SALES in Value"],
                        key="table_choice_auditor",
                        index=0 if st.session_state.get('table_choice_auditor', "Table 1: SALES in MT/Tonnage") == "Table 1: SALES in MT/Tonnage" else 1
                    )
                    
                    table_df = None
                    if table_choice_auditor == "Table 1: SALES in MT/Tonnage" and table1 is not None:
                        table_df = table1
                    elif table_choice_auditor == "Table 2: SALES in Value" and table2 is not None:
                        table_df = table2
                    
                    if table_df is None:
                        available_tables = []
                        if table1 is not None:
                            available_tables.append("Table 1: SALES in MT/Tonnage")
                        if table2 is not None:
                            available_tables.append("Table 2: SALES in Value")
                        
                        if available_tables:
                            table_choice_auditor = available_tables[0]
                            table_df = table1 if table_choice_auditor == "Table 1: SALES in MT/Tonnage" else table2
                        else:
                            st.error("No valid tables found in the sheet")
                            st.stop()
                    
                    if table_df.columns.duplicated().any():
                        table_df = table_df.loc[:, ~table_df.columns.duplicated()]
                        st.warning("⚠️ Duplicate columns detected and removed.")
                    
                    if is_region_analysis:
                        if table_choice_auditor == "Table 1: SALES in MT/Tonnage":
                            st.session_state.auditor_mt_table = table_df
                        else:
                            st.session_state.auditor_value_table = table_df
                    elif is_product_analysis:
                        if table_choice_auditor == "Table 1: SALES in MT/Tonnage":
                            st.session_state.auditor_product_mt_table = table_df
                        else:
                            st.session_state.auditor_product_value_table = table_df
                    elif is_sales_analysis_month_wise:
                        if table_choice_auditor == "Table 1: SALES in MT/Tonnage":
                            st.session_state.auditor_monthly_mt_table = table_df
                        else:
                            st.session_state.auditor_monthly_value_table = table_df
                    
                    # Use the simple preview display to avoid Arrow serialization issues
                    display_raw_dataframe_preview(
                        table_df, 
                        title=table_choice_auditor,
                        download_name=f"auditor_{table_choice_auditor.lower().replace(' ', '_').replace(':', '')}"
                    )
                    
        except Exception as e:
            st.error(f"Error processing auditor file: {str(e)}")
            st.write("Please check your file format and try again.")
    else:
        st.info("Please upload an Auditor Format file and select a sheet to view the data tables.")

# Tab 2: Total Sales, Budget, and Last Year Dataset
with tab2:
    st.header("📊  Sales, Budget, and Last Year Dataset")
    
    if st.session_state.uploaded_file_sales or st.session_state.uploaded_file_budget or st.session_state.uploaded_file_last_year:
        if st.session_state.uploaded_file_sales and 'selected_sheets_sales' in st.session_state:
            
            # Create tabs for each selected sales sheet
            sales_tabs = st.tabs([f"Sheet: {sheet}" for sheet in st.session_state.selected_sheets_sales])
            
            for i, sheet_name in enumerate(st.session_state.selected_sheets_sales):
                with sales_tabs[i]:
                    try:
                        # Read Excel with header in first row (row index 0)
                        df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                        df_sales = handle_duplicate_columns(df_sales)
                        
                        # Use simple preview display
                        display_raw_dataframe_preview(
                            df_sales, 
                            f"Sales Data - {sheet_name}", 
                            f"sales_{sheet_name.lower().replace(' ', '_')}"
                        )
                        
                    except Exception as e:
                        st.error(f"Error reading sheet {sheet_name}: {str(e)}")
        
        if st.session_state.uploaded_file_budget and 'selected_sheet_budget' in locals():
            
            try:
                # Read Excel with header in first row (row index 0)
                df_budget = pd.read_excel(xls_budget, sheet_name=selected_sheet_budget, header=0)
                df_budget.columns = df_budget.columns.str.strip()
                df_budget = df_budget.dropna(how='all').reset_index(drop=True)
                df_budget = handle_duplicate_columns(df_budget)
                
                budget_data = process_budget_data(df_budget, group_type='region')
                st.session_state.budget_data = budget_data
                
                # Use simple preview display
                display_raw_dataframe_preview(
                    df_budget, 
                    "Budget Data", 
                    f"budget_{selected_sheet_budget.lower().replace(' ', '_')}"
                )
                
            except Exception as e:
                st.error(f"Error reading budget sheet: {str(e)}")
        
        if st.session_state.uploaded_file_last_year and 'selected_sheet_last_year' in locals():
            
            try:
                # Read Excel with header in first row (row index 0)
                df_last_year = pd.read_excel(xls_last_year, sheet_name=selected_sheet_last_year, header=0)
                df_last_year.columns = df_last_year.columns.str.strip()
                df_last_year = df_last_year.dropna(how='all').reset_index(drop=True)
                df_last_year = handle_duplicate_columns(df_last_year)
                
                # Store raw DataFrame in session state
                st.session_state.last_year_data = df_last_year
                
                # Use simple preview display
                display_raw_dataframe_preview(
                    df_last_year, 
                    "Last Year Data", 
                    f"last_year_{selected_sheet_last_year.lower().replace(' ', '_')}"
                )
                
            except Exception as e:
                st.error(f"Error reading last year sheet: {str(e)}")
    else:
        st.info("ℹ️ Upload Sales, Budget, or Last Year files to view data.")

# Define column aliases for dynamic renaming
column_aliases = {
    'Date': ['Month Format', 'Month', 'Date', 'Time Period', 'Period', 'Month Format-Date'],
    'Product Name': ['Type(Make)', 'Product Group'],
    'Value': ['Value', 'Sales Value',  'Total Amount', 'Amount'],  # For df_sales
    'Amount': ['Amount', 'Total Amount', 'Sales Amount', 'Value', 'Sales Value'],  # For df_last_year
    'Branch': ['Branch.1', 'Branch'],
    'Actual Quantity': ['Actual Quantity', 'Acutal Quantity', 'Quantity', 'Sales Quantity', 'Qty', 'Volume']
}

with tab3:
    st.header("📊 Region Month-wise Analysis")
    if st.session_state.uploaded_file_budget and 'selected_sheet_budget' in locals():
        # Read budget data with header in first row
        df_budget = pd.read_excel(xls_budget, sheet_name=selected_sheet_budget, header=0)
        df_budget.columns = df_budget.columns.str.strip()
        
        df_budget = df_budget.dropna(how='all').reset_index(drop=True)
        
        budget_data = process_budget_data(df_budget, group_type='region')
        st.session_state.budget_data = budget_data
        if budget_data is None:
            st.error("Budget data processing failed.")
            st.stop()
            
        # Determine current, next, and last year dynamically
        current_year = datetime.now().year
        next_year = current_year + 1
        last_year = current_year - 1
        year_suffix = f"{str(current_year)[-2:]}-{str(next_year)[-2:]}"
        ly_year_suffix = f"{str(last_year)[-2:]}-{str(current_year)[-2:]}"
        
        # Define month order (April to March)
        months = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
        
        tab_mt, tab_value, tab_merge = st.tabs(["SALES in MT", "SALES in Value", "Merge Preview"])
        
        with tab_mt:
            mt_cols = [col for col in budget_data.columns if col.endswith('_MT')]
            # Exclude any aggregated budget columns
            mt_cols = [col for col in mt_cols if f'Budget-April{str(last_year)[-2:]}dec-{str(current_year)[-2:]}' not in col]
            
            if mt_cols:
                # Include current year budget columns
                month_cols = sorted(set(col.replace('_MT', '') for col in mt_cols 
                                       if not (col.endswith(f'-{str(last_year)[-2:]}') or col.endswith(f'-{str(current_year)[-2:]}'))))
                # Include last year columns (both year suffixes)
                last_year_cols = sorted(set(col.replace('_MT', '') for col in mt_cols 
                                           if col.endswith(f'-{str(last_year)[-2:]}') or col.endswith(f'-{str(current_year)[-2:]}')))
                
                result_mt = pd.DataFrame()
                for month_col in month_cols:
                    temp_df = budget_data[['REGIONS', f'{month_col}_MT']].rename(
                        columns={f'{month_col}_MT': month_col}
                    )
                    if result_mt.empty:
                        result_mt = temp_df
                    else:
                        result_mt = result_mt.merge(temp_df, on='REGIONS', how='outer')
                
                for month_col in last_year_cols:
                    # Handle both year suffixes for last year
                    if month_col.endswith(f'-{str(last_year)[-2:]}'):
                        orig_month = month_col.replace(f'-{str(last_year)[-2:]}', '')
                    else:
                        orig_month = month_col.replace(f'-{str(current_year)[-2:]}', '')
                    temp_df = budget_data[['REGIONS', f'{month_col}_MT']].rename(
                        columns={f'{month_col}_MT': f'LY-{orig_month}'}
                    )
                    result_mt = result_mt.merge(temp_df, on='REGIONS', how='left')
                
                # Process current year sales data
                if st.session_state.uploaded_file_sales and 'selected_sheets_sales' in st.session_state:
                    all_sales_data = []
                    for sheet_name in st.session_state.selected_sheets_sales:
                        df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                        if isinstance(df_sales.columns, pd.MultiIndex):
                            df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                        df_sales = handle_duplicate_columns(df_sales)
                        
                        branch_col = find_column(df_sales, column_aliases['Branch'])
                        date_col = find_column(df_sales, column_aliases['Date'])
                        qty_col = find_column(df_sales, column_aliases['Actual Quantity'])
                        
                        rename_dict = {}
                        if date_col:
                            rename_dict[date_col] = 'Date'
                        if branch_col:
                            rename_dict[branch_col] = 'Branch.1'
                        if qty_col:
                            rename_dict[qty_col] = 'Actual Quantity'
                        df_sales = df_sales.rename(columns=rename_dict)
                        
                        branch_col = "Branch.1" if "Branch.1" in df_sales.columns else None
                        date_col = "Date" if "Date" in df_sales.columns else None
                        qty_col = "Actual Quantity" if "Actual Quantity" in df_sales.columns else None
                        
                        if not (branch_col and date_col and qty_col):
                            st.warning(f"Required columns not found in sales sheet '{sheet_name}'. Expected: Branch.1, Date, Actual Quantity. Found: {df_sales.columns.tolist()}")
                            continue
                        
                        df_sales['Actual Quantity'] = pd.to_numeric(df_sales[qty_col], errors='coerce')
                        
                        # Robust date parsing
                        try:
                            df_sales['Date'] = pd.to_datetime(df_sales[date_col], errors='coerce')
                            if df_sales['Date'].isna().all():
                                df_sales['Month'] = df_sales[date_col].str.strip().str[:3]
                                df_sales['Year'] = df_sales[date_col].str.extract(r'(\d{4})')[0]
                            else:
                                df_sales['Month'] = df_sales['Date'].dt.strftime('%b')
                                df_sales['Year'] = df_sales['Date'].dt.year
                        except Exception as e:
                            st.warning(f"Date parsing failed for sheet '{sheet_name}': {str(e)}. Attempting string extraction.")
                            df_sales['Month'] = df_sales[date_col].str.strip().str[:3]
                            df_sales['Year'] = df_sales[date_col].str.extract(r'(\d{4})')[0]
                        
                        # Validate and clean Year
                        df_sales['Year'] = pd.to_numeric(df_sales['Year'], errors='coerce').fillna(0).astype(int)
                        if df_sales['Year'].eq(0).any():
                            df_sales.loc[df_sales['Year'] == 0, 'Year'] = current_year
                        
                        # Ensure Month is valid
                        valid_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        df_sales = df_sales[df_sales['Month'].isin(valid_months)]
                        if df_sales.empty:
                            st.warning(f"No valid months found in sheet '{sheet_name}'. Skipping.")
                            continue
                        
                        all_sales_data.append(df_sales)
                    
                    if all_sales_data:
                        combined_sales = pd.concat(all_sales_data, ignore_index=True)
                        
                        try:
                            sales_agg_current = combined_sales.groupby(['Branch.1', 'Month', 'Year'])['Actual Quantity'].sum().reset_index()
                            sales_agg_current.columns = ['REGIONS', 'Month', 'Year', 'Actual']
                            sales_agg_current['REGIONS'] = sales_agg_current['REGIONS'].str.strip().str.upper()
                            
                            # Ensure Year is integer and format Month_Year correctly
                            sales_agg_current['Year'] = sales_agg_current['Year'].astype(int)
                            sales_agg_current['Month_Year'] = sales_agg_current.apply(
                                lambda x: f'Act-{x["Month"]}-{str(x["Year"])[-2:]}', axis=1
                            )
                            
                            actual_mt_current = sales_agg_current.pivot_table(
                                index='REGIONS',
                                columns='Month_Year',
                                values='Actual',
                                aggfunc='sum'
                            ).reset_index().fillna(0)
                            
                            # Check for January-March 2026
                            expected_cols = [f'Act-{m}-26' for m in ['Jan', 'Feb', 'Mar']]
                            missing_cols = [col for col in expected_cols if col not in actual_mt_current.columns]
                            if missing_cols:
                                pass
                        except Exception as e:
                            st.error(f"Error in sales quantity grouping: {str(e)}")
                            actual_mt_current = pd.DataFrame({'REGIONS': result_mt['REGIONS']})
                
                # Process last year data (April to March)
                if st.session_state.uploaded_file_last_year and 'selected_sheet_last_year' in locals():
                    df_last_year = pd.read_excel(xls_last_year, sheet_name=selected_sheet_last_year, header=0)
                    if isinstance(df_last_year.columns, pd.MultiIndex):
                        df_last_year.columns = ['_'.join(col).strip() for col in df_last_year.columns.values]
                    df_last_year = handle_duplicate_columns(df_last_year)
                    
                    branch_col = find_column(df_last_year, column_aliases['Branch'])
                    date_col = find_column(df_last_year, column_aliases['Date'])
                    qty_col = find_column(df_last_year, column_aliases['Actual Quantity'])
                    
                    rename_dict = {}
                    if date_col:
                        rename_dict[date_col] = 'Date'
                    if branch_col:
                        rename_dict[branch_col] = 'Branch.1'
                    if qty_col:
                        rename_dict[qty_col] = 'Actual Quantity'
                    df_last_year = df_last_year.rename(columns=rename_dict)
                    
                    branch_col = "Branch.1" if "Branch.1" in df_last_year.columns else None
                    date_col = "Date" if "Date" in df_last_year.columns else None
                    qty_col = "Actual Quantity" if "Actual Quantity" in df_last_year.columns else None
                    
                    if not (branch_col and date_col and qty_col):
                        st.warning(f"Required columns not found in last year sheet '{selected_sheet_last_year}'. Expected: Branch.1, Date, Actual Quantity. Found: {df_last_year.columns.tolist()}")
                        actual_mt_last = pd.DataFrame({'REGIONS': result_mt['REGIONS']})
                    else:
                        df_last_year['Actual Quantity'] = pd.to_numeric(df_last_year[qty_col], errors='coerce')
                        
                        # Robust date parsing for last year
                        try:
                            df_last_year['Date'] = pd.to_datetime(df_last_year[date_col], errors='coerce')
                            if df_last_year['Date'].isna().all():
                                df_last_year['Month'] = df_last_year[date_col].str.strip().str[:3]
                                df_last_year['Year'] = df_last_year[date_col].str.extract(r'(\d{4})')[0]
                            else:
                                df_last_year['Month'] = df_last_year['Date'].dt.strftime('%b')
                                df_last_year['Year'] = df_last_year['Date'].dt.year
                        except Exception as e:
                            st.warning(f"Date parsing failed for last year sheet '{selected_sheet_last_year}': {str(e)}. Attempting string extraction.")
                            df_last_year['Month'] = df_last_year[date_col].str.strip().str[:3]
                            df_last_year['Year'] = df_last_year[date_col].str.extract(r'(\d{4})')[0]
                        
                        # Validate and clean Year
                        df_last_year['Year'] = pd.to_numeric(df_last_year['Year'], errors='coerce').fillna(0).astype(int)
                        if df_last_year['Year'].eq(0).any():
                            st.warning(f"Invalid or missing years found in last year sheet '{selected_sheet_last_year}'. Defaulting to last year ({last_year}).")
                            df_last_year.loc[df_last_year['Year'] == 0, 'Year'] = last_year
                        
                        # Ensure Month is valid
                        valid_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        df_last_year = df_last_year[df_last_year['Month'].isin(valid_months)]
                        if df_last_year.empty:
                            st.warning(f"No valid months found in last year sheet '{selected_sheet_last_year}'. Skipping.")
                            actual_mt_last = pd.DataFrame({'REGIONS': result_mt['REGIONS']})
                        else:
                            try:
                                last_year_agg = df_last_year.groupby(['Branch.1', 'Month', 'Year'])['Actual Quantity'].sum().reset_index()
                                last_year_agg.columns = ['REGIONS', 'Month', 'Year', 'LY_Actual']
                                last_year_agg['REGIONS'] = last_year_agg['REGIONS'].str.strip().str.upper()
                                last_year_agg['Month_Year'] = last_year_agg.apply(
                                    lambda x: f'LY-{x["Month"]}-{str(x["Year"])[-2:]}', axis=1
                                )
                                
                                actual_mt_last = last_year_agg.pivot_table(
                                    index='REGIONS',
                                    columns='Month_Year',
                                    values='LY_Actual',
                                    aggfunc='sum'
                                ).reset_index().fillna(0)
                                
                                # Check for January-March 2025
                                expected_cols = [f'LY-{m}-25' for m in ['Jan', 'Feb', 'Mar']]
                                missing_cols = [col for col in expected_cols if col not in actual_mt_last.columns]
                                if missing_cols:
                                    st.warning(f"Missing Last Year data for: {', '.join(missing_cols)}. Ensure xls_last_year includes January-March 2025.")
                            except Exception as e:
                                st.error(f"Error in last year quantity grouping: {str(e)}")
                                actual_mt_last = pd.DataFrame({'REGIONS': result_mt['REGIONS']})
                
                actual_mt = pd.DataFrame({'REGIONS': result_mt['REGIONS']})
                if 'actual_mt_current' in locals():
                    actual_mt = safe_merge_dataframes(actual_mt, actual_mt_current, on_column='REGIONS', how='left')
                if 'actual_mt_last' in locals():
                    actual_mt = safe_merge_dataframes(actual_mt, actual_mt_last, on_column='REGIONS', how='left')
                st.session_state.actual_mt_data = actual_mt
                
                result_mt = safe_merge_dataframes(result_mt, actual_mt, on_column='REGIONS', how='left')
                
                # Calculate Growth and Achievement
                for month in months:
                    budget_col = (
                        f'Budget-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Budget-{month}-{str(next_year)[-2:]}'
                    )
                    actual_col = (
                        f'Act-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Act-{month}-{str(next_year)[-2:]}'
                    )
                    ly_col = (
                        f'LY-{month}-{str(last_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'LY-{month}-{str(current_year)[-2:]}'
                    )
                    gr_col = (
                        f'Gr-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Gr-{month}-{str(next_year)[-2:]}'
                    )
                    ach_col = (
                        f'Ach-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Ach-{month}-{str(next_year)[-2:]}'
                    )
                    result_mt[gr_col] = np.nan
                    result_mt[ach_col] = np.nan
                    
                    if budget_col in result_mt.columns and actual_col in result_mt.columns and ly_col in result_mt.columns:
                        result_mt[gr_col] = (
                            (result_mt[actual_col] - result_mt[ly_col]) /
                            result_mt[ly_col].replace(0, np.nan) * 100
                        ).round(2)
                        result_mt[ach_col] = (
                            (result_mt[actual_col] /
                             result_mt[budget_col].replace(0, np.nan)) * 100
                        ).round(2)
                
                exclude_regions = ['NORTH TOTAL', 'WEST SALES', 'GRAND TOTAL']
                valid_regions = result_mt[~result_mt['REGIONS'].isin(exclude_regions)]
                grand_total_row = {'REGIONS': 'GRAND TOTAL'}
                numeric_cols = valid_regions.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    grand_total_row[col] = valid_regions[col].sum().round(2)
                
                result_mt = result_mt[result_mt['REGIONS'] != 'GRAND TOTAL']
                result_mt = pd.concat([result_mt, pd.DataFrame([grand_total_row])], ignore_index=True)
                
                result_mt = result_mt.rename(columns={'REGIONS': 'SALES in MT'})
                st.session_state.region_analysis_data = result_mt
                
                st.subheader("Region-wise Budget and Actual Quantity (Month-wise)")
                try:
                    styled_df = safe_format_dataframe(result_mt)
                    numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                    formatter = {col: "{:,.2f}" for col in numeric_cols}
                    st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                except:
                    st.dataframe(result_mt, use_container_width=True)
                
                csv_mt = result_mt.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "⬇️ Download Budget Quantity Data",
                    csv_mt,
                    file_name=f"region_budget_qty_{selected_sheet_budget}.csv",
                    mime="text/csv"
                )
        with tab_value:
            value_cols = [col for col in budget_data.columns if col.endswith('_Value')]
            value_cols = [col for col in value_cols if f'Budget-April{str(last_year)[-2:]}dec-{str(current_year)[-2:]}' not in col]
            
            if value_cols:
                month_cols = sorted(set(col.replace('_Value', '') for col in value_cols 
                                       if not (col.endswith(f'-{str(last_year)[-2:]}') or col.endswith(f'-{str(current_year)[-2:]}'))))
                last_year_cols = sorted(set(col.replace('_Value', '') for col in value_cols 
                                           if col.endswith(f'-{str(last_year)[-2:]}') or col.endswith(f'-{str(current_year)[-2:]}')))
                
                result_value = pd.DataFrame()
                for month_col in month_cols:
                    temp_df = budget_data[['REGIONS', f'{month_col}_Value']].rename(
                        columns={f'{month_col}_Value': month_col}
                    )
                    if result_value.empty:
                        result_value = temp_df
                    else:
                        result_value = result_value.merge(temp_df, on='REGIONS', how='outer')
                
                for month_col in last_year_cols:
                    if month_col.endswith(f'-{str(last_year)[-2:]}'):
                        orig_month = month_col.replace(f'-{str(last_year)[-2:]}', '')
                    else:
                        orig_month = month_col.replace(f'-{str(current_year)[-2:]}', '')
                    temp_df = budget_data[['REGIONS', f'{month_col}_Value']].rename(
                        columns={f'{month_col}_Value': f'LY-{orig_month}'}
                    )
                    result_value = result_value.merge(temp_df, on='REGIONS', how='left')
                
                # Process current year sales data
                if st.session_state.uploaded_file_sales and 'selected_sheets_sales' in st.session_state:
                    all_sales_data = []
                    for sheet_name in st.session_state.selected_sheets_sales:
                        df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                        if isinstance(df_sales.columns, pd.MultiIndex):
                            df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                        df_sales = handle_duplicate_columns(df_sales)
                        
                        branch_col = find_column(df_sales, column_aliases['Branch'])
                        date_col = find_column(df_sales, column_aliases['Date'])
                        value_col = find_column(df_sales, column_aliases['Value'])
                        
                        rename_dict = {}
                        if date_col:
                            rename_dict[date_col] = 'Date'
                        if branch_col:
                            rename_dict[branch_col] = 'Branch.1'
                        if value_col:
                            rename_dict[value_col] = 'Value'
                        df_sales = df_sales.rename(columns=rename_dict)
                        
                        branch_col = "Branch.1" if "Branch.1" in df_sales.columns else None
                        date_col = "Date" if "Date" in df_sales.columns else None
                        value_col = "Value" if "Value" in df_sales.columns else None
                        
                        if not (branch_col and date_col and value_col):
                            st.warning(f"Required columns not found in sales sheet '{sheet_name}'. Expected: Branch.1, Date, Value. Found: {df_sales.columns.tolist()}")
                            continue
                        
                        try:
                            df_sales[value_col] = pd.to_numeric(df_sales[value_col], errors='coerce')
                        except ValueError as e:
                            st.error(f"Error converting Value column to numeric in sales data: {str(e)}")
                            continue
                        
                        # Robust date parsing
                        try:
                            df_sales['Date'] = pd.to_datetime(df_sales[date_col], errors='coerce')
                            if df_sales['Date'].isna().all():
                                df_sales['Month'] = df_sales[date_col].str.strip().str[:3]
                                df_sales['Year'] = df_sales[date_col].str.extract(r'(\d{4})')[0]
                            else:
                                df_sales['Month'] = df_sales['Date'].dt.strftime('%b')
                                df_sales['Year'] = df_sales['Date'].dt.year
                        except Exception as e:
                            st.warning(f"Date parsing failed for sheet '{sheet_name}': {str(e)}. Attempting string extraction.")
                            df_sales['Month'] = df_sales[date_col].str.strip().str[:3]
                            df_sales['Year'] = df_sales[date_col].str.extract(r'(\d{4})')[0]
                        
                        # Validate and clean Year
                        df_sales['Year'] = pd.to_numeric(df_sales['Year'], errors='coerce').fillna(0).astype(int)
                        if df_sales['Year'].eq(0).any():
                            df_sales.loc[df_sales['Year'] == 0, 'Year'] = current_year
                        
                        # Ensure Month is valid
                        valid_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        df_sales = df_sales[df_sales['Month'].isin(valid_months)]
                        if df_sales.empty:
                            st.warning(f"No valid months found in sheet '{sheet_name}'. Skipping.")
                            continue
                        
                        all_sales_data.append(df_sales)
                    
                    if all_sales_data:
                        combined_sales = pd.concat(all_sales_data, ignore_index=True)
                        
                        try:
                            sales_agg_current = combined_sales.groupby(['Branch.1', 'Month', 'Year'])['Value'].sum().reset_index()
                            sales_agg_current.columns = ['REGIONS', 'Month', 'Year', 'Actual']
                            sales_agg_current['REGIONS'] = sales_agg_current['REGIONS'].str.strip().str.upper()
                            
                            # Ensure Year is integer and format Month_Year correctly
                            sales_agg_current['Year'] = sales_agg_current['Year'].astype(int)
                            sales_agg_current['Month_Year'] = sales_agg_current.apply(
                                lambda x: f'Act-{x["Month"]}-{str(x["Year"])[-2:]}', axis=1
                            )
                            
                            actual_value_current = sales_agg_current.pivot_table(
                                index='REGIONS',
                                columns='Month_Year',
                                values='Actual',
                                aggfunc='sum'
                            ).reset_index().fillna(0)
                            
                        except Exception as e:
                            st.error(f"Error in sales value grouping: {str(e)}")
                            actual_value_current = pd.DataFrame({'REGIONS': result_value['REGIONS']})
                
                # Process last year data (April to March)
                if st.session_state.uploaded_file_last_year and 'selected_sheet_last_year' in locals():
                    df_last_year = pd.read_excel(xls_last_year, sheet_name=selected_sheet_last_year, header=0)
                    if isinstance(df_last_year.columns, pd.MultiIndex):
                        df_last_year.columns = ['_'.join(col).strip() for col in df_last_year.columns.values]
                    df_last_year = handle_duplicate_columns(df_last_year)
                    
                    branch_col = find_column(df_last_year, column_aliases['Branch'])
                    date_col = find_column(df_last_year, column_aliases['Date'])
                    amount_col = find_column(df_last_year, column_aliases['Amount'])
                    
                    rename_dict = {}
                    if date_col:
                        rename_dict[date_col] = 'Date'
                    if branch_col:
                        rename_dict[branch_col] = 'Branch.1'
                    if amount_col:
                        rename_dict[amount_col] = 'Amount'
                    df_last_year = df_last_year.rename(columns=rename_dict)
                    
                    branch_col = "Branch.1" if "Branch.1" in df_last_year.columns else None
                    date_col = "Date" if "Date" in df_last_year.columns else None
                    amount_col = "Amount" if "Amount" in df_last_year.columns else None
                    
                    if not (branch_col and date_col and amount_col):
                        st.warning(f"Required columns not found in last year sheet '{selected_sheet_last_year}'. Expected: Branch.1, Date, Amount. Found: {df_last_year.columns.tolist()}")
                        actual_value_last = pd.DataFrame({'REGIONS': result_value['REGIONS']})
                    else:
                        try:
                            df_last_year[amount_col] = pd.to_numeric(df_last_year[amount_col], errors='coerce')
                        except ValueError as e:
                            st.error(f"Error converting Amount column to numeric in last year data: {str(e)}")
                            actual_value_last = pd.DataFrame({'REGIONS': result_value['REGIONS']})
                        else:
                            try:
                                df_last_year['Date'] = pd.to_datetime(df_last_year[date_col], errors='coerce')
                                if df_last_year['Date'].isna().all():
                                    df_last_year['Month'] = df_last_year[date_col].str.strip().str[:3]
                                    df_last_year['Year'] = df_last_year[date_col].str.extract(r'(\d{4})')[0]
                                else:
                                    df_last_year['Month'] = df_last_year['Date'].dt.strftime('%b')
                                    df_last_year['Year'] = df_last_year['Date'].dt.year
                            except Exception as e:
                                st.warning(f"Date parsing failed for last year sheet '{selected_sheet_last_year}': {str(e)}. Attempting string extraction.")
                                df_last_year['Month'] = df_last_year[date_col].str.strip().str[:3]
                                df_last_year['Year'] = df_last_year[date_col].str.extract(r'(\d{4})')[0]
                            
                            # Validate and clean Year
                            df_last_year['Year'] = pd.to_numeric(df_last_year['Year'], errors='coerce').fillna(0).astype(int)
                            if df_last_year['Year'].eq(0).any():
                                st.warning(f"Invalid or missing years found in last year sheet '{selected_sheet_last_year}'. Defaulting to last year ({last_year}).")
                                df_last_year.loc[df_last_year['Year'] == 0, 'Year'] = last_year
                            
                            # Ensure Month is valid
                            valid_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                            df_last_year = df_last_year[df_last_year['Month'].isin(valid_months)]
                            if df_last_year.empty:
                                st.warning(f"No valid months found in last year sheet '{selected_sheet_last_year}'. Skipping.")
                                actual_value_last = pd.DataFrame({'REGIONS': result_value['REGIONS']})
                            else:
                                try:
                                    last_year_agg = df_last_year.groupby(['Branch.1', 'Month', 'Year'])['Amount'].sum().reset_index()
                                    last_year_agg.columns = ['REGIONS', 'Month', 'Year', 'LY_Actual']
                                    last_year_agg['REGIONS'] = last_year_agg['REGIONS'].str.strip().str.upper()
                                    last_year_agg['Month_Year'] = last_year_agg.apply(
                                        lambda x: f'LY-{x["Month"]}-{str(x["Year"])[-2:]}', axis=1
                                    )
                                    
                                    actual_value_last = last_year_agg.pivot_table(
                                        index='REGIONS',
                                        columns='Month_Year',
                                        values='LY_Actual',
                                        aggfunc='sum'
                                    ).reset_index().fillna(0)
                                    
                                except Exception as e:
                                    st.error(f"Error in last year value grouping: {str(e)}")
                                    actual_value_last = pd.DataFrame({'REGIONS': result_value['REGIONS']})
                
                actual_value = pd.DataFrame({'REGIONS': result_value['REGIONS']})
                if 'actual_value_current' in locals():
                    actual_value = safe_merge_dataframes(actual_value, actual_value_current, on_column='REGIONS', how='left')
                if 'actual_value_last' in locals():
                    actual_value = safe_merge_dataframes(actual_value, actual_value_last, on_column='REGIONS', how='left')
                st.session_state.actual_value_data = actual_value
                
                result_value = safe_merge_dataframes(result_value, actual_value, on_column='REGIONS', how='left')
                
                # Calculate Growth and Achievement
                for month in months:
                    budget_col = (
                        f'Budget-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Budget-{month}-{str(next_year)[-2:]}'
                    )
                    actual_col = (
                        f'Act-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Act-{month}-{str(next_year)[-2:]}'
                    )
                    ly_col = (
                        f'LY-{month}-{str(last_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'LY-{month}-{str(current_year)[-2:]}'
                    )
                    gr_col = (
                        f'Gr-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Gr-{month}-{str(next_year)[-2:]}'
                    )
                    ach_col = (
                        f'Ach-{month}-{str(current_year)[-2:]}'
                        if month in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        else f'Ach-{month}-{str(next_year)[-2:]}'
                    )
                    result_value[gr_col] = np.nan
                    result_value[ach_col] = np.nan
                    
                    if budget_col in result_value.columns and actual_col in result_value.columns and ly_col in result_value.columns:
                        result_value[gr_col] = (
                            (result_value[actual_col] - result_value[ly_col]) /
                            result_value[ly_col].replace(0, np.nan) * 100
                        ).round(2)
                        result_value[ach_col] = (
                            (result_value[actual_col] /
                             result_value[budget_col].replace(0, np.nan)) * 100
                        ).round(2)
                
                exclude_regions = ['NORTH TOTAL', 'WEST SALES', 'GRAND TOTAL']
                valid_regions = result_value[~result_value['REGIONS'].isin(exclude_regions)]
                grand_total_row = {'REGIONS': 'GRAND TOTAL'}
                numeric_cols = valid_regions.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    grand_total_row[col] = valid_regions[col].sum().round(2)
                
                result_value = result_value[result_value['REGIONS'] != 'GRAND TOTAL']
                result_value = pd.concat([result_value, pd.DataFrame([grand_total_row])], ignore_index=True)
                
                result_value = result_value.rename(columns={'REGIONS': 'SALES in Value'})
                st.session_state.region_value_data = result_value
                
                st.subheader("Region-wise Budget and Actual Value (Month-wise)")
                try:
                    styled_df = safe_format_dataframe(result_value)
                    numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                    formatter = {col: "{:,.2f}" for col in numeric_cols}
                    st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                except:
                    st.dataframe(result_value, use_container_width=True)
                
                csv_value = result_value.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "⬇️ Download Budget and Actual Value Data",
                    csv_value,
                    file_name=f"region_budget_actual_value_{selected_sheet_budget}.csv",
                    mime="text/csv"
                )

        with tab_merge:
            if st.session_state.uploaded_file_auditor and st.session_state.region_analysis_data is not None and 'selected_sheet_auditor' in locals():
                st.subheader("🔀 Merge Preview with Auditor Data")
                xls_auditor = pd.ExcelFile(st.session_state.uploaded_file_auditor)
                
                region_sheet = None
                for sheet in xls_auditor.sheet_names:
                    if 'region' in sheet.lower():
                        region_sheet = sheet
                        break
                
                if region_sheet:
                    df_auditor = pd.read_excel(xls_auditor, sheet_name=region_sheet, header=None)
                    
                    mt_table_headers = ["SALES in MT", "SALES IN MT"]
                    value_table_headers = ["SALES in Value", "SALES IN VALUE"]
                    
                    mt_idx, mt_data_start = extract_tables(df_auditor, mt_table_headers)
                    value_idx, value_data_start = extract_tables(df_auditor, value_table_headers)
                    
                    if mt_idx is not None:
                        if value_idx is not None and value_idx > mt_idx:
                            mt_table = df_auditor.iloc[mt_data_start:value_idx].dropna(how='all')
                        else:
                            mt_table = df_auditor.iloc[mt_data_start:].dropna(how='all')
                        original_columns = df_auditor.iloc[mt_idx]
                        renamed_columns = ['SALES in MT'] + list(original_columns[1:])
                        mt_table.columns = renamed_columns
                        mt_table.columns = rename_columns(mt_table.columns)
                        mt_table = handle_duplicate_columns(mt_table)
                        mt_table['SALES in MT'] = mt_table['SALES in MT'].astype(str).str.strip().str.upper()
                        for col in mt_table.columns[1:]:
                            mt_table[col] = pd.to_numeric(mt_table[col], errors='coerce').fillna(0)
                        numeric_cols = mt_table.select_dtypes(include=[np.number]).columns
                        mt_table[numeric_cols] = mt_table[numeric_cols].astype(float).round(2)
                        st.session_state.auditor_mt_table = mt_table
                    
                    if value_idx is not None:
                        value_table = df_auditor.iloc[value_data_start:].dropna(how='all')
                        original_columns = df_auditor.iloc[value_idx]
                        renamed_columns = ['SALES in Value'] + list(original_columns[1:])
                        value_table.columns = renamed_columns
                        value_table.columns = rename_columns(renamed_columns)
                        value_table = handle_duplicate_columns(value_table)
                        value_table['SALES in Value'] = value_table['SALES in Value'].astype(str).str.strip().str.upper()
                        for col in value_table.columns[1:]:
                            value_table[col] = pd.to_numeric(value_table[col], errors='coerce').fillna(0)
                        numeric_cols = value_table.select_dtypes(include=[np.number]).columns
                        value_table[numeric_cols] = value_table[numeric_cols].astype(float).round(2)
                        st.session_state.auditor_value_table = value_table
                    
                    merged_mt_data = pd.DataFrame()
                    if st.session_state.auditor_mt_table is not None and not result_mt.empty:
                        merged_mt_data = st.session_state.auditor_mt_table.copy()
                        
                        for col in merged_mt_data.columns[1:]:
                            merged_mt_data[col] = pd.to_numeric(merged_mt_data[col], errors='coerce').fillna(0)
                        numeric_cols = merged_mt_data.select_dtypes(include=[np.number]).columns
                        
                        north_regions = ['BGLR', 'CHENNAI', 'PONDY']
                        west_regions = ['COVAI', 'ERODE', 'MADURAI', 'POULTRY', 'KARUR', 'SALEM', 'TIRUPUR']
                        valid_north_regions = [r for r in north_regions if r in merged_mt_data['SALES in MT'].values]
                        valid_west_regions = [r for r in west_regions if r in merged_mt_data['SALES in MT'].values]
                        
                        common_columns = set(merged_mt_data.columns) & set(result_mt.columns) - {'SALES in MT'}
                        if common_columns:
                            for col in common_columns:
                                for region in merged_mt_data['SALES in MT']:
                                    if region in result_mt['SALES in MT'].values and region not in ['GRAND TOTAL', 'NORTH TOTAL', 'WEST SALES']:
                                        region_value = result_mt.loc[result_mt['SALES in MT'] == region, col].values
                                        if len(region_value) > 0:
                                            merged_mt_data.loc[merged_mt_data['SALES in MT'] == region, col] = region_value[0]
                        
                        if valid_north_regions and 'NORTH TOTAL' in merged_mt_data['SALES in MT'].values:
                            for col in numeric_cols:
                                sum_value = merged_mt_data[merged_mt_data['SALES in MT'].isin(valid_north_regions)][col].sum().round(2)
                                merged_mt_data.loc[merged_mt_data['SALES in MT'] == 'NORTH TOTAL', col] = sum_value
                        
                        west_mask = merged_mt_data['SALES in MT'] == 'WEST SALES'
                        if valid_west_regions and west_mask.any():
                            for col in numeric_cols:
                                sum_value = merged_mt_data[merged_mt_data['SALES in MT'].isin(valid_west_regions)][col].sum().round(2)
                                merged_mt_data.loc[west_mask, col] = sum_value
                        
                        if 'GRAND TOTAL' in merged_mt_data['SALES in MT'].values:
                            for col in numeric_cols:
                                sum_value = merged_mt_data[~merged_mt_data['SALES in MT'].isin(['NORTH TOTAL', 'WEST SALES', 'GRAND TOTAL'])][col].sum().round(2)
                                merged_mt_data.loc[merged_mt_data['SALES in MT'] == 'GRAND TOTAL', col] = sum_value
                        else:
                            grand_total_row = {'SALES in MT': 'GRAND TOTAL'}
                            for col in numeric_cols:
                                grand_total_row[col] = merged_mt_data[~merged_mt_data['SALES in MT'].isin(['NORTH TOTAL', 'WEST SALES'])][col].sum().round(2)
                            merged_mt_data = pd.concat([merged_mt_data, pd.DataFrame([grand_total_row])], ignore_index=True)
                        
                        ytd_periods = {
                            f'YTD-{year_suffix} (Apr to Jun)Budget': [f'Budget-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun']],
                            f'YTD-{year_suffix} (Apr to Sep)Budget': [f'Budget-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']],
                            f'YTD-{year_suffix} (Apr to Dec)Budget': [f'Budget-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']],
                            f'YTD-{year_suffix} (Apr to Mar)Budget': [f'Budget-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']] +
                                                                     [f'Budget-{m}-{str(next_year)[-2:]}' for m in ['Jan', 'Feb', 'Mar']],
                            f'YTD-{ly_year_suffix} (Apr to Jun)LY': [f'LY-{m}-{str(last_year)[-2:]}' for m in ['Apr', 'May', 'Jun']],
                            f'YTD-{ly_year_suffix} (Apr to Sep)LY': [f'LY-{m}-{str(last_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']],
                            f'YTD-{ly_year_suffix} (Apr to Dec)LY': [f'LY-{m}-{str(last_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']],
                            f'YTD-{ly_year_suffix} (Apr to Mar)LY': [f'LY-{m}-{str(last_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']] +
                                                                    [f'LY-{m}-{str(current_year)[-2:]}' for m in ['Jan', 'Feb', 'Mar']],
                            f'Act-YTD-{year_suffix} (Apr to Jun)': [f'Act-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun']],
                            f'Act-YTD-{year_suffix} (Apr to Sep)': [f'Act-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']],
                            f'Act-YTD-{year_suffix} (Apr to Dec)': [f'Act-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']],
                            f'Act-YTD-{year_suffix} (Apr to Mar)': [f'Act-{m}-{str(current_year)[-2:]}' for m in ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']] +
                                                                   [f'Act-{m}-{str(next_year)[-2:]}' for m in ['Jan', 'Feb', 'Mar']]
                        }
                        
                        for ytd_col, months in ytd_periods.items():
                            valid_months = [month for month in months if month in merged_mt_data.columns]
                            if valid_months:
                                merged_mt_data[ytd_col] = merged_mt_data[valid_months].sum(axis=1, skipna=True).astype(float).round(2)
                        
                        ytd_pairs = [
                            ('Apr to Jun', f'YTD-{year_suffix} (Apr to Jun)Budget', f'YTD-{ly_year_suffix} (Apr to Jun)LY', f'Act-YTD-{year_suffix} (Apr to Jun)'),
                            ('Apr to Sep', f'YTD-{year_suffix} (Apr to Sep)Budget', f'YTD-{ly_year_suffix} (Apr to Sep)LY', f'Act-YTD-{year_suffix} (Apr to Sep)'),
                            ('Apr to Dec', f'YTD-{year_suffix} (Apr to Dec)Budget', f'YTD-{ly_year_suffix} (Apr to Dec)LY', f'Act-YTD-{year_suffix} (Apr to Dec)'),
                            ('Apr to Mar', f'YTD-{year_suffix} (Apr to Mar)Budget', f'YTD-{ly_year_suffix} (Apr to Mar)LY', f'Act-YTD-{year_suffix} (Apr to Mar)')
                        ]
                        
                        for period, budget_col, ly_col, act_col in ytd_pairs:
                            if all(col in merged_mt_data.columns for col in [budget_col, ly_col, act_col]):
                                merged_mt_data[f'Gr-YTD-{year_suffix} ({period})'] = (
                                    ((merged_mt_data[act_col] - merged_mt_data[ly_col]) /
                                     merged_mt_data[ly_col].replace(0, np.nan)) * 100
                                ).fillna(0).astype(float).round(2)
                                merged_mt_data[f'Ach-YTD-{year_suffix} ({period})'] = (
                                    (merged_mt_data[act_col] /
                                     merged_mt_data[budget_col].replace(0, np.nan)) * 100
                                ).fillna(0).astype(float).round(2)
                        
                        # Remove 'REGIONS' row from merged_mt_data
                        merged_mt_data = merged_mt_data[merged_mt_data['SALES in MT'] != 'REGIONS'].reset_index(drop=True)
                    
                    merged_value_data = pd.DataFrame()
                    if st.session_state.auditor_value_table is not None and not result_value.empty:
                        merged_value_data = st.session_state.auditor_value_table.copy()
                        common_columns = set(merged_value_data.columns) & set(result_value.columns) - {'SALES in Value'}
                        if common_columns:
                            for col in common_columns:
                                for region in merged_value_data['SALES in Value']:
                                    if region in result_value['SALES in Value'].values and region not in ['GRAND TOTAL', 'NORTH TOTAL', 'WEST SALES']:
                                        region_value = result_value.loc[result_value['SALES in Value'] == region, col].values
                                        if len(region_value) > 0:
                                            merged_value_data.loc[merged_value_data['SALES in Value'] == region, col] = region_value[0]
                        
                        for col in merged_value_data.columns:
                            if col != 'SALES in Value':
                                merged_value_data[col] = pd.to_numeric(merged_value_data[col], errors='coerce').fillna(0)
                        
                        numeric_cols = merged_value_data.select_dtypes(include=[np.number]).columns
                        merged_value_data[numeric_cols] = merged_value_data[numeric_cols].astype(float).round(2)
                        
                        north_regions = ['BGLR', 'CHENNAI', 'PONDY']
                        west_regions = ['COVAI', 'ERODE', 'MADURAI', 'POULTRY', 'KARUR', 'SALEM', 'TIRUPUR']
                        valid_north_regions = [r for r in north_regions if r in merged_value_data['SALES in Value'].values]
                        valid_west_regions = [r for r in west_regions if r in merged_value_data['SALES in Value'].values]
                        
                        if valid_north_regions and 'NORTH TOTAL' in merged_value_data['SALES in Value'].values:
                            for col in numeric_cols:
                                sum_value = merged_value_data[merged_value_data['SALES in Value'].isin(valid_north_regions)][col].sum().round(2)
                                merged_value_data.loc[merged_value_data['SALES in Value'] == 'NORTH TOTAL', col] = sum_value
                        
                        west_mask = merged_value_data['SALES in Value'] == 'WEST SALES'
                        if valid_west_regions and west_mask.any():
                            for col in numeric_cols:
                                sum_value = merged_value_data[merged_value_data['SALES in Value'].isin(valid_west_regions)][col].sum().round(2)
                                merged_value_data.loc[west_mask, col] = sum_value
                        
                        for ytd_col, months in ytd_periods.items():
                            valid_months = [month for month in months if month in merged_value_data.columns]
                            if valid_months:
                                merged_value_data[ytd_col] = merged_value_data[valid_months].sum(axis=1, skipna=True).astype(float).round(2)
                        
                        for period, budget_col, ly_col, act_col in ytd_pairs:
                            if all(col in merged_value_data.columns for col in [budget_col, ly_col, act_col]):
                                merged_value_data[f'Gr-YTD-{year_suffix} ({period})'] = (
                                    ((merged_value_data[act_col] - merged_value_data[ly_col]) /
                                     merged_value_data[ly_col].replace(0, np.nan)) * 100
                                ).fillna(0).astype(float).round(2)
                                merged_value_data[f'Ach-YTD-{year_suffix} ({period})'] = (
                                    (merged_value_data[act_col] /
                                     merged_value_data[budget_col].replace(0, np.nan)) * 100
                                ).fillna(0).astype(float).round(2)
                        
                        if not merged_value_data.empty:
                            grand_total_mask = merged_value_data['SALES in Value'] == 'GRAND TOTAL'
                            if grand_total_mask.any():
                                non_total_data = merged_value_data[~grand_total_mask & 
                                                                  ~merged_value_data['SALES in Value'].isin(['NORTH TOTAL', 'WEST SALES'])]
                                total_sums = non_total_data.select_dtypes(include=[np.number]).sum().round(2)
                                for col in total_sums.index:
                                    merged_value_data.loc[grand_total_mask, col] = total_sums[col]
                        
                        # Remove 'REGIONS' row from merged_value_data
                        merged_value_data = merged_value_data[merged_value_data['SALES in Value'] != 'REGIONS'].reset_index(drop=True)
                    
                    # Store merged data in session state
                    st.session_state.merged_region_mt_data = merged_mt_data
                    st.session_state.merged_region_value_data = merged_value_data
                    
                    # Display merged data
                    if not merged_mt_data.empty:
                        st.subheader("Merged Data (SALES in MT)")
                        try:
                            styled_df = safe_format_dataframe(merged_mt_data)
                            numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                            formatter = {col: "{:,.2f}" for col in numeric_cols}
                            st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                        except:
                            st.dataframe(merged_mt_data, use_container_width=True)
                    
                    if not merged_value_data.empty:
                        st.subheader("Merged Data (SALES in Value)")
                        try:
                            styled_df = safe_format_dataframe(merged_value_data)
                            numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                            formatter = {col: "{:,.2f}" for col in numeric_cols}
                            st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                        except:
                            st.dataframe(merged_value_data, use_container_width=True)
                    
                    # Excel export
                    if not merged_mt_data.empty or not merged_value_data.empty:
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            workbook = writer.book
                            worksheet = workbook.add_worksheet('Merged_Region_Data')
                            title_format = workbook.add_format({
                                'bold': True,
                                'align': 'center',
                                'valign': 'vcenter',
                                'font_size': 14,
                                'font_color': '#000000'
                            })
                            header_format = workbook.add_format({
                                'bold': True,
                                'text_wrap': True,
                                'valign': 'top',
                                'fg_color': '#4472C4',
                                'font_color': 'white',
                                'border': 1
                            })
                            num_format = workbook.add_format({'num_format': '#,##0.00'})
                            total_format = workbook.add_format({
                                'bold': True,
                                'fg_color': '#D3D3D3',
                                'border': 1,
                                'num_format': '#,##0.00'
                            })
                            north_total_format = workbook.add_format({
                                'bold': True,
                                'fg_color': '#90EE90',
                                'border': 1,
                                'num_format': '#,##0.00'
                            })
                            west_sales_format = workbook.add_format({
                                'bold': True,
                                'fg_color': '#ADD8E6',
                                'border': 1,
                                'num_format': '#,##0.00'
                            })
                            
                            num_cols = max(
                                len(merged_mt_data.columns) if not merged_mt_data.empty else 0,
                                len(merged_value_data.columns) if not merged_value_data.empty else 0
                            )
                            worksheet.merge_range(
                                2, 0, 2, num_cols - 1,
                                f"REGION WISE REVIEW FOR THE YR {current_year} VS {next_year}",
                                title_format
                            )
                            start_row = 4
                            if not merged_mt_data.empty:
                                merged_mt_data.to_excel(
                                    writer,
                                    sheet_name='Merged_Region_Data',
                                    startrow=start_row,
                                    index=False
                                )
                                for col_num, value in enumerate(merged_mt_data.columns.values):
                                    worksheet.write(start_row, col_num, value, header_format)
                                for col in merged_mt_data.select_dtypes(include=[np.number]):
                                    col_idx = merged_mt_data.columns.get_loc(col)
                                    worksheet.set_column(col_idx, col_idx, None, num_format)
                                grand_total_mask = merged_mt_data['SALES in MT'] == 'GRAND TOTAL'
                                if grand_total_mask.any():
                                    grand_total_idx = merged_mt_data.index[grand_total_mask][0]
                                    total_row_idx = start_row + 1 + grand_total_idx
                                    for col_idx in range(len(merged_mt_data.columns)):
                                        worksheet.write(total_row_idx, col_idx, 
                                                        merged_mt_data.iloc[grand_total_idx, col_idx], total_format)
                                north_total_mask = merged_mt_data['SALES in MT'] == 'NORTH TOTAL'
                                if north_total_mask.any():
                                    north_total_idx = merged_mt_data.index[north_total_mask][0]
                                    north_row_idx = start_row + 1 + north_total_idx
                                    for col_idx in range(len(merged_mt_data.columns)):
                                        worksheet.write(north_row_idx, col_idx, 
                                                        merged_mt_data.iloc[north_total_idx, col_idx], north_total_format)
                                west_sales_mask = merged_mt_data['SALES in MT'] == 'WEST SALES'
                                if west_sales_mask.any():
                                    west_sales_idx = merged_mt_data.index[west_sales_mask][0]
                                    west_row_idx = start_row + 1 + west_sales_idx
                                    for col_idx in range(len(merged_mt_data.columns)):
                                        worksheet.write(west_row_idx, col_idx, 
                                                        merged_mt_data.iloc[west_sales_idx, col_idx], west_sales_format)
                                for i, col in enumerate(merged_mt_data.columns):
                                    max_len = max((merged_mt_data[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, max_len)
                                start_row += len(merged_mt_data) + 4
                            
                            if not merged_value_data.empty:
                                merged_value_data.to_excel(
                                    writer,
                                    sheet_name='Merged_Region_Data',
                                    startrow=start_row,
                                    index=False
                                )
                                for col_num, value in enumerate(merged_value_data.columns.values):
                                    worksheet.write(start_row, col_num, value, header_format)
                                for col in merged_value_data.select_dtypes(include=[np.number]):
                                    col_idx = merged_value_data.columns.get_loc(col)
                                    worksheet.set_column(col_idx, col_idx, None, num_format)
                                grand_total_mask = merged_value_data['SALES in Value'] == 'GRAND TOTAL'
                                if grand_total_mask.any():
                                    grand_total_idx = merged_value_data.index[grand_total_mask][0]
                                    total_row_idx = start_row + 1 + grand_total_idx
                                    for col_idx in range(len(merged_value_data.columns)):
                                        worksheet.write(total_row_idx, col_idx, 
                                                        merged_value_data.iloc[grand_total_idx, col_idx], total_format)
                                north_total_mask = merged_value_data['SALES in Value'] == 'NORTH TOTAL'
                                if north_total_mask.any():
                                    north_total_idx = merged_value_data.index[north_total_mask][0]
                                    north_row_idx = start_row + 1 + north_total_idx
                                    for col_idx in range(len(merged_value_data.columns)):
                                        worksheet.write(north_row_idx, col_idx, 
                                                        merged_value_data.iloc[north_total_idx, col_idx], north_total_format)
                                west_sales_mask = merged_value_data['SALES in Value'] == 'WEST SALES'
                                if west_sales_mask.any():
                                    west_sales_idx = merged_value_data.index[west_sales_mask][0]
                                    west_row_idx = start_row + 1 + west_sales_idx
                                    for col_idx in range(len(merged_value_data.columns)):
                                        worksheet.write(west_row_idx, col_idx, 
                                                        merged_value_data.iloc[west_sales_idx, col_idx], west_sales_format)
                                for i, col in enumerate(merged_value_data.columns):
                                    max_len = max((merged_value_data[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, max_len)
                        
                        output.seek(0)
                        excel_data = output.getvalue()
                        st.download_button(
                            label="⬇️ Download Merged Region Data as Excel",
                            data=excel_data,
                            file_name="merged_region_data.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="region_merge_download"
                        )
                else:
                    st.info("ℹ️ No region sheet found in auditor file. Please ensure a sheet with 'region' in its name exists.")
            else: 
                st.info("ℹ️ Upload auditor file and generate region data first.")
    else: 
        st.info("ℹ️ Please upload the Budget file first.")



def find_column(df, possible_names, case_sensitive=False):
    # Placeholder for finding column names
    # Should return the matching column name or None
    for col in df.columns:
        col_clean = col.strip()
        if not case_sensitive:
            col_clean = col_clean.lower()
            possible_names = [name.lower() for name in possible_names]
        if col_clean in possible_names:
            return col
    return None

with tab4:
    st.header("📊 Product-wise Analysis")
    
    # Get current date and determine fiscal year
    current_date = datetime.now()
    current_year = current_date.year
    if current_date.month >= 4:
        fiscal_year_start = current_year
        fiscal_year_end = current_year + 1
    else:
        fiscal_year_start = current_year - 1
        fiscal_year_end = current_year
    fiscal_year_str = f"{str(fiscal_year_start)[-2:]}-{str(fiscal_year_end)[-2:]}"
    last_fiscal_year_start = fiscal_year_start - 1
    last_fiscal_year_end = fiscal_year_end - 1
    last_fiscal_year_str = f"{str(last_fiscal_year_start)[-2:]}-{str(last_fiscal_year_end)[-2:]}"
    
    # Define months for April to March
    months = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
    
    selected_sheet_budget = None
    if st.session_state.get('uploaded_file_budget'):
        xls_budget = pd.ExcelFile(st.session_state.uploaded_file_budget)
        budget_sheet_names = xls_budget.sheet_names
        if budget_sheet_names:
            selected_sheet_budget = budget_sheet_names[0]
            if 'budget_sheet_selection' in st.session_state:
                selected_sheet_budget = st.session_state.budget_sheet_selection
    
    if (st.session_state.get('uploaded_file_sales') and st.session_state.get('uploaded_file_budget') and 
        'selected_sheets_sales' in st.session_state and selected_sheet_budget):
        try:
            xls_budget = pd.ExcelFile(st.session_state.uploaded_file_budget)
            df_budget = pd.read_excel(xls_budget, sheet_name=selected_sheet_budget)
            df_budget.columns = df_budget.columns.str.strip()
            df_budget = df_budget.dropna(how='all').reset_index(drop=True)

            budget_data = process_budget_data(df_budget, group_type='product')
            if budget_data is None:
                st.error("Failed to process budget data for products.")
                st.stop()
            st.session_state.product_budget_data = budget_data

            required_cols = ['PRODUCT NAME']
            if all(col in budget_data.columns for col in required_cols):
                # Define YTD periods dynamically (only for merge tab)
                ytd_periods = {}
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Jun)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Sep)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Dec)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Mar)Budget'] = (
                    [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'Budget-{month}-{str(fiscal_year_end)[-2:]}' for month in months[9:]]
                )
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Jun)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Sep)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Dec)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Mar)LY'] = (
                    [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'LY-{month}-{str(last_fiscal_year_end)[-2:]}' for month in months[9:]]
                )
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Jun)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Sep)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Dec)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Mar)'] = (
                    [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'Act-{month}-{str(fiscal_year_end)[-2:]}' for month in months[9:]]
                )

                tab_product_mt, tab_product_value, tab_product_merge = st.tabs(
                    ["SALES in Tonage", "SALES in Value", "Merge Preview"]
                )

                with tab_product_mt:
                    mt_cols = [col for col in budget_data.columns if col.endswith('_MT')]
                    mt_cols = [col for col in mt_cols if not col.endswith(f'-{last_fiscal_year_start}_MT')]
                    
                    if mt_cols:
                        month_cols = sorted(set(col.replace('_MT', '') for col in mt_cols if not col.endswith(f'-{last_fiscal_year_start}_MT')))
                        last_year_cols = sorted(set(col.replace('_MT', '') for col in budget_data.columns if col.endswith(f'-{last_fiscal_year_start}_MT')))
                        
                        result_product_mt = budget_data[['PRODUCT NAME']].copy()
                        
                        for month_col in month_cols:
                            if f'{month_col}_MT' in budget_data.columns:
                                result_product_mt[month_col] = budget_data[f'{month_col}_MT']
                        
                        for month_col in last_year_cols:
                            orig_month = month_col.replace(f'-{last_fiscal_year_start}', '')
                            if f'{month_col}_MT' in budget_data.columns:
                                result_product_mt[f'LY-{orig_month}'] = budget_data[f'{month_col}_MT']
                        
                        result_product_mt['PRODUCT NAME'] = result_product_mt['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')

                        actual_mt_data = {}
                        selected_sheet_last_year = st.session_state.get('last_year_sheet')
                        
                        if st.session_state.get('uploaded_file_sales') and 'selected_sheets_sales' in st.session_state:
                            xls_sales = pd.ExcelFile(st.session_state.uploaded_file_sales)
                            
                            for sheet_name in st.session_state.selected_sheets_sales:
                                try:
                                    df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                                    
                                    if isinstance(df_sales.columns, pd.MultiIndex):
                                        df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                                    
                                    df_sales = handle_duplicate_columns(df_sales)
                                    
                                    product_col = find_column(df_sales, ['Type (Make)', 'Type(Make)'], case_sensitive=False)
                                    date_col = find_column(df_sales, ['Month Format', 'Date', 'Month'], case_sensitive=False)
                                    qty_col = find_column(df_sales, ['Actual Quantity', 'Acutal Quantity'], case_sensitive=False)
                                    
                                    if sheet_name.lower() == 'sheet1' or 'total sales' in sheet_name.lower():
                                        product_col = find_column(df_sales, ['Type (Make)', 'Type(Make)'], case_sensitive=False) or product_col
                                        qty_col = find_column(df_sales, ['Actual Quantity', 'Acutal Quantity'], case_sensitive=False) or qty_col
                                    
                                    if product_col and date_col and qty_col:
                                        df_sales = df_sales[[product_col, date_col, qty_col]].copy()
                                        df_sales.columns = ['Product Group', 'Month Format', 'Actual Quantity']
                                        
                                        df_sales['Actual Quantity'] = pd.to_numeric(df_sales['Actual Quantity'], errors='coerce').fillna(0)
                                        df_sales['Product Group'] = df_sales['Product Group'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                        
                                        if pd.api.types.is_datetime64_any_dtype(df_sales['Month Format']):
                                            df_sales['Month'] = pd.to_datetime(df_sales['Month Format']).dt.strftime('%b')
                                        else:
                                            df_sales['Month'] = df_sales['Month Format'].astype(str).str.strip().str[:3]
                                        
                                        df_sales = df_sales.dropna(subset=['Actual Quantity', 'Month'])
                                        df_sales = df_sales[df_sales['Actual Quantity'] != 0]
                                        
                                        grouped = df_sales.groupby(['Product Group', 'Month'])['Actual Quantity'].sum().reset_index()
                                        
                                        for _, row in grouped.iterrows():
                                            product = row['Product Group']
                                            month = row['Month']
                                            qty = row['Actual Quantity']
                                            
                                            year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                                            col_name = f'Act-{month}-{year}'
                                            
                                            if product not in actual_mt_data:
                                                actual_mt_data[product] = {}
                                            if col_name in actual_mt_data[product]:
                                                actual_mt_data[product][col_name] += qty
                                            else:
                                                actual_mt_data[product][col_name] = qty
                                    else:
                                        st.warning(f"Required columns not found in sales sheet '{sheet_name}'. Expected: Type (Make)/Product Group, Month Format/Date, Actual Quantity. Found: {df_sales.columns.tolist()}")
                                        
                                except Exception as e:
                                    st.warning(f"Error processing sheet '{sheet_name}': {str(e)}")
                                    continue
                        
                        if st.session_state.get('uploaded_file_last_year') and selected_sheet_last_year:
                            try:
                                xls_last_year = pd.ExcelFile(st.session_state.uploaded_file_last_year)
                                df_last_year = pd.read_excel(xls_last_year, sheet_name=selected_sheet_last_year, header=0)
                                
                                if isinstance(df_last_year.columns, pd.MultiIndex):
                                    df_last_year.columns = ['_'.join(col).strip() for col in df_last_year.columns.values]
                                df_last_year = handle_duplicate_columns(df_last_year)
                                
                                product_col = find_column(df_last_year, ['Type (Make)', 'Type(Make)', 'Product Group', 'Product'], case_sensitive=False)
                                date_col = find_column(df_last_year, ['Month Format', 'Date', 'Month'], case_sensitive=False)
                                qty_col = find_column(df_last_year, ['Actual Quantity', 'Acutal Quantity', 'Quantity'], case_sensitive=False)
                                
                                if product_col and date_col and qty_col:
                                    df_last_year = df_last_year[[product_col, date_col, qty_col]].copy()
                                    df_last_year.columns = ['Product Group', 'Month Format', 'Actual Quantity']
                                    
                                    df_last_year['Actual Quantity'] = pd.to_numeric(df_last_year['Actual Quantity'], errors='coerce').fillna(0)
                                    df_last_year['Product Group'] = df_last_year['Product Group'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    if pd.api.types.is_datetime64_any_dtype(df_last_year['Month Format']):
                                        df_last_year['Month'] = pd.to_datetime(df_last_year['Month Format']).dt.strftime('%b')
                                    else:
                                        df_last_year['Month'] = df_last_year['Month Format'].astype(str).str.strip().str[:3]
                                    
                                    df_last_year = df_last_year.dropna(subset=['Actual Quantity', 'Month'])
                                    df_last_year = df_last_year[df_last_year['Actual Quantity'] != 0]
                                    
                                    grouped_ly = df_last_year.groupby(['Product Group', 'Month'])['Actual Quantity'].sum().reset_index()
                                    
                                    for _, row in grouped_ly.iterrows():
                                        product = row['Product Group']
                                        month = row['Month']
                                        qty = row['Actual Quantity']
                                        
                                        year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                                        col_name = f'LY-{month}-{year}'
                                        
                                        if product not in actual_mt_data:
                                            actual_mt_data[product] = {}
                                        if col_name in actual_mt_data[product]:
                                            actual_mt_data[product][col_name] += qty
                                        else:
                                            actual_mt_data[product][col_name] = qty
                                    
                            
                                else:
                                    st.warning(f"Required columns not found in last year sheet '{selected_sheet_last_year}'. Expected: Type (Make)/Product Group, Month Format/Date, Actual Quantity. Found: {df_last_year.columns.tolist()}")
                            except Exception as e:
                                st.warning(f"Error processing last year data: {str(e)}")
                        else:
                            st.info("ℹ️ No last year file uploaded. Last year comparison data will not be available.")
                        
                        for product, data in actual_mt_data.items():
                            matching_rows = result_product_mt[result_product_mt['PRODUCT NAME'] == product]
                            if not matching_rows.empty:
                                idx = matching_rows.index[0]
                                for col, value in data.items():
                                    if col not in result_product_mt.columns:
                                        result_product_mt[col] = 0.0
                                    if len(matching_rows) > 1:
                                        existing_value = result_product_mt.loc[idx, col] if col in result_product_mt.columns else 0
                                        result_product_mt.loc[idx, col] = existing_value + value
                                    else:
                                        result_product_mt.loc[idx, col] = value
                        
                        if result_product_mt['PRODUCT NAME'].duplicated().any():
                            
                            numeric_cols_result = result_product_mt.select_dtypes(include=[np.number]).columns
                            agg_dict = {col: 'sum' for col in numeric_cols_result}
                            result_product_mt = result_product_mt.groupby('PRODUCT NAME', as_index=False).agg(agg_dict)
                        
                        numeric_cols = result_product_mt.select_dtypes(include=[np.number]).columns
                        result_product_mt[numeric_cols] = result_product_mt[numeric_cols].fillna(0)

                        # Calculate Growth Rate (Gr) and Achievement (Ach) for each month up to March
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            # Initialize Gr and Ach columns if they don't exist
                            if gr_col not in result_product_mt.columns:
                                result_product_mt[gr_col] = 0.0
                            if ach_col not in result_product_mt.columns:
                                result_product_mt[ach_col] = 0.0
                            
                            # Calculate Growth Rate: ((Actual - Last Year) / Last Year) * 100
                            if ly_col in result_product_mt.columns and actual_col in result_product_mt.columns:
                                result_product_mt[gr_col] = np.where(
                                    (result_product_mt[ly_col] != 0) & (pd.notna(result_product_mt[ly_col])) & (pd.notna(result_product_mt[actual_col])),
                                    ((result_product_mt[actual_col] - result_product_mt[ly_col]) / result_product_mt[ly_col] * 100).round(2),
                                    0
                                )
                            
                            # Calculate Achievement: (Actual / Budget) * 100
                            if budget_col in result_product_mt.columns and actual_col in result_product_mt.columns:
                                result_product_mt[ach_col] = np.where(
                                    (result_product_mt[budget_col] != 0) & (pd.notna(result_product_mt[budget_col])) & (pd.notna(result_product_mt[actual_col])),
                                    (result_product_mt[actual_col] / result_product_mt[budget_col] * 100).round(2),
                                    0
                                )

                        exclude_products = ['NORTH TOTAL', 'WEST SALES', 'GRAND TOTAL']
                        mask = ~result_product_mt['PRODUCT NAME'].isin(exclude_products + ['TOTAL SALES'])
                        valid_products = result_product_mt[mask]
                        
                        total_row = pd.DataFrame({'PRODUCT NAME': ['TOTAL SALES']})
                        for col in numeric_cols:
                            if col in valid_products.columns:
                                total_row[col] = [valid_products[col].sum().round(2)]
                        
                        # Recalculate Gr and Ach for TOTAL SALES row
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            if all(col in total_row.columns for col in [budget_col, actual_col, ly_col]):
                                # Recalculate Growth Rate for totals
                                if total_row[ly_col].iloc[0] != 0:
                                    total_row[gr_col] = [((total_row[actual_col].iloc[0] - total_row[ly_col].iloc[0]) / total_row[ly_col].iloc[0] * 100).round(2)]
                                else:
                                    total_row[gr_col] = [0]
                                
                                # Recalculate Achievement for totals
                                if total_row[budget_col].iloc[0] != 0:
                                    total_row[ach_col] = [(total_row[actual_col].iloc[0] / total_row[budget_col].iloc[0] * 100).round(2)]
                                else:
                                    total_row[ach_col] = [0]
                        
                        result_product_mt = pd.concat([valid_products, total_row], ignore_index=True)
                        result_product_mt = result_product_mt.rename(columns={'PRODUCT NAME': 'SALES in Tonage'})
                        
                        st.session_state.product_mt_data = result_product_mt

                        st.subheader(f"Product-wise Budget and Actual Tonage (Month-wise) [{fiscal_year_str}]")
                        
                        display_df = result_product_mt.copy()
                        numeric_display_cols = display_df.select_dtypes(include=[np.number]).columns
                        
                        try:
                            for col in numeric_display_cols:
                                display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "0.00")
                            st.dataframe(display_df, use_container_width=True)
                        except:
                            st.dataframe(result_product_mt, use_container_width=True)

                        csv_mt = result_product_mt.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            "⬇️ Download Budget Tonage Data",
                            csv_mt,
                            file_name=f"product_budget_qty_{selected_sheet_budget}_{fiscal_year_str}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.warning("No budget tonage columns found.")

                with tab_product_value:
                    value_cols = [col for col in budget_data.columns if col.endswith('_Value')]
                    value_cols = [col for col in value_cols if not col.endswith(f'-{last_fiscal_year_start}_Value')]
                    
                    if value_cols:
                        month_cols = sorted(set(col.replace('_Value', '') for col in value_cols if not col.endswith(f'-{last_fiscal_year_start}_Value')))
                        last_year_cols = sorted(set(col.replace('_Value', '') for col in budget_data.columns if col.endswith(f'-{last_fiscal_year_start}_Value')))
                        
                        result_product_value = budget_data[['PRODUCT NAME']].copy()
                        
                        for month_col in month_cols:
                            if f'{month_col}_Value' in budget_data.columns:
                                result_product_value[month_col] = budget_data[f'{month_col}_Value']
                        
                        for month_col in last_year_cols:
                            orig_month = month_col.replace(f'-{last_fiscal_year_start}', '')
                            if f'{month_col}_Value' in budget_data.columns:
                                result_product_value[f'LY-{orig_month}'] = budget_data[f'{month_col}_Value']
                        
                        result_product_value['PRODUCT NAME'] = result_product_value['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')

                        actual_value_data = {}
                        
                        if st.session_state.get('uploaded_file_sales') and 'selected_sheets_sales' in st.session_state:
                            xls_sales = pd.ExcelFile(st.session_state.uploaded_file_sales)
                            
                            for sheet_name in st.session_state.selected_sheets_sales:
                                try:
                                    df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                                    
                                    if isinstance(df_sales.columns, pd.MultiIndex):
                                        df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                                    
                                    df_sales = handle_duplicate_columns(df_sales)
                                    
                                    product_col = find_column(df_sales, ['Type (Make)', 'Type(Make)'], case_sensitive=False)
                                    date_col = find_column(df_sales, ['Month Format', 'Date', 'Month'], case_sensitive=False)
                                    value_col = find_column(df_sales, ['Amount', 'Value', 'Sales Value'], case_sensitive=False)
                                    
                                    if product_col and date_col and value_col:
                                        df_sales = df_sales[[product_col, date_col, value_col]].copy()
                                        df_sales.columns = ['Product Group', 'Month Format', 'Value']
                                        
                                        df_sales['Value'] = pd.to_numeric(df_sales['Value'], errors='coerce').fillna(0)
                                        df_sales['Product Group'] = df_sales['Product Group'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                        
                                        if pd.api.types.is_datetime64_any_dtype(df_sales['Month Format']):
                                            df_sales['Month'] = pd.to_datetime(df_sales['Month Format']).dt.strftime('%b')
                                        else:
                                            df_sales['Month'] = df_sales['Month Format'].astype(str).str.strip().str[:3]
                                        
                                        df_sales = df_sales.dropna(subset=['Value', 'Month'])
                                        df_sales = df_sales[df_sales['Value'] != 0]
                                        
                                        grouped = df_sales.groupby(['Product Group', 'Month'])['Value'].sum().reset_index()
                                        
                                        for _, row in grouped.iterrows():
                                            product = row['Product Group']
                                            month = row['Month']
                                            value = row['Value']
                                            
                                            year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                                            col_name = f'Act-{month}-{year}'
                                            
                                            if product not in actual_value_data:
                                                actual_value_data[product] = {}
                                            if col_name in actual_value_data[product]:
                                                actual_value_data[product][col_name] += value
                                            else:
                                                actual_value_data[product][col_name] = value
                                    else:
                                        st.warning(f"Required columns not found in sales sheet '{sheet_name}'. Expected: Type (Make)/Product Group, Month Format/Date, Amount/Value. Found: {df_sales.columns.tolist()}")
                                        
                                except Exception as e:
                                    st.warning(f"Error processing sheet '{sheet_name}': {str(e)}")
                                    continue
                        
                        if st.session_state.get('uploaded_file_last_year') and selected_sheet_last_year:
                            try:
                                xls_last_year = pd.ExcelFile(st.session_state.uploaded_file_last_year)
                                df_last_year = pd.read_excel(xls_last_year, sheet_name=selected_sheet_last_year, header=0)
                                
                                if isinstance(df_last_year.columns, pd.MultiIndex):
                                    df_last_year.columns = ['_'.join(col).strip() for col in df_last_year.columns.values]
                                df_last_year = handle_duplicate_columns(df_last_year)
                                
                                product_col = find_column(df_last_year, ['Type (Make)', 'Type(Make)', 'Product Group', 'Product'], case_sensitive=False)
                                date_col = find_column(df_last_year, ['Month Format', 'Date', 'Month'], case_sensitive=False)
                                amount_col = find_column(df_last_year, ['Amount', 'Value', 'Sales Value'], case_sensitive=False)
                                
                                if product_col and date_col and amount_col:
                                    df_last_year = df_last_year[[product_col, date_col, amount_col]].copy()
                                    df_last_year.columns = ['Product Group', 'Month Format', 'Amount']
                                    
                                    df_last_year['Amount'] = pd.to_numeric(df_last_year['Amount'], errors='coerce').fillna(0)
                                    df_last_year['Product Group'] = df_last_year['Product Group'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    if pd.api.types.is_datetime64_any_dtype(df_last_year['Month Format']):
                                        df_last_year['Month'] = pd.to_datetime(df_last_year['Month Format']).dt.strftime('%b')
                                    else:
                                        df_last_year['Month'] = df_last_year['Month Format'].astype(str).str.strip().str[:3]
                                    
                                    df_last_year = df_last_year.dropna(subset=['Amount', 'Month'])
                                    df_last_year = df_last_year[df_last_year['Amount'] != 0]
                                    
                                    grouped_ly = df_last_year.groupby(['Product Group', 'Month'])['Amount'].sum().reset_index()
                                    
                                    for _, row in grouped_ly.iterrows():
                                        product = row['Product Group']
                                        month = row['Month']
                                        amount = row['Amount']
                                        
                                        year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                                        col_name = f'LY-{month}-{year}'
                                        
                                        if product not in actual_value_data:
                                            actual_value_data[product] = {}
                                        if col_name in actual_value_data[product]:
                                            actual_value_data[product][col_name] += amount
                                        else:
                                            actual_value_data[product][col_name] = amount
                                    
                                    
                                else:
                                    st.warning(f"Required columns not found in last year sheet '{selected_sheet_last_year}'. Expected: Type (Make)/Product Group, Month Format/Date, Amount/Value. Found: {df_last_year.columns.tolist()}")
                            except Exception as e:
                                st.warning(f"Error processing last year value data: {str(e)}")
                        else:
                            st.info("ℹ️ No last year file uploaded. Last year comparison data will not be available.")
                        
                        for product, data in actual_value_data.items():
                            matching_rows = result_product_value[result_product_value['PRODUCT NAME'] == product]
                            if not matching_rows.empty:
                                idx = matching_rows.index[0]
                                for col, value in data.items():
                                    if col not in result_product_value.columns:
                                        result_product_value[col] = 0.0
                                    if len(matching_rows) > 1:
                                        existing_value = result_product_value.loc[idx, col] if col in result_product_value.columns else 0
                                        result_product_value.loc[idx, col] = existing_value + value
                                    else:
                                        result_product_value.loc[idx, col] = value
                        
                        if result_product_value['PRODUCT NAME'].duplicated().any():
                            
                            numeric_cols_result = result_product_value.select_dtypes(include=[np.number]).columns
                            agg_dict = {col: 'sum' for col in numeric_cols_result}
                            result_product_value = result_product_value.groupby('PRODUCT NAME', as_index=False).agg(agg_dict)
                        
                        numeric_cols = result_product_value.select_dtypes(include=[np.number]).columns
                        result_product_value[numeric_cols] = result_product_value[numeric_cols].fillna(0)

                        # Calculate Growth Rate (Gr) and Achievement (Ach) for each month up to March
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            # Initialize Gr and Ach columns if they don't exist
                            if gr_col not in result_product_value.columns:
                                result_product_value[gr_col] = 0.0
                            if ach_col not in result_product_value.columns:
                                result_product_value[ach_col] = 0.0
                            
                            # Calculate Growth Rate: ((Actual - Last Year) / Last Year) * 100
                            if ly_col in result_product_value.columns and actual_col in result_product_value.columns:
                                result_product_value[gr_col] = np.where(
                                    (result_product_value[ly_col] != 0) & (pd.notna(result_product_value[ly_col])) & (pd.notna(result_product_value[actual_col])),
                                    ((result_product_value[actual_col] - result_product_value[ly_col]) / result_product_value[ly_col] * 100).round(2),
                                    0
                                )
                            
                            # Calculate Achievement: (Actual / Budget) * 100
                            if budget_col in result_product_value.columns and actual_col in result_product_value.columns:
                                result_product_value[ach_col] = np.where(
                                    (result_product_value[budget_col] != 0) & (pd.notna(result_product_value[budget_col])) & (pd.notna(result_product_value[actual_col])),
                                    (result_product_value[actual_col] / result_product_value[budget_col] * 100).round(2),
                                    0
                                )

                        exclude_products = ['NORTH TOTAL', 'WEST SALES', 'GRAND TOTAL']
                        mask = ~result_product_value['PRODUCT NAME'].isin(exclude_products + ['TOTAL SALES'])
                        valid_products = result_product_value[mask]
                        
                        total_row = pd.DataFrame({'PRODUCT NAME': ['TOTAL SALES']})
                        for col in numeric_cols:
                            if col in valid_products.columns:
                                total_row[col] = [valid_products[col].sum().round(2)]
                        
                        # Recalculate Gr and Ach for TOTAL SALES row in value data
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            if all(col in total_row.columns for col in [budget_col, actual_col, ly_col]):
                                # Recalculate Growth Rate for totals
                                if total_row[ly_col].iloc[0] != 0:
                                    total_row[gr_col] = [((total_row[actual_col].iloc[0] - total_row[ly_col].iloc[0]) / total_row[ly_col].iloc[0] * 100).round(2)]
                                else:
                                    total_row[gr_col] = [0]
                                
                                # Recalculate Achievement for totals
                                if total_row[budget_col].iloc[0] != 0:
                                    total_row[ach_col] = [(total_row[actual_col].iloc[0] / total_row[budget_col].iloc[0] * 100).round(2)]
                                else:
                                    total_row[ach_col] = [0]
                        
                        result_product_value = pd.concat([valid_products, total_row], ignore_index=True)
                        result_product_value = result_product_value.rename(columns={'PRODUCT NAME': 'SALES in Value'})
                        
                        st.session_state.product_value_data = result_product_value

                        st.subheader(f"Product-wise Budget and Actual Value (Month-wise) [{fiscal_year_str}]")
                        
                        display_df = result_product_value.copy()
                        numeric_display_cols = display_df.select_dtypes(include=[np.number]).columns
                        
                        try:
                            for col in numeric_display_cols:
                                display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "0.00")
                            st.dataframe(display_df, use_container_width=True)
                        except:
                            st.dataframe(result_product_value, use_container_width=True)

                        csv_value = result_product_value.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            "⬇️ Download Budget and Actual Value Data",
                            csv_value,
                            file_name=f"product_budget_actual_value_{selected_sheet_budget}_{fiscal_year_str}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.warning("No budget value columns found.")

                with tab_product_merge:
                    duplicate_info = []
                    if 'product_mt_data' in st.session_state and not st.session_state.product_mt_data.empty:
                        mt_data = st.session_state.product_mt_data.copy()
                        if 'SALES in Tonage' in mt_data.columns:
                            mt_data = mt_data.rename(columns={'SALES in Tonage': 'PRODUCT NAME'})
                        mt_duplicates = mt_data[mt_data['PRODUCT NAME'].duplicated(keep=False)]['PRODUCT NAME'].unique()
                        if len(mt_duplicates) > 0:
                            duplicate_info.append(f"**Tonage data:** {len(mt_duplicates)} products with duplicates: {', '.join(mt_duplicates[:5])}{'...' if len(mt_duplicates) > 5 else ''}")
                    
                    if 'product_value_data' in st.session_state and not st.session_state.product_value_data.empty:
                        value_data = st.session_state.product_value_data.copy()
                        if 'SALES in Value' in value_data.columns:
                            value_data = value_data.rename(columns={'SALES in Value': 'PRODUCT NAME'})
                        value_duplicates = value_data[value_data['PRODUCT NAME'].duplicated(keep=False)]['PRODUCT NAME'].unique()
                        if len(value_duplicates) > 0:
                            duplicate_info.append(f"**Value data:** {len(value_duplicates)} products with duplicates: {', '.join(value_duplicates[:5])}{'...' if len(value_duplicates) > 5 else ''}")
                    
                    if duplicate_info:
                        st.warning("⚠️ **Duplicate Products Detected**")
                        st.info("Products appearing in multiple tabs will be automatically aggregated (summed) during merge:")
                        for info in duplicate_info:
                            st.write(info)
                        st.info("💡 This is normal when products appear across different sales sheets or regions.")
                    
                    if st.session_state.get('uploaded_file_auditor'):
                        try:
                            st.subheader(f"🔀 Merge Preview with Auditor Data [{fiscal_year_str}]")
                            xls_auditor = pd.ExcelFile(st.session_state.uploaded_file_auditor)
                            auditor_sheet_names = xls_auditor.sheet_names

                            product_sheet_auditor = None
                            for sheet in auditor_sheet_names:
                                if 'product' in sheet.lower():
                                    product_sheet_auditor = sheet
                                    break

                            if not product_sheet_auditor:
                                st.error("No product analysis sheet found in auditor file.")
                                st.stop()

                            df_auditor = pd.read_excel(xls_auditor, sheet_name=product_sheet_auditor, header=None)

                            mt_table_headers = [
                                "SALES in Tonage", "SALES IN TONAGE", "Tonage", "TONAGE",
                                "Sales in MT", "SALES IN MT", "SALES in Ton", "Metric Tons", "MT Sales",
                                "Tonage Sales", "Sales Tonage"
                            ]
                            value_table_headers = [
                                "SALES in Value", "SALES IN VALUE", "Sales in Rs", "SALES IN RS",
                                "Value", "VALUE", "Sales Value"
                            ]

                            mt_idx, mt_data_start = extract_tables(df_auditor, mt_table_headers, is_product_analysis=True)
                            value_idx, value_data_start = extract_tables(df_auditor, value_table_headers, is_product_analysis=True)

                            auditor_mt_table = None
                            auditor_value_table = None

                            if mt_idx is not None:
                                if value_idx is not None and value_idx > mt_idx:
                                    mt_table = df_auditor.iloc[mt_data_start:value_idx].dropna(how='all')
                                else:
                                    mt_table = df_auditor.iloc[mt_data_start:].dropna(how='all')
                                
                                # Filter out rows where the first column contains "PRODUCT NAME" (remove header row)
                                mt_table = mt_table[~mt_table.iloc[:, 0].astype(str).str.upper().str.contains('PRODUCT NAME', na=False)]
                                
                                mt_table.columns = df_auditor.iloc[mt_idx]
                                mt_table.columns = rename_columns(mt_table.columns)
                                mt_table = handle_duplicate_columns(mt_table)
                                if mt_table.columns[0] != 'SALES in Tonage':
                                    mt_table = mt_table.rename(columns={mt_table.columns[0]: 'SALES in Tonage'})
                                mt_table['SALES in Tonage'] = mt_table['SALES in Tonage'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                for col in mt_table.columns[1:]:
                                    mt_table[col] = pd.to_numeric(mt_table[col], errors='coerce').fillna(0)
                                auditor_mt_table = mt_table
                            
                            if value_idx is not None:
                                value_table = df_auditor.iloc[value_data_start:].dropna(how='all')
                                
                                # Filter out rows where the first column contains "PRODUCT NAME" (remove header row)
                                value_table = value_table[~value_table.iloc[:, 0].astype(str).str.upper().str.contains('PRODUCT NAME', na=False)]
                                
                                value_table.columns = df_auditor.iloc[value_idx]
                                value_table.columns = rename_columns(value_table.columns)
                                value_table = handle_duplicate_columns(value_table)
                                if value_table.columns[0] != 'SALES in Value':
                                    value_table = value_table.rename(columns={value_table.columns[0]: 'SALES in Value'})
                                value_table['SALES in Value'] = value_table['SALES in Value'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                for col in value_table.columns[1:]:
                                    value_table[col] = pd.to_numeric(value_table[col], errors='coerce').fillna(0)
                                auditor_value_table = value_table

                            merged_mt_data = pd.DataFrame()
                            if (auditor_mt_table is not None and 
                                'product_mt_data' in st.session_state and 
                                not st.session_state.product_mt_data.empty):
                                
                                auditor_mt = auditor_mt_table.copy()
                                generated_mt = st.session_state.product_mt_data.copy()
                                
                                if 'SALES in Tonage' in generated_mt.columns:
                                    generated_mt = generated_mt.rename(columns={'SALES in Tonage': 'PRODUCT NAME'})
                                generated_mt['PRODUCT NAME'] = generated_mt['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                auditor_products = set(auditor_mt['SALES in Tonage'].str.strip().str.upper())
                                generated_products = set(generated_mt['PRODUCT NAME'].str.strip().str.upper())
                                all_products = sorted(auditor_products.union(generated_products))
                                
                                exclude_from_sort = ['TOTAL SALES', 'GRAND TOTAL', 'NORTH TOTAL', 'WEST SALES']
                                regular_products = sorted([p for p in all_products if p not in exclude_from_sort])
                                total_products = [p for p in all_products if p in exclude_from_sort]
                                sorted_products = regular_products + total_products
                                
                                merged_mt_data = pd.DataFrame({'SALES in Tonage': sorted_products})
                                
                                auditor_cols = [col for col in auditor_mt.columns if col != 'SALES in Tonage']
                                generated_cols = [col for col in generated_mt.columns if col != 'PRODUCT NAME']
                                common_cols = list(set(auditor_cols) & set(generated_cols))
                                
                                for col in auditor_cols:
                                    merged_mt_data[col] = 0.0
                                
                                auditor_dict = auditor_mt.set_index('SALES in Tonage').to_dict('index')
                                for product in merged_mt_data['SALES in Tonage']:
                                    if product in auditor_dict:
                                        for col in auditor_cols:
                                            if col in auditor_dict[product]:
                                                idx = merged_mt_data[merged_mt_data['SALES in Tonage'] == product].index[0]
                                                merged_mt_data.loc[idx, col] = auditor_dict[product][col]
                                
                                if common_cols:
                                    if generated_mt['PRODUCT NAME'].duplicated().any():
                                        st.warning("⚠️ Duplicate products detected in generated data. Aggregating values...")
                                        numeric_cols_gen = generated_mt.select_dtypes(include=[np.number]).columns
                                        agg_dict = {col: 'sum' for col in numeric_cols_gen}
                                        for col in generated_mt.columns:
                                            if col not in numeric_cols_gen and col != 'PRODUCT NAME':
                                                agg_dict[col] = 'first'
                                        generated_mt = generated_mt.groupby('PRODUCT NAME', as_index=False).agg(agg_dict)
                                    
                                    generated_dict = generated_mt.set_index('PRODUCT NAME').to_dict('index')
                                    for product in merged_mt_data['SALES in Tonage']:
                                        if product in generated_dict:
                                            for col in common_cols:
                                                if col in generated_dict[product] and pd.notna(generated_dict[product][col]):
                                                    idx = merged_mt_data[merged_mt_data['SALES in Tonage'] == product].index[0]
                                                    merged_mt_data.loc[idx, col] = generated_dict[product][col]
                                
                                if 'TOTAL SALES' in merged_mt_data['SALES in Tonage'].values:
                                    numeric_cols = merged_mt_data.select_dtypes(include=[np.number]).columns
                                    for col in numeric_cols:
                                        sum_value = merged_mt_data[
                                            ~merged_mt_data['SALES in Tonage'].isin(['TOTAL SALES', 'GRAND TOTAL'])
                                        ][col].sum()
                                        merged_mt_data.loc[
                                            merged_mt_data['SALES in Tonage'] == 'TOTAL SALES', col
                                        ] = round(sum_value, 2)
                                
                                for ytd_col, months_list in ytd_periods.items():
                                    valid_months = [month for month in months_list if month in merged_mt_data.columns]
                                    if valid_months:
                                        merged_mt_data[ytd_col] = merged_mt_data[valid_months].sum(axis=1, skipna=True).round(2)
                                
                                ytd_pairs = [
                                    ('Apr to Jun', f'YTD-{fiscal_year_str} (Apr to Jun)Budget', f'YTD-{last_fiscal_year_str} (Apr to Jun)LY', f'Act-YTD-{fiscal_year_str} (Apr to Jun)'),
                                    ('Apr to Sep', f'YTD-{fiscal_year_str} (Apr to Sep)Budget', f'YTD-{last_fiscal_year_str} (Apr to Sep)LY', f'Act-YTD-{fiscal_year_str} (Apr to Sep)'),
                                    ('Apr to Dec', f'YTD-{fiscal_year_str} (Apr to Dec)Budget', f'YTD-{last_fiscal_year_str} (Apr to Dec)LY', f'Act-YTD-{fiscal_year_str} (Apr to Dec)'),
                                    ('Apr to Mar', f'YTD-{fiscal_year_str} (Apr to Mar)Budget', f'YTD-{last_fiscal_year_str} (Apr to Mar)LY', f'Act-YTD-{fiscal_year_str} (Apr to Mar)')
                                ]
                                
                                for period, budget_col, ly_col, act_col in ytd_pairs:
                                    if all(col in merged_mt_data.columns for col in [budget_col, ly_col, act_col]):
                                        merged_mt_data[f'Gr-YTD-{fiscal_year_str} ({period})'] = np.where(
                                            merged_mt_data[ly_col] != 0,
                                            ((merged_mt_data[act_col] - merged_mt_data[ly_col]) / merged_mt_data[ly_col] * 100).round(2),
                                            0
                                        )
                                        merged_mt_data[f'Ach-YTD-{fiscal_year_str} ({period})'] = np.where(
                                            merged_mt_data[budget_col] != 0,
                                            (merged_mt_data[act_col] / merged_mt_data[budget_col] * 100).round(2),
                                            0
                                        )
                                
                                # Store merged tonage data in session state
                                st.session_state.merged_product_mt_data = merged_mt_data
                            
                            merged_value_data = pd.DataFrame()
                            if (auditor_value_table is not None and 
                                'product_value_data' in st.session_state and 
                                not st.session_state.product_value_data.empty):
                                
                                auditor_value = auditor_value_table.copy()
                                generated_value = st.session_state.product_value_data.copy()
                                
                                if 'SALES in Value' in generated_value.columns:
                                    generated_value = generated_value.rename(columns={'SALES in Value': 'PRODUCT NAME'})
                                generated_value['PRODUCT NAME'] = generated_value['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                auditor_products = set(auditor_value['SALES in Value'].str.strip().str.upper())
                                generated_products = set(generated_value['PRODUCT NAME'].str.strip().str.upper())
                                all_products = sorted(auditor_products.union(generated_products))
                                
                                exclude_from_sort = ['TOTAL SALES', 'GRAND TOTAL', 'NORTH TOTAL', 'WEST SALES']
                                regular_products = sorted([p for p in all_products if p not in exclude_from_sort])
                                total_products = [p for p in all_products if p in exclude_from_sort]
                                sorted_products = regular_products + total_products
                                
                                merged_value_data = pd.DataFrame({'SALES in Value': sorted_products})
                                
                                auditor_cols = [col for col in auditor_value.columns if col != 'SALES in Value']
                                generated_cols = [col for col in generated_value.columns if col != 'PRODUCT NAME']
                                common_cols = list(set(auditor_cols) & set(generated_cols))
                                
                                for col in auditor_cols:
                                    merged_value_data[col] = 0.0
                                
                                auditor_dict = auditor_value.set_index('SALES in Value').to_dict('index')
                                for product in merged_value_data['SALES in Value']:
                                    if product in auditor_dict:
                                        for col in auditor_cols:
                                            if col in auditor_dict[product]:
                                                idx = merged_value_data[merged_value_data['SALES in Value'] == product].index[0]
                                                merged_value_data.loc[idx, col] = auditor_dict[product][col]
                                
                                if common_cols:
                                    if generated_value['PRODUCT NAME'].duplicated().any():
                                        st.warning("⚠️ Duplicate products detected in generated value data. Aggregating values...")
                                        numeric_cols_gen = generated_value.select_dtypes(include=[np.number]).columns
                                        agg_dict = {col: 'sum' for col in numeric_cols_gen}
                                        for col in generated_value.columns:
                                            if col not in numeric_cols_gen and col != 'PRODUCT NAME':
                                                agg_dict[col] = 'first'
                                        generated_value = generated_value.groupby('PRODUCT NAME', as_index=False).agg(agg_dict)
                                    
                                    generated_dict = generated_value.set_index('PRODUCT NAME').to_dict('index')
                                    for product in merged_value_data['SALES in Value']:
                                        if product in generated_dict:
                                            for col in common_cols:
                                                if col in generated_dict[product] and pd.notna(generated_dict[product][col]):
                                                    idx = merged_value_data[merged_value_data['SALES in Value'] == product].index[0]
                                                    merged_value_data.loc[idx, col] = generated_dict[product][col]
                                
                                if 'TOTAL SALES' in merged_value_data['SALES in Value'].values:
                                    numeric_cols = merged_value_data.select_dtypes(include=[np.number]).columns
                                    for col in numeric_cols:
                                        sum_value = merged_value_data[
                                            ~merged_value_data['SALES in Value'].isin(['TOTAL SALES', 'GRAND TOTAL'])
                                        ][col].sum()
                                        merged_value_data.loc[
                                            merged_value_data['SALES in Value'] == 'TOTAL SALES', col
                                        ] = round(sum_value, 2)

                                for ytd_col, months_list in ytd_periods.items():
                                    valid_months = [month for month in months_list if month in merged_value_data.columns]
                                    if valid_months:
                                        merged_value_data[ytd_col] = merged_value_data[valid_months].sum(axis=1, skipna=True).round(2)
                                
                                for period, budget_col, ly_col, act_col in ytd_pairs:
                                    if all(col in merged_value_data.columns for col in [budget_col, ly_col, act_col]):
                                        merged_value_data[f'Gr-YTD-{fiscal_year_str} ({period})'] = np.where(
                                            merged_value_data[ly_col] != 0,
                                            ((merged_value_data[act_col] - merged_value_data[ly_col]) / merged_value_data[ly_col] * 100).round(2),
                                            0
                                        )
                                        merged_value_data[f'Ach-YTD-{fiscal_year_str} ({period})'] = np.where(
                                            merged_value_data[budget_col] != 0,
                                            (merged_value_data[act_col] / merged_value_data[budget_col] * 100).round(2),
                                            0
                                        )
                                
                                # Store merged value data in session state
                                st.session_state.merged_product_value_data = merged_value_data

                            if not merged_mt_data.empty:
                                st.subheader(f"Merged Data (SALES in Tonage) [{fiscal_year_str}]")
                                total_products = len(merged_mt_data)
                                summary_rows = len([p for p in merged_mt_data['SALES in Tonage'] if 'TOTAL' in p.upper()])
                                regular_products = total_products - summary_rows
                                
                                
                                if len(merged_mt_data) > 50:
                                    st.warning("⚠️ Large dataset detected. Showing first 50 rows. Download full data using button below.")
                                    display_mt = merged_mt_data.head(50)
                                else:
                                    display_mt = merged_mt_data
                                
                                display_df = display_mt.copy()
                                numeric_cols = display_df.select_dtypes(include=[np.number]).columns
                                for col in numeric_cols:
                                    display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "0.00")
                                
                                st.dataframe(display_df, use_container_width=True)

                            if not merged_value_data.empty:
                                st.subheader(f"Merged Data (SALES in Value) [{fiscal_year_str}]")
                                total_products = len(merged_value_data)
                                summary_rows = len([p for p in merged_value_data['SALES in Value'] if 'TOTAL' in p.upper()])
                                regular_products = total_products - summary_rows
                                
                                
                                if len(merged_value_data) > 50:
                                    st.warning("⚠️ Large dataset detected. Showing first 50 rows. Download full data using button below.")
                                    display_value = merged_value_data.head(50)
                                else:
                                    display_value = merged_value_data
                                
                                display_df = display_value.copy()
                                numeric_cols = display_df.select_dtypes(include=[np.number]).columns
                                for col in numeric_cols:
                                    display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "0.00")
                                
                                st.dataframe(display_df, use_container_width=True)

                            if not merged_mt_data.empty or not merged_value_data.empty:
                                with st.spinner("Preparing Excel file..."):
                                    output = BytesIO()
                                    
                                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                        workbook = writer.book
                                        
                                        title_format = workbook.add_format({
                                            'bold': True, 'align': 'center', 'valign': 'vcenter',
                                            'font_size': 16, 'font_color': '#000000', 'bg_color': '#D9E1F2'
                                        })
                                        header_format = workbook.add_format({
                                            'bold': True, 'text_wrap': True, 'valign': 'top', 'align': 'center',
                                            'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                                        })
                                        num_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
                                        text_format = workbook.add_format({'border': 1, 'valign': 'vcenter'})
                                        total_format = workbook.add_format({
                                            'bold': True, 'num_format': '#,##0.00', 'bg_color': '#E2EFDA', 'border': 1
                                        })

                                        if not merged_mt_data.empty:
                                            merged_mt_data.to_excel(writer, sheet_name='MT_Data', index=False, startrow=3)
                                            worksheet = writer.sheets['MT_Data']
                                            
                                            worksheet.merge_range(0, 0, 0, len(merged_mt_data.columns)-1, 
                                                                f"PRODUCT WISE SALES - TONAGE DATA [{fiscal_year_str}]", title_format)
                                            
                                            for col_num, value in enumerate(merged_mt_data.columns):
                                                worksheet.write(3, col_num, value, header_format)
                                            
                                            for row_num in range(len(merged_mt_data)):
                                                product_name = merged_mt_data.iloc[row_num, 0]
                                                is_total = 'TOTAL' in str(product_name).upper()
                                                
                                                for col_num in range(len(merged_mt_data.columns)):
                                                    if col_num == 0:
                                                        fmt = total_format if is_total else text_format
                                                    else:
                                                        fmt = total_format if is_total else num_format
                                                    value = merged_mt_data.iloc[row_num, col_num]
                                                    if col_num > 0 and isinstance(value, str):
                                                        try:
                                                            value = float(value.replace(',', '')) if value else 0.0
                                                        except (ValueError, TypeError):
                                                            value = value
                                                    worksheet.write(row_num + 4, col_num, value, fmt)
                                            
                                            for i, col in enumerate(merged_mt_data.columns):
                                                if i == 0:
                                                    max_len = max(merged_mt_data[col].astype(str).str.len().max(), len(col)) + 2
                                                    worksheet.set_column(i, i, min(max_len, 30))
                                                else:
                                                    worksheet.set_column(i, i, 12)
                                        
                                        if not merged_value_data.empty:
                                            merged_value_data.to_excel(writer, sheet_name='Value_Data', index=False, startrow=3)
                                            worksheet = writer.sheets['Value_Data']
                                            
                                            worksheet.merge_range(0, 0, 0, len(merged_value_data.columns)-1,
                                                                f"PRODUCT WISE SALES - VALUE DATA [{fiscal_year_str}]", title_format)
                                            
                                            for col_num, value in enumerate(merged_value_data.columns):
                                                worksheet.write(3, col_num, value, header_format)
                                            
                                            for row_num in range(len(merged_value_data)):
                                                product_name = merged_value_data.iloc[row_num, 0]
                                                is_total = 'TOTAL' in str(product_name).upper()
                                                
                                                for col_num in range(len(merged_value_data.columns)):
                                                    if col_num == 0:
                                                        fmt = total_format if is_total else text_format
                                                    else:
                                                        fmt = total_format if is_total else num_format
                                                    value = merged_value_data.iloc[row_num, col_num]
                                                    if col_num > 0 and isinstance(value, str):
                                                        try:
                                                            value = float(value.replace(',', '')) if value else 0.0
                                                        except (ValueError, TypeError):
                                                            value = value
                                                    worksheet.write(row_num + 4, col_num, value, fmt)
                                            
                                            for i, col in enumerate(merged_value_data.columns):
                                                if i == 0:
                                                    max_len = max(merged_value_data[col].astype(str).str.len().max(), len(col)) + 2
                                                    worksheet.set_column(i, i, min(max_len, 30))
                                                else:
                                                    worksheet.set_column(i, i, 12)
                                
                                excel_data = output.getvalue()
                                
                                st.download_button(
                                    label="⬇️ Download Complete Merged Product Data",
                                    data=excel_data,
                                    file_name=f"complete_merged_product_data_{fiscal_year_str}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="product_merge_download_optimized"
                                )
                                
                                
                        except Exception as e:
                            st.error(f"Error during merge process: {str(e)}")
                    else:
                        st.info("ℹ️ Upload auditor file to see merge preview")
        
                        
            else:
                st.warning("Required column 'PRODUCT NAME' not found in budget data.")
                
        except Exception as e:
            st.error("An error occurred while processing the data. Please check your input files and try again.")
            st.error(f"Error details: {str(e)}")
            
    else: 
        st.info("ℹ️ Please upload both Sales and Budget files and select appropriate sheets.")

with tab5:
    st.header("📊 TS-PW Data Analysis (NORTH)")
    
    # Get current date and determine fiscal year
    current_date = datetime.now()
    current_year = current_date.year
    if current_date.month >= 4:
        fiscal_year_start = current_year
        fiscal_year_end = current_year + 1
    else:
        fiscal_year_start = current_year - 1
        fiscal_year_end = current_year
    fiscal_year_str = f"{str(fiscal_year_start)[-2:]}-{str(fiscal_year_end)[-2:]}"
    last_fiscal_year_start = fiscal_year_start - 1
    last_fiscal_year_end = fiscal_year_end - 1
    last_fiscal_year_str = f"{str(last_fiscal_year_start)[-2:]}-{str(last_fiscal_year_end)[-2:]}"
    
    # Define months for April to March
    months = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
    
    selected_sheet_name = None
    if st.session_state.get('uploaded_file_budget'):
        xls_budget = pd.ExcelFile(st.session_state.uploaded_file_budget)
        budget_sheet_names = xls_budget.sheet_names
        if budget_sheet_names:
            selected_sheet_name = st.session_state.get('budget_sheet_selection', budget_sheet_names[0])
    
    selected_sheet_last_year = st.session_state.get('last_year_sheet')
    
    if (st.session_state.get('uploaded_file_sales') and 
        st.session_state.get('uploaded_file_budget') and 
        'selected_sheets_sales' in st.session_state and 
        selected_sheet_name):
        try:
            # Process budget data
            xls_budget = pd.ExcelFile(st.session_state.uploaded_file_budget)
            df_budget = pd.read_excel(xls_budget, sheet_name=selected_sheet_name)
            df_budget.columns = df_budget.columns.str.strip()
            df_budget = df_budget.dropna(how='all').reset_index(drop=True)

            budget_data = process_budget_data_product_region(df_budget, group_type='product_region')
            
            if 'Region' in budget_data.columns:
                budget_data = budget_data[budget_data['Region'].str.strip().str.upper() == 'NORTH']
                if budget_data.empty:
                    st.error("No data found for NORTH region.")
                    st.stop()

            st.session_state.ts_pw_budget_data = budget_data

            required_cols = ['PRODUCT NAME']
            if all(col in budget_data.columns for col in required_cols):
                
                mt_cols = [col for col in budget_data.columns if col.endswith('_MT')]
                value_cols = [col for col in budget_data.columns if col.endswith('_Value')]

                actual_mt_last = None
                actual_value_last = None

                # Process Last Year Actual Data
                if st.session_state.get('uploaded_file_last_year') and selected_sheet_last_year:
                    try:
                        xls_last_year = pd.ExcelFile(st.session_state.uploaded_file_last_year)
                        df_last_year = pd.read_excel(xls_last_year, sheet_name=selected_sheet_last_year, header=0)
                        
                        if isinstance(df_last_year.columns, pd.MultiIndex):
                            df_last_year.columns = ['_'.join(col).strip() for col in df_last_year.columns.values]
                        df_last_year = handle_duplicate_columns(df_last_year)
                        
                        region_col = find_column(df_last_year, ['Region', 'Area', 'Zone'], case_sensitive=False)
                        product_col = find_column(df_last_year, ['Type (Make)', 'Type(Make)', 'Product Group', 'Product'], case_sensitive=False)
                        date_col = find_column(df_last_year, ['Date', 'Month Format', 'Month'], case_sensitive=False)
                        qty_col = find_column(df_last_year, ['Actual Quantity', 'Acutal Quantity', 'Quantity'], case_sensitive=False)
                        amount_col = find_column(df_last_year, ['Amount', 'Value', 'Sales Value'], case_sensitive=False)
                        
                        rename_dict = {}
                        if date_col:
                            rename_dict[date_col] = 'Month Format'
                        if product_col:
                            rename_dict[product_col] = 'Product Group'
                        if qty_col:
                            rename_dict[qty_col] = 'Actual Quantity'
                        if amount_col:
                            rename_dict[amount_col] = 'Amount'
                        if region_col:
                            rename_dict[region_col] = 'Region'
                        
                        df_last_year = df_last_year.rename(columns=rename_dict)
                        
                        region_col = "Region" if "Region" in df_last_year.columns else None
                        product_col = "Product Group" if "Product Group" in df_last_year.columns else None
                        date_col = "Month Format" if "Month Format" in df_last_year.columns else None
                        qty_col = "Actual Quantity" if "Actual Quantity" in df_last_year.columns else None
                        amount_col = "Amount" if "Amount" in df_last_year.columns else None
                        
                        if product_col and date_col:
                            if region_col:
                                df_last_year = df_last_year[df_last_year[region_col].str.strip().str.upper() == 'NORTH']
                            
                            if not df_last_year.empty:
                                if qty_col:
                                    try:
                                        df_last_year_qty = df_last_year.copy()
                                        df_last_year_qty['Actual Quantity'] = pd.to_numeric(df_last_year_qty[qty_col], errors='coerce')
                                        
                                        if pd.api.types.is_datetime64_any_dtype(df_last_year_qty[date_col]):
                                            df_last_year_qty['Month'] = pd.to_datetime(df_last_year_qty[date_col]).dt.strftime('%b')
                                        else:
                                            df_last_year_qty['Month'] = df_last_year_qty[date_col].str.strip().str[:3]
                                        
                                        last_year_agg = df_last_year_qty.groupby(['Product Group', 'Month'])['Actual Quantity'].sum().reset_index()
                                        last_year_agg.columns = ['PRODUCT NAME', 'Month', 'LY_Actual']
                                        last_year_agg['PRODUCT NAME'] = last_year_agg['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                        last_year_agg['Month_Year'] = 'LY-' + last_year_agg['Month'] + '-' + str(last_fiscal_year_start)[-2:]
                                        for month in months[:9]:  # Apr to Dec
                                            last_year_agg.loc[last_year_agg['Month'] == month, 'Month_Year'] = f'LY-{month}-{str(last_fiscal_year_start)[-2:]}'
                                        for month in months[9:]:  # Jan to Mar
                                            last_year_agg.loc[last_year_agg['Month'] == month, 'Month_Year'] = f'LY-{month}-{str(last_fiscal_year_end)[-2:]}'
                                        
                                        actual_mt_last = last_year_agg.pivot_table(
                                            index='PRODUCT NAME',
                                            columns='Month_Year',
                                            values='LY_Actual',
                                            aggfunc='sum'
                                        ).reset_index().fillna(0)
                                        actual_mt_last['Region'] = 'NORTH'
                                    except Exception as e:
                                        st.error(f"Error in last year quantity grouping: {str(e)}")
                                        actual_mt_last = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                                
                                if amount_col:
                                    try:
                                        df_last_year_val = df_last_year.copy()
                                        df_last_year_val[amount_col] = pd.to_numeric(df_last_year_val[amount_col], errors='coerce')
                                        
                                        if pd.api.types.is_datetime64_any_dtype(df_last_year_val[date_col]):
                                            df_last_year_val['Month'] = pd.to_datetime(df_last_year_val[date_col]).dt.strftime('%b')
                                        else:
                                            df_last_year_val['Month'] = df_last_year_val[date_col].str.strip().str[:3]
                                        
                                        last_year_value_agg = df_last_year_val.groupby(['Product Group', 'Month'])['Amount'].sum().reset_index()
                                        last_year_value_agg.columns = ['PRODUCT NAME', 'Month', 'LY_Actual']
                                        last_year_value_agg['PRODUCT NAME'] = last_year_value_agg['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                        last_year_value_agg['Month_Year'] = 'LY-' + last_year_value_agg['Month'] + '-' + str(last_fiscal_year_start)[-2:]
                                        for month in months[:9]:  # Apr to Dec
                                            last_year_value_agg.loc[last_year_value_agg['Month'] == month, 'Month_Year'] = f'LY-{month}-{str(last_fiscal_year_start)[-2:]}'
                                        for month in months[9:]:  # Jan to Mar
                                            last_year_value_agg.loc[last_year_value_agg['Month'] == month, 'Month_Year'] = f'LY-{month}-{str(last_fiscal_year_end)[-2:]}'
                                        
                                        actual_value_last = last_year_value_agg.pivot_table(
                                            index='PRODUCT NAME',
                                            columns='Month_Year',
                                            values='LY_Actual',
                                            aggfunc='sum'
                                        ).reset_index().fillna(0)
                                        actual_value_last['Region'] = 'NORTH'
                                    except Exception as e:
                                        st.error(f"Error in last year value grouping: {str(e)}")
                                        actual_value_last = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                            else:
                                st.warning("No NORTH region data found in last year file.")
                        else:
                            st.warning(f"Missing required columns in '{selected_sheet_last_year}'.")
                    except Exception as e:
                        st.error(f"Error processing last year data: {e}")

                # Define YTD periods dynamically
                ytd_periods = {}
                # Budget YTD periods
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Jun)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Sep)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Dec)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Mar)Budget'] = (
                    [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'Budget-{month}-{str(fiscal_year_end)[-2:]}' for month in months[9:]]
                )
                # Last Year YTD periods
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Jun)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Sep)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Dec)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Mar)LY'] = (
                    [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'LY-{month}-{str(last_fiscal_year_end)[-2:]}' for month in months[9:]]
                )
                # Actual YTD periods
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Jun)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Sep)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Dec)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Mar)'] = (
                    [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'Act-{month}-{str(fiscal_year_end)[-2:]}' for month in months[9:]]
                )

                if 'ts_pw_analysis_data' not in st.session_state:
                    st.session_state.ts_pw_analysis_data = pd.DataFrame()
                if 'ts_pw_value_data' not in st.session_state:
                    st.session_state.ts_pw_value_data = pd.DataFrame()
                if 'actual_ts_pw_mt_data' not in st.session_state:
                    st.session_state.actual_ts_pw_mt_data = pd.DataFrame()
                if 'actual_ts_pw_value_data' not in st.session_state:
                    st.session_state.actual_ts_pw_value_data = pd.DataFrame()

                result_ts_pw_mt = pd.DataFrame()
                result_ts_pw_value = pd.DataFrame()

                tab_ts_pw_mt, tab_ts_pw_value, tab_ts_pw_merge = st.tabs(
                    ["SALES in Tonage", "SALES in Value", "Merge Preview"]
                )

                with tab_ts_pw_mt:
                    if mt_cols:
                        month_cols = sorted(set(col.replace('_MT', '') for col in mt_cols if not col.endswith(f'-{last_fiscal_year_start}_MT')))
                        last_year_cols = sorted(set(col.replace('_MT', '') for col in mt_cols if col.endswith(f'-{last_fiscal_year_start}_MT')))
                        
                        result_ts_pw_mt = pd.DataFrame()
                        for month_col in month_cols:
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_MT']].rename(columns={f'{month_col}_MT': month_col})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[month_col].sum()
                            if result_ts_pw_mt.empty:
                                result_ts_pw_mt = temp_df
                            else:
                                result_ts_pw_mt = result_ts_pw_mt.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='outer')
                        
                        for month_col in last_year_cols:
                            orig_month = month_col.replace(f'-{last_fiscal_year_start}', '')
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_MT']].rename(columns={f'{month_col}_MT': f'LY-{orig_month}'})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[f'LY-{orig_month}'].sum()
                            result_ts_pw_mt = result_ts_pw_mt.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        result_ts_pw_mt['PRODUCT NAME'] = result_ts_pw_mt['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                        result_ts_pw_mt['Region'] = result_ts_pw_mt['Region'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')

                        # Process Current Year Actual Sales Data
                        actual_mt_current = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                        if st.session_state.get('uploaded_file_sales') and 'selected_sheets_sales' in st.session_state:
                            all_sales_data = []
                            xls_sales = pd.ExcelFile(st.session_state.uploaded_file_sales)
                            
                            for sheet_name in st.session_state.selected_sheets_sales:
                                try:
                                    df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                                    if isinstance(df_sales.columns, pd.MultiIndex):
                                        df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                                    df_sales = handle_duplicate_columns(df_sales)
                                    
                                    region_col = find_column(df_sales, ['Region', 'Area', 'Zone'], case_sensitive=False)
                                    product_col = find_column(df_sales, ['Type (Make)', 'Type(Make)'], case_sensitive=False)
                                    date_col = find_column(df_sales, ['Date', 'Month Format', 'Month'], case_sensitive=False)
                                    qty_col = find_column(df_sales, ['Actual Quantity', 'Acutal Quantity'], case_sensitive=False)
                                    
                                    rename_dict = {}
                                    if date_col:
                                        rename_dict[date_col] = 'Month Format'
                                    if product_col:
                                        rename_dict[product_col] = 'Product Group'
                                    if qty_col:
                                        rename_dict[qty_col] = 'Actual Quantity'
                                    if region_col:
                                        rename_dict[region_col] = 'Region'
                                    df_sales = df_sales.rename(columns=rename_dict)
                                    
                                    region_col = "Region" if "Region" in df_sales.columns else None
                                    product_col = "Product Group" if "Product Group" in df_sales.columns else None
                                    date_col = "Month Format" if "Month Format" in df_sales.columns else None
                                    qty_col = "Actual Quantity" if "Actual Quantity" in df_sales.columns else None
                                    
                                    if product_col and date_col and qty_col:
                                        if region_col:
                                            df_sales = df_sales[df_sales[region_col].str.strip().str.upper() == 'NORTH']
                                        
                                        if not df_sales.empty:
                                            df_sales['Actual Quantity'] = pd.to_numeric(df_sales[qty_col], errors='coerce')
                                            
                                            if pd.api.types.is_datetime64_any_dtype(df_sales[date_col]):
                                                df_sales['Month'] = pd.to_datetime(df_sales[date_col]).dt.strftime('%b')
                                            else:
                                                df_sales['Month'] = df_sales[date_col].astype(str).str.strip().str[:3]
                                            
                                            df_sales = df_sales.dropna(subset=['Actual Quantity', 'Month'])
                                            df_sales = df_sales[df_sales['Actual Quantity'] != 0]
                                            
                                            all_sales_data.append(df_sales)
                                except Exception as e:
                                    st.error(f"Error processing sales sheet {sheet_name}: {e}")
                            
                            if all_sales_data:
                                combined_sales = pd.concat(all_sales_data, ignore_index=True)
                                
                                try:
                                    sales_agg_current = combined_sales.groupby(['Product Group', 'Month'])['Actual Quantity'].sum().reset_index()
                                    sales_agg_current.columns = ['PRODUCT NAME', 'Month', 'Actual']
                                    sales_agg_current['PRODUCT NAME'] = sales_agg_current['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    sales_agg_current['Month_Year'] = sales_agg_current['Month'].apply(
                                        lambda x: f'Act-{x}-{str(fiscal_year_start)[-2:]}' if x in months[:9] else f'Act-{x}-{str(fiscal_year_end)[-2:]}'
                                    )
                                    
                                    actual_mt_current = sales_agg_current.pivot_table(
                                        index='PRODUCT NAME',
                                        columns='Month_Year',
                                        values='Actual',
                                        aggfunc='sum'
                                    ).reset_index().fillna(0)
                                    actual_mt_current['Region'] = 'NORTH'
                                except Exception as e:
                                    st.error(f"Error in sales quantity grouping: {e}")
                        
                        # Merge actual data properly
                        actual_mt = pd.DataFrame({'PRODUCT NAME': result_ts_pw_mt['PRODUCT NAME'], 'Region': 'NORTH'})
                        
                        if not actual_mt_current.empty and 'PRODUCT NAME' in actual_mt_current.columns:
                            actual_mt = actual_mt.merge(actual_mt_current, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        if actual_mt_last is not None and not actual_mt_last.empty:
                            actual_mt = actual_mt.merge(actual_mt_last, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        st.session_state.actual_ts_pw_mt_data = actual_mt
                        
                        result_ts_pw_mt = result_ts_pw_mt.merge(actual_mt, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        # Calculate Growth and Achievement columns
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            if gr_col not in result_ts_pw_mt.columns:
                                result_ts_pw_mt[gr_col] = np.nan
                            if ach_col not in result_ts_pw_mt.columns:
                                result_ts_pw_mt[ach_col] = np.nan
                            
                            if actual_col in result_ts_pw_mt.columns and ly_col in result_ts_pw_mt.columns:
                                result_ts_pw_mt[gr_col] = (
                                    (result_ts_pw_mt[actual_col] - result_ts_pw_mt[ly_col]) /
                                    result_ts_pw_mt[ly_col].replace(0, np.nan) * 100
                                ).round(2)
                            
                            if budget_col in result_ts_pw_mt.columns and actual_col in result_ts_pw_mt.columns:
                                result_ts_pw_mt[ach_col] = (
                                    (result_ts_pw_mt[actual_col] /
                                     result_ts_pw_mt[budget_col].replace(0, np.nan)) * 100
                                ).round(2)

                        # Calculate YTD columns
                        for ytd_period, period_cols in ytd_periods.items():
                            valid_cols = [col for col in period_cols if col in result_ts_pw_mt.columns]
                            if valid_cols:
                                result_ts_pw_mt[ytd_period] = result_ts_pw_mt[valid_cols].sum(axis=1).round(2)

                        # Calculate total row
                        exclude_products = ['NORTH TOTAL', 'WEST SALES', 'GRAND TOTAL']
                        valid_products = result_ts_pw_mt[~result_ts_pw_mt['PRODUCT NAME'].isin(exclude_products)]
                        grand_total_row = {'PRODUCT NAME': 'TOTAL SALES', 'Region': 'NORTH'}
                        numeric_cols = valid_products.select_dtypes(include=[np.number]).columns
                        for col in numeric_cols:
                            grand_total_row[col] = valid_products[col].sum().round(2)
                        
                        result_ts_pw_mt = result_ts_pw_mt[result_ts_pw_mt['PRODUCT NAME'] != 'TOTAL SALES']
                        result_ts_pw_mt = pd.concat([result_ts_pw_mt, pd.DataFrame([grand_total_row])], ignore_index=True)
                        
                        result_ts_pw_mt = result_ts_pw_mt.rename(columns={'PRODUCT NAME': 'SALES in Tonage'})
                        st.session_state.ts_pw_analysis_data = result_ts_pw_mt
                            
                        try:
                            styled_df = safe_format_dataframe(result_ts_pw_mt)
                            numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                            formatter = {col: "{:,.2f}" for col in numeric_cols}
                            st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                        except Exception as e:
                            st.error(f"Error displaying tonage dataframe: {e}")
                            st.dataframe(result_ts_pw_mt, use_container_width=True)

                        if not result_ts_pw_mt.empty:
                            output = BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                result_ts_pw_mt.to_excel(writer, sheet_name='TS_PW_MT_Analysis', index=False)
                                workbook = writer.book
                                worksheet = writer.sheets['TS_PW_MT_Analysis']
                                header_format = workbook.add_format({
                                    'bold': True, 'text_wrap': True, 'valign': 'top',
                                    'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                                })
                                num_format = workbook.add_format({'num_format': '0.00'})
                                
                                for col_num, value in enumerate(result_ts_pw_mt.columns.values):
                                    worksheet.write(0, col_num, value, header_format)
                                for col in result_ts_pw_mt.select_dtypes(include=[np.number]).columns:
                                    col_idx = result_ts_pw_mt.columns.get_loc(col)
                                    worksheet.set_column(col_idx, col_idx, None, num_format)
                                for i, col in enumerate(result_ts_pw_mt.columns):
                                    max_len = max((result_ts_pw_mt[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, max_len)
                            
                            excel_data = output.getvalue()
                            st.download_button(
                                label="⬇️ Download Budget Tonage as Excel",
                                data=excel_data,
                                file_name=f"ts_pw_monthly_budget_tonage_north_{fiscal_year_str}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_ts_pw_mt_excel"
                            )
                    else:
                        st.warning("No budget tonage columns found.")

                with tab_ts_pw_value:
                    if value_cols:
                        month_cols = sorted(set(col.replace('_Value', '') for col in value_cols if not col.endswith(f'-{last_fiscal_year_start}_Value')))
                        last_year_cols = sorted(set(col.replace('_Value', '') for col in value_cols if col.endswith(f'-{last_fiscal_year_start}_Value')))
                        
                        result_ts_pw_value = pd.DataFrame()
                        for month_col in month_cols:
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_Value']].rename(columns={f'{month_col}_Value': month_col})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[month_col].sum()
                            if result_ts_pw_value.empty:
                                result_ts_pw_value = temp_df
                            else:
                                result_ts_pw_value = result_ts_pw_value.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='outer')
                        
                        for month_col in last_year_cols:
                            orig_month = month_col.replace(f'-{last_fiscal_year_start}', '')
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_Value']].rename(columns={f'{month_col}_Value': f'LY-{orig_month}'})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[f'LY-{orig_month}'].sum()
                            result_ts_pw_value = result_ts_pw_value.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        result_ts_pw_value['PRODUCT NAME'] = result_ts_pw_value['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                        result_ts_pw_value['Region'] = result_ts_pw_value['Region'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')

                        # Process Current Year Actual Sales Value Data
                        actual_value_current = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                        if st.session_state.get('uploaded_file_sales') and 'selected_sheets_sales' in st.session_state:
                            all_sales_data = []
                            xls_sales = pd.ExcelFile(st.session_state.uploaded_file_sales)
                            
                            for sheet_name in st.session_state.selected_sheets_sales:
                                try:
                                    df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                                    if isinstance(df_sales.columns, pd.MultiIndex):
                                        df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                                    df_sales = handle_duplicate_columns(df_sales)
                                    
                                    region_col = find_column(df_sales, ['Region', 'Area', 'Zone'], case_sensitive=False)
                                    product_col = find_column(df_sales, ['Type (Make)', 'Type(Make)'], case_sensitive=False)
                                    date_col = find_column(df_sales, ['Date', 'Month Format', 'Month'], case_sensitive=False)
                                    value_col = find_column(df_sales, ['Value', 'Amount', 'Sales Value'], case_sensitive=False)
                                    
                                    rename_dict = {}
                                    if date_col:
                                        rename_dict[date_col] = 'Month Format'
                                    if product_col:
                                        rename_dict[product_col] = 'Product Group'
                                    if value_col:
                                        rename_dict[value_col] = 'Value'
                                    if region_col:
                                        rename_dict[region_col] = 'Region'
                                    df_sales = df_sales.rename(columns=rename_dict)
                                    
                                    region_col = "Region" if "Region" in df_sales.columns else None
                                    product_col = "Product Group" if "Product Group" in df_sales.columns else None
                                    date_col = "Month Format" if "Month Format" in df_sales.columns else None
                                    value_col = "Value" if "Value" in df_sales.columns else None
                                    
                                    if product_col and date_col and value_col:
                                        if region_col:
                                            df_sales = df_sales[df_sales[region_col].str.strip().str.upper() == 'NORTH']
                                        
                                        if not df_sales.empty:
                                            df_sales[value_col] = pd.to_numeric(df_sales[value_col], errors='coerce')
                                            
                                            if pd.api.types.is_datetime64_any_dtype(df_sales[date_col]):
                                                df_sales['Month'] = pd.to_datetime(df_sales[date_col]).dt.strftime('%b')
                                            else:
                                                df_sales['Month'] = df_sales[date_col].astype(str).str.strip().str[:3]
                                            
                                            df_sales = df_sales.dropna(subset=[value_col, 'Month'])
                                            df_sales = df_sales[df_sales[value_col] != 0]
                                            
                                            all_sales_data.append(df_sales)
                                except Exception as e:
                                    st.error(f"Error processing sales value sheet {sheet_name}: {e}")
                            
                            if all_sales_data:
                                combined_sales = pd.concat(all_sales_data, ignore_index=True)
                                
                                try:
                                    sales_agg_current = combined_sales.groupby(['Product Group', 'Month'])['Value'].sum().reset_index()
                                    sales_agg_current.columns = ['PRODUCT NAME', 'Month', 'Actual']
                                    sales_agg_current['PRODUCT NAME'] = sales_agg_current['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    sales_agg_current['Month_Year'] = sales_agg_current['Month'].apply(
                                        lambda x: f'Act-{x}-{str(fiscal_year_start)[-2:]}' if x in months[:9] else f'Act-{x}-{str(fiscal_year_end)[-2:]}'
                                    )
                                    
                                    actual_value_current = sales_agg_current.pivot_table(
                                        index='PRODUCT NAME',
                                        columns='Month_Year',
                                        values='Actual',
                                        aggfunc='sum'
                                    ).reset_index().fillna(0)
                                    actual_value_current['Region'] = 'NORTH'
                                except Exception as e:
                                    st.error(f"Error in sales value grouping: {e}")

                        # Merge actual value data properly
                        actual_value = pd.DataFrame({'PRODUCT NAME': result_ts_pw_value['PRODUCT NAME'], 'Region': 'NORTH'})
                        
                        if not actual_value_current.empty and 'PRODUCT NAME' in actual_value_current.columns:
                            actual_value = actual_value.merge(actual_value_current, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        if actual_value_last is not None and not actual_value_last.empty:
                            actual_value = actual_value.merge(actual_value_last, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        st.session_state.actual_ts_pw_value_data = actual_value
                        
                        result_ts_pw_value = result_ts_pw_value.merge(actual_value, on=['PRODUCT NAME', 'Region'], how='left')

                        # Calculate Growth and Achievement columns for Value
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            if gr_col not in result_ts_pw_value.columns:
                                result_ts_pw_value[gr_col] = np.nan
                            if ach_col not in result_ts_pw_value.columns:
                                result_ts_pw_value[ach_col] = np.nan
                            
                            if actual_col in result_ts_pw_value.columns and ly_col in result_ts_pw_value.columns:
                                result_ts_pw_value[gr_col] = (
                                    (result_ts_pw_value[actual_col] - result_ts_pw_value[ly_col]) /
                                    result_ts_pw_value[ly_col].replace(0, np.nan) * 100
                                ).round(2)
                            
                            if budget_col in result_ts_pw_value.columns and actual_col in result_ts_pw_value.columns:
                                result_ts_pw_value[ach_col] = (
                                    result_ts_pw_value[actual_col] /
                                    result_ts_pw_value[budget_col].replace(0, np.nan) * 100
                                ).round(2)

                        # Calculate YTD columns for Value
                        for ytd_period, period_cols in ytd_periods.items():
                            valid_cols = [col for col in period_cols if col in result_ts_pw_value.columns]
                            if valid_cols:
                                result_ts_pw_value[ytd_period] = result_ts_pw_value[valid_cols].sum(axis=1).round(2)

                        # Calculate total row for Value
                        exclude_products = ['NORTH TOTAL', 'WEST SALES', 'GRAND TOTAL']
                        valid_products = result_ts_pw_value[~result_ts_pw_value['PRODUCT NAME'].isin(exclude_products)]
                        grand_total_row = {'PRODUCT NAME': 'TOTAL SALES', 'Region': 'NORTH'}
                        numeric_cols = valid_products.select_dtypes(include=[np.number]).columns
                        for col in numeric_cols:
                            grand_total_row[col] = valid_products[col].sum().round(2)
                        
                        result_ts_pw_value = result_ts_pw_value[result_ts_pw_value['PRODUCT NAME'] != 'TOTAL SALES']
                        result_ts_pw_value = pd.concat([result_ts_pw_value, pd.DataFrame([grand_total_row])], ignore_index=True)
                        
                        result_ts_pw_value = result_ts_pw_value.rename(columns={'PRODUCT NAME': 'SALES in Value'})
                        st.session_state.ts_pw_value_data = result_ts_pw_value

                        st.subheader(f"TS-PW Monthly Budget and Actual Value (NORTH) [{fiscal_year_str}]")
                        
                        try:
                            styled_df = safe_format_dataframe(result_ts_pw_value)
                            numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                            formatter = {col: "{:,.2f}" for col in numeric_cols}
                            st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                        except Exception as e:
                            st.error(f"Error displaying value dataframe: {e}")
                            st.dataframe(result_ts_pw_value, use_container_width=True)

                        if not result_ts_pw_value.empty:
                            output = BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                result_ts_pw_value.to_excel(writer, sheet_name='TS_PW_Value_Analysis', index=False)
                                workbook = writer.book
                                worksheet = writer.sheets['TS_PW_Value_Analysis']
                                header_format = workbook.add_format({
                                    'bold': True, 'text_wrap': True, 'valign': 'top',
                                    'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                                })
                                num_format = workbook.add_format({'num_format': '0.00'})
                                
                                for col_num, value in enumerate(result_ts_pw_value.columns.values):
                                    worksheet.write(0, col_num, value, header_format)
                                for col in result_ts_pw_value.select_dtypes(include=[np.number]).columns:
                                    col_idx = result_ts_pw_value.columns.get_loc(col)
                                    worksheet.set_column(col_idx, col_idx, None, num_format)
                                for i, col in enumerate(result_ts_pw_value.columns):
                                    max_len = max((result_ts_pw_value[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, max_len)
                            
                            excel_data = output.getvalue()
                            st.download_button(
                                label="⬇️ Download Budget Value as Excel",
                                data=excel_data,
                                file_name=f"ts_pw_monthly_budget_value_north_{fiscal_year_str}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_ts_pw_value_excel"
                            )
                    else:
                        st.warning("No budget value columns found.")

                with tab_ts_pw_merge:
                    if st.session_state.get('uploaded_file_auditor'):
                        try:
                            st.subheader(f"🔀 Merge Preview with Auditor Data (NORTH) [{fiscal_year_str}]")
                            xls_auditor = pd.ExcelFile(st.session_state.uploaded_file_auditor)
                            auditor_sheet_names = xls_auditor.sheet_names

                            ts_pw_sheet = None
                            for sheet in auditor_sheet_names:
                                if any(term.lower() in sheet.lower() for term in ['ts-pw', 'tspw', 'north']):
                                    ts_pw_sheet = sheet
                                    break

                            if not ts_pw_sheet:
                                st.error("No TS-PW analysis sheet found in auditor file.")
                                st.stop()

                            df_auditor = pd.read_excel(xls_auditor, sheet_name=ts_pw_sheet, header=None)

                            mt_table_headers = [
                                "SALES in Tonage", "SALES IN TONAGE", "Tonage", "TONAGE",
                                "Sales in MT", "SALES IN MT", "SALES in Ton", "Metric Tons", "MT Sales",
                                "Tonage Sales", "Sales Tonage"
                            ]
                            value_table_headers = [
                                "SALES in Value", "SALES IN VALUE", "Sales in Rs", "SALES IN RS",
                                "Value", "VALUE", "Sales Value"
                            ]

                            mt_idx, mt_data_start = extract_tables(df_auditor, mt_table_headers, is_product_analysis=True)
                            value_idx, value_data_start = extract_tables(df_auditor, value_table_headers, is_product_analysis=True)

                            auditor_ts_pw_mt_table = None
                            auditor_ts_pw_value_table = None
                            
                            if mt_idx is not None and mt_data_start is not None:
                                if value_idx is not None and value_idx > mt_idx:
                                    mt_table = df_auditor.iloc[mt_data_start:value_idx].dropna(how='all')
                                else:
                                    mt_table = df_auditor.iloc[mt_data_start:].dropna(how='all')
                                
                                if not mt_table.empty:
                                    mt_table.columns = df_auditor.iloc[mt_idx]
                                    mt_table.columns = rename_columns(mt_table.columns)
                                    mt_table = handle_duplicate_columns(mt_table)
                                    if mt_table.columns[0] != 'SALES in Tonage':
                                        mt_table = mt_table.rename(columns={mt_table.columns[0]: 'SALES in Tonage'})
                                    
                                    mt_table['SALES in Tonage'] = mt_table['SALES in Tonage'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    mt_table = mt_table[~mt_table['SALES in Tonage'].isin(['PRODUCT NAME', '', 'NAN'])]
                                    mt_table = mt_table.dropna(subset=['SALES in Tonage'])
                                    
                                    for col in mt_table.columns[1:]:
                                        mt_table[col] = pd.to_numeric(mt_table[col], errors='coerce').fillna(0)
                                    numeric_cols = mt_table.select_dtypes(include=[np.number]).columns
                                    mt_table[numeric_cols] = mt_table[numeric_cols].astype(float).round(2)
                                    auditor_ts_pw_mt_table = mt_table
                            
                            if value_idx is not None and value_data_start is not None:
                                value_table = df_auditor.iloc[value_data_start:].dropna(how='all')
                                
                                if not value_table.empty:
                                    value_table.columns = df_auditor.iloc[value_idx]
                                    value_table.columns = rename_columns(value_table.columns)
                                    value_table = handle_duplicate_columns(value_table)
                                    if value_table.columns[0] != 'SALES in Value':
                                        value_table = value_table.rename(columns={value_table.columns[0]: 'SALES in Value'})
                                    
                                    value_table['SALES in Value'] = value_table['SALES in Value'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    value_table = value_table[~value_table['SALES in Value'].isin(['PRODUCT NAME', '', 'NAN'])]
                                    value_table = value_table[value_table['SALES in Value'].str.strip() != '']
                                    
                                    for col in value_table.columns[1:]:
                                        value_table[col] = pd.to_numeric(value_table[col], errors='coerce').fillna(0)
                                    numeric_cols = value_table.select_dtypes(include=[np.number]).columns
                                    value_table[numeric_cols] = value_table[numeric_cols].astype(float).round(2)
                                    auditor_ts_pw_value_table = value_table

                            def calculate_ytd_growth_achievement(data, product_col_name):
                                ytd_quarterlies = [
                                    ('Apr to Jun', months[:3]),
                                    ('Apr to Sep', months[:6]),
                                    ('Apr to Dec', months[:9]),
                                    ('Apr to Mar', months)
                                ]
                                
                                for quarter_name, quarter_months in ytd_quarterlies:
                                    actual_ytd_col = f'Act-YTD-{fiscal_year_str} ({quarter_name})'
                                    ly_ytd_col = f'YTD-{last_fiscal_year_str} ({quarter_name})LY'
                                    gr_ytd_col = f'Gr-YTD-{fiscal_year_str} ({quarter_name})'
                                    
                                    if actual_ytd_col in data.columns and ly_ytd_col in data.columns:
                                        data[gr_ytd_col] = (
                                            (data[actual_ytd_col] - data[ly_ytd_col]) /
                                            data[ly_ytd_col].replace(0, np.nan) * 100
                                        ).round(2)
                                    
                                    budget_ytd_col = f'YTD-{fiscal_year_str} ({quarter_name})Budget'
                                    ach_ytd_col = f'Ach-YTD-{fiscal_year_str} ({quarter_name})'
                                    
                                    if actual_ytd_col in data.columns and budget_ytd_col in data.columns:
                                        data[ach_ytd_col] = (
                                            data[actual_ytd_col] /
                                            data[budget_ytd_col].replace(0, np.nan) * 100
                                        ).round(2)
                                
                                return data

                            merged_mt_data = pd.DataFrame()
                            if (auditor_ts_pw_mt_table is not None and 
                                hasattr(st.session_state, 'ts_pw_analysis_data') and 
                                not st.session_state.ts_pw_analysis_data.empty):
                                
                                result_mt_for_merge = st.session_state.ts_pw_analysis_data.copy()
                                if 'SALES in Tonage' in result_mt_for_merge.columns:
                                    result_mt_for_merge = result_mt_for_merge.rename(columns={'SALES in Tonage': 'PRODUCT NAME'})
                                result_mt_for_merge['PRODUCT NAME'] = result_mt_for_merge['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                calc_products = set(result_mt_for_merge['PRODUCT NAME']) - {'TOTAL SALES', '', 'NAN'}
                                
                                if not auditor_ts_pw_mt_table.empty:
                                    merged_mt_data = auditor_ts_pw_mt_table.copy()
                                    merged_mt_data['SALES in Tonage'] = merged_mt_data['SALES in Tonage'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                else:
                                    merged_mt_data = pd.DataFrame({'SALES in Tonage': list(calc_products)})
                                
                                auditor_products = set(merged_mt_data['SALES in Tonage']) if not merged_mt_data.empty else set()
                                missing_products = calc_products - auditor_products
                                if missing_products:
                                    missing_df = pd.DataFrame({'SALES in Tonage': list(missing_products)})
                                    for col in merged_mt_data.columns[1:]:
                                        missing_df[col] = 0.0
                                    merged_mt_data = pd.concat([merged_mt_data, missing_df], ignore_index=True)
                                
                                common_columns = set(merged_mt_data.columns) & set(result_mt_for_merge.columns) - {'SALES in Tonage', 'PRODUCT NAME', 'Region'}
                                if common_columns:
                                    for col in common_columns:
                                        for product in merged_mt_data['SALES in Tonage']:
                                            if product in result_mt_for_merge['PRODUCT NAME'].values and product != 'TOTAL SALES':
                                                product_value = result_mt_for_merge.loc[result_mt_for_merge['PRODUCT NAME'] == product, col].values
                                                if len(product_value) > 0:
                                                    merged_mt_data.loc[merged_mt_data['SALES in Tonage'] == product, col] = product_value[0]
                                
                                for ytd_period, columns in ytd_periods.items():
                                    valid_cols = [col for col in columns if col in merged_mt_data.columns]
                                    if valid_cols:
                                        merged_mt_data[ytd_period] = merged_mt_data[valid_cols].sum(axis=1).round(2)
                                
                                merged_mt_data = calculate_ytd_growth_achievement(merged_mt_data, 'SALES in Tonage')
                                
                                if 'TOTAL SALES' not in merged_mt_data['SALES in Tonage'].values:
                                    total_sales_row = {'SALES in Tonage': 'TOTAL SALES'}
                                    numeric_cols = merged_mt_data.select_dtypes(include=[np.number]).columns
                                    for col in numeric_cols:
                                        total_sales_row[col] = merged_mt_data[~merged_mt_data['SALES in Tonage'].isin(['TOTAL SALES'])][col].sum().round(2)
                                    merged_mt_data = pd.concat([merged_mt_data, pd.DataFrame([total_sales_row])], ignore_index=True)
                                else:
                                    numeric_cols = merged_mt_data.select_dtypes(include=[np.number]).columns
                                    for col in numeric_cols:
                                        sum_value = merged_mt_data[~merged_mt_data['SALES in Tonage'].isin(['TOTAL SALES'])][col].sum().round(2)
                                        merged_mt_data.loc[merged_mt_data['SALES in Tonage'] == 'TOTAL SALES', col] = sum_value
                                
                                total_sales_row = merged_mt_data[merged_mt_data['SALES in Tonage'] == 'TOTAL SALES']
                                other_rows = merged_mt_data[merged_mt_data['SALES in Tonage'] != 'TOTAL SALES']
                                other_rows = other_rows.sort_values(by='SALES in Tonage')
                                merged_mt_data = pd.concat([other_rows, total_sales_row], ignore_index=True)
                                
                                # Store merged tonage data in session state
                                st.session_state.merged_ts_pw_mt_data = merged_mt_data

                            merged_value_data = pd.DataFrame()
                            if (auditor_ts_pw_value_table is not None and 
                                hasattr(st.session_state, 'ts_pw_value_data') and 
                                not st.session_state.ts_pw_value_data.empty):
                                
                                result_value_for_merge = st.session_state.ts_pw_value_data.copy()
                                if 'SALES in Value' in result_value_for_merge.columns:
                                    result_value_for_merge = result_value_for_merge.rename(columns={'SALES in Value': 'PRODUCT NAME'})
                                result_value_for_merge['PRODUCT NAME'] = result_value_for_merge['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                calc_products = set(result_value_for_merge['PRODUCT NAME']) - {'TOTAL SALES', '', 'NAN'}
                                
                                if not auditor_ts_pw_value_table.empty:
                                    merged_value_data = auditor_ts_pw_value_table.copy()
                                    merged_value_data['SALES in Value'] = merged_value_data['SALES in Value'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                else:
                                    merged_value_data = pd.DataFrame({'SALES in Value': list(calc_products)})
                                
                                auditor_products = set(merged_value_data['SALES in Value']) if not merged_value_data.empty else set()
                                missing_products = calc_products - auditor_products
                                if missing_products:
                                    missing_df = pd.DataFrame({'SALES in Value': list(missing_products)})
                                    for col in merged_value_data.columns[1:]:
                                        missing_df[col] = 0.0
                                    merged_value_data = pd.concat([merged_value_data, missing_df], ignore_index=True)
                                
                                common_columns = set(merged_value_data.columns) & set(result_value_for_merge.columns) - {'SALES in Value', 'PRODUCT NAME', 'Region'}
                                if common_columns:
                                    for col in common_columns:
                                        for product in merged_value_data['SALES in Value']:
                                            if product in result_value_for_merge['PRODUCT NAME'].values and product != 'TOTAL SALES':
                                                product_value = result_value_for_merge.loc[result_value_for_merge['PRODUCT NAME'] == product, col].values
                                                if len(product_value) > 0:
                                                    merged_value_data.loc[merged_value_data['SALES in Value'] == product, col] = product_value[0]
                                
                                for ytd_period, columns in ytd_periods.items():
                                    valid_cols = [col for col in columns if col in merged_value_data.columns]
                                    if valid_cols:
                                        merged_value_data[ytd_period] = merged_value_data[valid_cols].sum(axis=1).round(2)
                                
                                merged_value_data = calculate_ytd_growth_achievement(merged_value_data, 'SALES in Value')
                                
                                for col in merged_value_data.columns:
                                    if col != 'SALES in Value':
                                        merged_value_data[col] = pd.to_numeric(merged_value_data[col], errors='coerce').fillna(0)
                                
                                numeric_cols = merged_value_data.select_dtypes(include=[np.number]).columns
                                merged_value_data[numeric_cols] = merged_value_data[numeric_cols].astype(float).round(2)
                                
                                if 'TOTAL SALES' not in merged_value_data['SALES in Value'].values:
                                    total_sales_row = {'SALES in Value': 'TOTAL SALES'}
                                    for col in numeric_cols:
                                        total_sales_row[col] = merged_value_data[~merged_value_data['SALES in Value'].isin(['TOTAL SALES'])][col].sum().round(2)
                                    merged_value_data = pd.concat([merged_value_data, pd.DataFrame([total_sales_row])], ignore_index=True)
                                else:
                                    for col in numeric_cols:
                                        sum_value = merged_value_data[~merged_value_data['SALES in Value'].isin(['TOTAL SALES'])][col].sum().round(2)
                                        merged_value_data.loc[merged_value_data['SALES in Value'] == 'TOTAL SALES', col] = sum_value
                                
                                total_sales_row = merged_value_data[merged_value_data['SALES in Value'] == 'TOTAL SALES']
                                other_rows = merged_value_data[merged_value_data['SALES in Value'] != 'TOTAL SALES']
                                other_rows = other_rows.sort_values(by='SALES in Value')
                                merged_value_data = pd.concat([other_rows, total_sales_row], ignore_index=True)
                                
                                # Store merged value data in session state
                                st.session_state.merged_ts_pw_value_data = merged_value_data

                            if not merged_mt_data.empty:
                                st.subheader(f"Merged Data (SALES in Tonage - NORTH) [{fiscal_year_str}]")
                                try:
                                    styled_df = safe_format_dataframe(merged_mt_data)
                                    numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                                    formatter = {col: "{:,.2f}" for col in numeric_cols}
                                    st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                                except Exception as e:
                                    st.error(f"Error displaying tonage merge: {e}")
                                    st.dataframe(merged_mt_data, use_container_width=True)

                            if not merged_value_data.empty:
                                st.subheader(f"Merged Data (SALES in Value - NORTH) [{fiscal_year_str}]")
                                try:
                                    styled_df = safe_format_dataframe(merged_value_data)
                                    numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                                    formatter = {col: "{:,.2f}" for col in numeric_cols}
                                    st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                                except Exception as e:
                                    st.error(f"Error displaying value merge: {e}")
                                    st.dataframe(merged_value_data, use_container_width=True)

                            if not merged_mt_data.empty or not merged_value_data.empty:
                                output = BytesIO()
                                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                    workbook = writer.book
                                    worksheet = workbook.add_worksheet('Merged_TS_PW_Data')
                                    title_format = workbook.add_format({
                                        'bold': True, 'align': 'center', 'valign': 'vcenter',
                                        'font_size': 14, 'font_color': '#000000'
                                    })
                                    header_format = workbook.add_format({
                                        'bold': True, 'text_wrap': True, 'valign': 'top',
                                        'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                                    })
                                    num_format = workbook.add_format({'num_format': '#,##0.00'})
                                    
                                    num_cols = max(
                                        len(merged_mt_data.columns) if not merged_mt_data.empty else 0,
                                        len(merged_value_data.columns) if not merged_value_data.empty else 0
                                    )
                                    worksheet.merge_range(2, 0, 2, num_cols - 1, f"TS-PW SALES REVIEW FOR NORTH REGION [{fiscal_year_str}]", title_format)
                                    start_row = 4
                                    
                                    if not merged_mt_data.empty:
                                        merged_mt_data.to_excel(writer, sheet_name='Merged_TS_PW_Data', startrow=start_row, index=False)
                                        for col_num, value in enumerate(merged_mt_data.columns.values):
                                            worksheet.write(start_row, col_num, value, header_format)
                                        for col in merged_mt_data.select_dtypes(include=[np.number]).columns:
                                            col_idx = merged_mt_data.columns.get_loc(col)
                                            worksheet.set_column(col_idx, col_idx, None, num_format)
                                        for i, col in enumerate(merged_mt_data.columns):
                                            max_len = max((merged_mt_data[col].astype(str).str.len().max(), len(str(col)))) + 2
                                            worksheet.set_column(i, i, max_len)
                                        start_row += len(merged_mt_data) + 4
                                    
                                    if not merged_value_data.empty:
                                        merged_value_data.to_excel(writer, sheet_name='Merged_TS_PW_Data', startrow=start_row, index=False)
                                        for col_num, value in enumerate(merged_value_data.columns.values):
                                            worksheet.write(start_row, col_num, value, header_format)
                                        for col in merged_value_data.select_dtypes(include=[np.number]).columns:
                                            col_idx = merged_value_data.columns.get_loc(col)
                                            worksheet.set_column(col_idx, col_idx, None, num_format)
                                        for i, col in enumerate(merged_value_data.columns):
                                            max_len = max((merged_value_data[col].astype(str).str.len().max(), len(str(col)))) + 2
                                            worksheet.set_column(i, i, max_len)
                                
                                excel_data = output.getvalue()
                                st.download_button(
                                    label="⬇️ Download Merged TS-PW Data as Excel",
                                    data=excel_data,
                                    file_name=f"merged_ts_pw_data_north_{fiscal_year_str}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_ts_pw_merge"
                                )
                            else:
                                st.info("No valid merged data available to export.")
                        except Exception as e:
                            st.error(f"Error during TS-PW merge: {e}")
                    else:
                        st.info("ℹ Upload audit file and generate TS-PW data first")
            else:
                st.warning("Required column 'PRODUCT NAME' not found in budget data.")
        except Exception as e:
            st.error(f"Error processing TS-PW data: {e}")
    else:
        st.info("Please upload Sales and Budget files and select a sales sheet.")




with tab6:
    st.header("📊 ERO-PW Data Analysis (WEST)")
    
    # Get current date and determine fiscal year
    current_date = datetime.now()
    current_year = current_date.year
    if current_date.month >= 4:
        fiscal_year_start = current_year
        fiscal_year_end = current_year + 1
    else:
        fiscal_year_start = current_year - 1
        fiscal_year_end = current_year
    fiscal_year_str = f"{str(fiscal_year_start)[-2:]}-{str(fiscal_year_end)[-2:]}"
    last_fiscal_year_start = fiscal_year_start - 1
    last_fiscal_year_end = fiscal_year_end - 1
    last_fiscal_year_str = f"{str(last_fiscal_year_start)[-2:]}-{str(last_fiscal_year_end)[-2:]}"
    
    # Define months for April to March
    months = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']
    
    selected_sheet_budget = None
    if st.session_state.get('uploaded_file_budget'):
        xls_budget = pd.ExcelFile(st.session_state.uploaded_file_budget)
        budget_sheet_names = xls_budget.sheet_names
        if budget_sheet_names:
            if 'budget_sheet_selection' in st.session_state and st.session_state.budget_sheet_selection:
                selected_sheet_budget = st.session_state.budget_sheet_selection
            else:
                selected_sheet_budget = budget_sheet_names[0]
    
    selected_sheet_last_year = st.session_state.get('last_year_sheet')
    
    if (st.session_state.get('uploaded_file_sales') and st.session_state.get('uploaded_file_budget') and 
        'selected_sheets_sales' in st.session_state and selected_sheet_budget):
        try:
            xls_budget = pd.ExcelFile(st.session_state.uploaded_file_budget)
            df_budget = pd.read_excel(xls_budget, sheet_name=selected_sheet_budget)
            df_budget.columns = df_budget.columns.str.strip()
            df_budget = df_budget.dropna(how='all').reset_index(drop=True)

            budget_data = process_budget_data_product_region(df_budget, group_type='product_region')
            if budget_data is None:
                st.error("Failed to process budget data for ERO-PW analysis.")
                st.stop()

            if 'Region' in budget_data.columns:
                budget_data = budget_data[budget_data['Region'].str.strip().str.upper() == 'WEST']
                if budget_data.empty:
                    st.error("No budget data found for WEST region.")
                    st.stop()

            st.session_state.ero_pw_budget_data = budget_data

            required_cols = ['PRODUCT NAME']
            if all(col in budget_data.columns for col in required_cols):
                mt_cols = [col for col in budget_data.columns if col.endswith('_MT')]
                value_cols = [col for col in budget_data.columns if col.endswith('_Value')]
    
                actual_mt_last = None
                actual_value_last = None

                # Process Last Year Actual Data
                if st.session_state.get('uploaded_file_last_year') and selected_sheet_last_year:
                    try:
                        xls_last_year = pd.ExcelFile(st.session_state.uploaded_file_last_year)
                        df_last_year = pd.read_excel(xls_last_year, sheet_name=selected_sheet_last_year, header=0)
                        
                        if isinstance(df_last_year.columns, pd.MultiIndex):
                            df_last_year.columns = ['_'.join(col).strip() for col in df_last_year.columns.values]
                        df_last_year = handle_duplicate_columns(df_last_year)
                        
                        region_col = find_column(df_last_year, ['Region', 'Area', 'Zone'], case_sensitive=False)
                        product_col = find_column(df_last_year, ['Type (Make)', 'Type(Make)', 'Product Group', 'Product'], case_sensitive=False)
                        date_col = find_column(df_last_year, ['Month Format', 'Date', 'Month'], case_sensitive=False)
                        qty_col = find_column(df_last_year, ['Actual Quantity', 'Acutal Quantity', 'Quantity'], case_sensitive=False)
                        amount_col = find_column(df_last_year, ['Amount', 'Value', 'Sales Value'], case_sensitive=False)
                        
                        rename_dict = {}
                        if date_col:
                            rename_dict[date_col] = 'Month Format'
                        if product_col:
                            rename_dict[product_col] = 'Product Group'
                        if qty_col:
                            rename_dict[qty_col] = 'Actual Quantity'
                        if amount_col:
                            rename_dict[amount_col] = 'Amount'
                        if region_col:
                            rename_dict[region_col] = 'Region'
                        
                        df_last_year = df_last_year.rename(columns=rename_dict)
                        
                        region_col = "Region" if "Region" in df_last_year.columns else None
                        product_col = "Product Group" if "Product Group" in df_last_year.columns else None
                        date_col = "Month Format" if "Month Format" in df_last_year.columns else None
                        qty_col = "Actual Quantity" if "Actual Quantity" in df_last_year.columns else None
                        amount_col = "Amount" if "Amount" in df_last_year.columns else None
                        
                        if product_col and date_col:
                            if region_col:
                                df_last_year = df_last_year[df_last_year[region_col].str.strip().str.upper() == 'WEST']
                            
                            if not df_last_year.empty:
                                if qty_col:
                                    try:
                                        df_last_year_qty = df_last_year.copy()
                                        df_last_year_qty['Actual Quantity'] = pd.to_numeric(df_last_year_qty[qty_col], errors='coerce')
                                        
                                        if pd.api.types.is_datetime64_any_dtype(df_last_year_qty[date_col]):
                                            df_last_year_qty['Month'] = pd.to_datetime(df_last_year_qty[date_col]).dt.strftime('%b')
                                        else:
                                            df_last_year_qty['Month'] = df_last_year_qty[date_col].str.strip().str[:3]
                                        
                                        last_year_agg = df_last_year_qty.groupby(['Product Group', 'Month'])['Actual Quantity'].sum().reset_index()
                                        last_year_agg.columns = ['PRODUCT NAME', 'Month', 'LY_Actual']
                                        last_year_agg['PRODUCT NAME'] = last_year_agg['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                        
                                        last_year_agg['Month_Year'] = last_year_agg['Month'].apply(
                                            lambda x: f'LY-{x}-{str(last_fiscal_year_start)[-2:]}' if x in months[:9] else f'LY-{x}-{str(last_fiscal_year_end)[-2:]}'
                                        )
                                        
                                        actual_mt_last = last_year_agg.pivot_table(
                                            index='PRODUCT NAME',
                                            columns='Month_Year',
                                            values='LY_Actual',
                                            aggfunc='sum'
                                        ).reset_index().fillna(0)
                                        actual_mt_last['Region'] = 'WEST'
                                    except Exception as e:
                                        st.error(f"Error in last year quantity grouping: {str(e)}")
                                        actual_mt_last = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                                
                                if amount_col:
                                    try:
                                        df_last_year_val = df_last_year.copy()
                                        df_last_year_val[amount_col] = pd.to_numeric(df_last_year_val[amount_col], errors='coerce')
                                        
                                        if pd.api.types.is_datetime64_any_dtype(df_last_year_val[date_col]):
                                            df_last_year_val['Month'] = pd.to_datetime(df_last_year_val[date_col]).dt.strftime('%b')
                                        else:
                                            df_last_year_val['Month'] = df_last_year_val[date_col].str.strip().str[:3]
                                        
                                        last_year_value_agg = df_last_year_val.groupby(['Product Group', 'Month'])['Amount'].sum().reset_index()
                                        last_year_value_agg.columns = ['PRODUCT NAME', 'Month', 'LY_Actual']
                                        last_year_value_agg['PRODUCT NAME'] = last_year_value_agg['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                        
                                        last_year_value_agg['Month_Year'] = last_year_value_agg['Month'].apply(
                                            lambda x: f'LY-{x}-{str(last_fiscal_year_start)[-2:]}' if x in months[:9] else f'LY-{x}-{str(last_fiscal_year_end)[-2:]}'
                                        )
                                        
                                        actual_value_last = last_year_value_agg.pivot_table(
                                            index='PRODUCT NAME',
                                            columns='Month_Year',
                                            values='LY_Actual',
                                            aggfunc='sum'
                                        ).reset_index().fillna(0)
                                        actual_value_last['Region'] = 'WEST'
                                    except Exception as e:
                                        st.error(f"Error in last year value grouping: {str(e)}")
                                        actual_value_last = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                            else:
                                st.warning("No WEST region data found in last year file.")
                        else:
                            st.warning(f"Required columns not found in last year sheet '{selected_sheet_last_year}'.")
                    except Exception as e:
                        st.error(f"Error processing last year data: {e}")

                # Define YTD periods dynamically
                ytd_periods = {}
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Jun)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Sep)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Dec)Budget'] = [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'YTD-{fiscal_year_str} (Apr to Mar)Budget'] = (
                    [f'Budget-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'Budget-{month}-{str(fiscal_year_end)[-2:]}' for month in months[9:]]
                )
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Jun)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Sep)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Dec)LY'] = [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'YTD-{last_fiscal_year_str} (Apr to Mar)LY'] = (
                    [f'LY-{month}-{str(last_fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'LY-{month}-{str(last_fiscal_year_end)[-2:]}' for month in months[9:]]
                )
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Jun)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:3]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Sep)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:6]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Dec)'] = [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]]
                ytd_periods[f'Act-YTD-{fiscal_year_str} (Apr to Mar)'] = (
                    [f'Act-{month}-{str(fiscal_year_start)[-2:]}' for month in months[:9]] +
                    [f'Act-{month}-{str(fiscal_year_end)[-2:]}' for month in months[9:]]
                )

                # Initialize session state variables
                if 'ero_pw_analysis_data' not in st.session_state:
                    st.session_state.ero_pw_analysis_data = pd.DataFrame()
                if 'ero_pw_value_data' not in st.session_state:
                    st.session_state.ero_pw_value_data = pd.DataFrame()
                if 'actual_ero_pw_mt_data' not in st.session_state:
                    st.session_state.actual_ero_pw_mt_data = pd.DataFrame()
                if 'actual_ero_pw_value_data' not in st.session_state:
                    st.session_state.actual_ero_pw_value_data = pd.DataFrame()

                tab_ero_pw_mt, tab_ero_pw_value, tab_ero_pw_merge = st.tabs(
                    ["SALES in Tonage", "SALES in Value", "Merge Preview"]
                )

                with tab_ero_pw_mt:
                    if mt_cols:
                        month_cols = sorted(set(col.replace('_MT', '') for col in mt_cols if not col.endswith(f'-{last_fiscal_year_start}_MT')))
                        last_year_cols = sorted(set(col.replace('_MT', '') for col in mt_cols if col.endswith(f'-{last_fiscal_year_start}_MT')))
                        
                        result_ero_pw_mt = pd.DataFrame()
                        for month_col in month_cols:
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_MT']].rename(columns={f'{month_col}_MT': month_col})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[month_col].sum()
                            if result_ero_pw_mt.empty:
                                result_ero_pw_mt = temp_df
                            else:
                                result_ero_pw_mt = result_ero_pw_mt.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='outer')
                        
                        for month_col in last_year_cols:
                            orig_month = month_col.replace(f'-{last_fiscal_year_start}', '')
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_MT']].rename(columns={f'{month_col}_MT': f'LY-{orig_month}'})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[f'LY-{orig_month}'].sum()
                            result_ero_pw_mt = result_ero_pw_mt.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        result_ero_pw_mt['PRODUCT NAME'] = result_ero_pw_mt['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                        result_ero_pw_mt['Region'] = result_ero_pw_mt['Region'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')

                        # Process Current Year Actual Sales Data
                        actual_mt_current = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                        if st.session_state.get('uploaded_file_sales') and 'selected_sheets_sales' in st.session_state:
                            all_sales_data = []
                            xls_sales = pd.ExcelFile(st.session_state.uploaded_file_sales)
                            
                            for sheet_name in st.session_state.selected_sheets_sales:
                                try:
                                    df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                                    if isinstance(df_sales.columns, pd.MultiIndex):
                                        df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                                    df_sales = handle_duplicate_columns(df_sales)
                                    
                                    region_col = find_column(df_sales, ['Region', 'Area', 'Zone'], case_sensitive=False)
                                    product_col = find_column(df_sales, ['Type(Make)', 'Type (Make)'], case_sensitive=False)
                                    date_col = find_column(df_sales, ['Date', 'Month Format', 'Month'], case_sensitive=False)
                                    qty_col = find_column(df_sales, ['Actual Quantity', 'Acutal Quantity'], case_sensitive=False)
                                    
                                    rename_dict = {}
                                    if date_col:
                                        rename_dict[date_col] = 'Month Format'
                                    if product_col:
                                        rename_dict[product_col] = 'Product Group'
                                    if qty_col:
                                        rename_dict[qty_col] = 'Actual Quantity'
                                    if region_col:
                                        rename_dict[region_col] = 'Region'
                                    df_sales = df_sales.rename(columns=rename_dict)
                                    
                                    region_col = "Region" if "Region" in df_sales.columns else None
                                    product_col = "Product Group" if "Product Group" in df_sales.columns else None
                                    date_col = "Month Format" if "Month Format" in df_sales.columns else None
                                    qty_col = "Actual Quantity" if "Actual Quantity" in df_sales.columns else None
                                    
                                    if product_col and date_col and qty_col:
                                        if region_col:
                                            df_sales = df_sales[df_sales[region_col].str.strip().str.upper() == 'WEST']
                                        
                                        if not df_sales.empty:
                                            df_sales['Actual Quantity'] = pd.to_numeric(df_sales[qty_col], errors='coerce')
                                            
                                            if pd.api.types.is_datetime64_any_dtype(df_sales[date_col]):
                                                df_sales['Month'] = pd.to_datetime(df_sales[date_col]).dt.strftime('%b')
                                            else:
                                                df_sales['Month'] = df_sales[date_col].astype(str).str.strip().str[:3]
                                            
                                            df_sales = df_sales.dropna(subset=['Actual Quantity', 'Month'])
                                            df_sales = df_sales[df_sales['Actual Quantity'] != 0]
                                            
                                            all_sales_data.append(df_sales)
                                except Exception as e:
                                    st.error(f"Error processing sales sheet {sheet_name}: {e}")
                            
                            if all_sales_data:
                                combined_sales = pd.concat(all_sales_data, ignore_index=True)
                                
                                try:
                                    sales_agg_current = combined_sales.groupby(['Product Group', 'Month'])['Actual Quantity'].sum().reset_index()
                                    sales_agg_current.columns = ['PRODUCT NAME', 'Month', 'Actual']
                                    sales_agg_current['PRODUCT NAME'] = sales_agg_current['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    sales_agg_current['Month_Year'] = sales_agg_current['Month'].apply(
                                        lambda x: f'Act-{x}-{str(fiscal_year_start)[-2:]}' if x in months[:9] else f'Act-{x}-{str(fiscal_year_end)[-2:]}'
                                    )
                                    
                                    actual_mt_current = sales_agg_current.pivot_table(
                                        index='PRODUCT NAME',
                                        columns='Month_Year',
                                        values='Actual',
                                        aggfunc='sum'
                                    ).reset_index().fillna(0)
                                    actual_mt_current['Region'] = 'WEST'
                                except Exception as e:
                                    st.error(f"Error in sales quantity grouping: {e}")

                        # Merge actual data properly
                        actual_mt = pd.DataFrame({'PRODUCT NAME': result_ero_pw_mt['PRODUCT NAME'], 'Region': 'WEST'})
                        
                        if not actual_mt_current.empty and 'PRODUCT NAME' in actual_mt_current.columns:
                            actual_mt = actual_mt.merge(actual_mt_current, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        if actual_mt_last is not None and not actual_mt_last.empty:
                            actual_mt = actual_mt.merge(actual_mt_last, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        st.session_state.actual_ero_pw_mt_data = actual_mt
                        result_ero_pw_mt = result_ero_pw_mt.merge(actual_mt, on=['PRODUCT NAME', 'Region'], how='left')

                        # Calculate Growth and Achievement columns
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            if gr_col not in result_ero_pw_mt.columns:
                                result_ero_pw_mt[gr_col] = np.nan
                            if ach_col not in result_ero_pw_mt.columns:
                                result_ero_pw_mt[ach_col] = np.nan
                            
                            if actual_col in result_ero_pw_mt.columns and ly_col in result_ero_pw_mt.columns:
                                result_ero_pw_mt[gr_col] = (
                                    (result_ero_pw_mt[actual_col] - result_ero_pw_mt[ly_col]) /
                                    result_ero_pw_mt[ly_col].replace(0, np.nan) * 100
                                ).round(2)
                            
                            if budget_col in result_ero_pw_mt.columns and actual_col in result_ero_pw_mt.columns:
                                result_ero_pw_mt[ach_col] = (
                                    (result_ero_pw_mt[actual_col] /
                                     result_ero_pw_mt[budget_col].replace(0, np.nan)) * 100
                                ).round(2)

                        # Calculate YTD columns
                        for ytd_period, period_cols in ytd_periods.items():
                            valid_cols = [col for col in period_cols if col in result_ero_pw_mt.columns]
                            if valid_cols:
                                result_ero_pw_mt[ytd_period] = result_ero_pw_mt[valid_cols].sum(axis=1).round(2)

                        # Calculate total row
                        exclude_products = ['WEST TOTAL', 'EAST SALES', 'GRAND TOTAL']
                        valid_products = result_ero_pw_mt[~result_ero_pw_mt['PRODUCT NAME'].isin(exclude_products)]
                        grand_total_row = {'PRODUCT NAME': 'TOTAL SALES', 'Region': 'WEST'}
                        numeric_cols = valid_products.select_dtypes(include=[np.number]).columns
                        for col in numeric_cols:
                            grand_total_row[col] = valid_products[col].sum().round(2)
                        
                        result_ero_pw_mt = result_ero_pw_mt[result_ero_pw_mt['PRODUCT NAME'] != 'TOTAL SALES']
                        result_ero_pw_mt = pd.concat([result_ero_pw_mt, pd.DataFrame([grand_total_row])], ignore_index=True)
                        
                        result_ero_pw_mt = result_ero_pw_mt.rename(columns={'PRODUCT NAME': 'SALES in Tonage'})
                        st.session_state.ero_pw_analysis_data = result_ero_pw_mt

                        st.subheader(f"ERO-PW Monthly Budget and Actual Tonage (WEST) [{fiscal_year_str}]")
                        
                        try:
                            styled_df = safe_format_dataframe(result_ero_pw_mt)
                            numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                            formatter = {col: "{:,.2f}" for col in numeric_cols}
                            st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                        except Exception as e:
                            st.error(f"Error displaying dataframe: {str(e)}")
                            st.dataframe(result_ero_pw_mt, use_container_width=True)

                        if not result_ero_pw_mt.empty:
                            output = BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                result_ero_pw_mt.to_excel(writer, sheet_name='ERO_PW_MT_Analysis', index=False)
                                workbook = writer.book
                                worksheet = writer.sheets['ERO_PW_MT_Analysis']
                                header_format = workbook.add_format({
                                    'bold': True, 'text_wrap': True, 'valign': 'top',
                                    'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                                })
                                num_format = workbook.add_format({'num_format': '0.00'})
                                
                                for col_num, value in enumerate(result_ero_pw_mt.columns.values):
                                    worksheet.write(0, col_num, value, header_format)
                                for col in result_ero_pw_mt.select_dtypes(include=[np.number]).columns:
                                    col_idx = result_ero_pw_mt.columns.get_loc(col)
                                    worksheet.set_column(col_idx, col_idx, None, num_format)
                                for i, col in enumerate(result_ero_pw_mt.columns):
                                    max_len = max((result_ero_pw_mt[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, max_len)
                            
                            excel_data = output.getvalue()
                            st.download_button(
                                label="⬇️ Download Budget Tonage as Excel",
                                data=excel_data,
                                file_name=f"ero_pw_monthly_budget_tonage_west_{fiscal_year_str}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_ero_pw_mt_excel"
                            )
                    else:
                        st.warning("No budget tonage columns found.")

                with tab_ero_pw_value:
                    if value_cols:
                        month_cols = sorted(set(col.replace('_Value', '') for col in value_cols if not col.endswith(f'-{last_fiscal_year_start}_Value')))
                        last_year_cols = sorted(set(col.replace('_Value', '') for col in value_cols if col.endswith(f'-{last_fiscal_year_start}_Value')))
                        
                        result_ero_pw_value = pd.DataFrame()
                        for month_col in month_cols:
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_Value']].rename(columns={f'{month_col}_Value': month_col})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[month_col].sum()
                            if result_ero_pw_value.empty:
                                result_ero_pw_value = temp_df
                            else:
                                result_ero_pw_value = result_ero_pw_value.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='outer')
                        
                        for month_col in last_year_cols:
                            orig_month = month_col.replace(f'-{last_fiscal_year_start}', '')
                            temp_df = budget_data[['PRODUCT NAME', 'Region', f'{month_col}_Value']].rename(columns={f'{month_col}_Value': f'LY-{orig_month}'})
                            temp_df = temp_df.groupby(['PRODUCT NAME', 'Region'], as_index=False)[f'LY-{orig_month}'].sum()
                            result_ero_pw_value = result_ero_pw_value.merge(temp_df, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        result_ero_pw_value['PRODUCT NAME'] = result_ero_pw_value['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                        result_ero_pw_value['Region'] = result_ero_pw_value['Region'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')

                        # Process Current Year Actual Sales Value Data
                        actual_value_current = pd.DataFrame({'PRODUCT NAME': [], 'Region': []})
                        if st.session_state.get('uploaded_file_sales') and 'selected_sheets_sales' in st.session_state:
                            all_sales_data = []
                            xls_sales = pd.ExcelFile(st.session_state.uploaded_file_sales)
                            
                            for sheet_name in st.session_state.selected_sheets_sales:
                                try:
                                    df_sales = pd.read_excel(xls_sales, sheet_name=sheet_name, header=0)
                                    if isinstance(df_sales.columns, pd.MultiIndex):
                                        df_sales.columns = ['_'.join(col).strip() for col in df_sales.columns.values]
                                    df_sales = handle_duplicate_columns(df_sales)
                                    
                                    region_col = find_column(df_sales, ['Region', 'Area', 'Zone'], case_sensitive=False)
                                    product_col = find_column(df_sales, ['Type(Make)', 'Type (Make)'], case_sensitive=False)
                                    date_col = find_column(df_sales, ['Date', 'Month Format', 'Month'], case_sensitive=False)
                                    value_col = find_column(df_sales, ['Value', 'Sales', 'Sales Value'], case_sensitive=False)
                                    
                                    rename_dict = {}
                                    if date_col:
                                        rename_dict[date_col] = 'Month Format'
                                    if product_col:
                                        rename_dict[product_col] = 'Product Group'
                                    if value_col:
                                        rename_dict[value_col] = 'Value'
                                    if region_col:
                                        rename_dict[region_col] = 'Region'
                                    df_sales = df_sales.rename(columns=rename_dict)
                                    
                                    region_col = "Region" if "Region" in df_sales.columns else None
                                    product_col = "Product Group" if "Product Group" in df_sales.columns else None
                                    date_col = "Month Format" if "Month Format" in df_sales.columns else None
                                    value_col = "Value" if "Value" in df_sales.columns else None
                                    
                                    if product_col and date_col and value_col:
                                        if region_col:
                                            df_sales = df_sales[df_sales[region_col].str.strip().str.upper() == 'WEST']
                                        
                                        if not df_sales.empty:
                                            df_sales[value_col] = pd.to_numeric(df_sales[value_col], errors='coerce')
                                            
                                            if pd.api.types.is_datetime64_any_dtype(df_sales[date_col]):
                                                df_sales['Month'] = pd.to_datetime(df_sales[date_col]).dt.strftime('%b')
                                            else:
                                                df_sales['Month'] = df_sales[date_col].astype(str).str.strip().str[:3]
                                            
                                            df_sales = df_sales.dropna(subset=[value_col, 'Month'])
                                            df_sales = df_sales[df_sales[value_col] != 0]
                                            
                                            all_sales_data.append(df_sales)
                                except Exception as e:
                                    st.error(f"Error processing sales value sheet {sheet_name}: {e}")
                            
                            if all_sales_data:
                                combined_sales = pd.concat(all_sales_data, ignore_index=True)
                                
                                try:
                                    sales_agg_current = combined_sales.groupby(['Product Group', 'Month'])['Value'].sum().reset_index()
                                    sales_agg_current.columns = ['PRODUCT NAME', 'Month', 'Actual']
                                    sales_agg_current['PRODUCT NAME'] = sales_agg_current['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    sales_agg_current['Month_Year'] = sales_agg_current['Month'].apply(
                                        lambda x: f'Act-{x}-{str(fiscal_year_start)[-2:]}' if x in months[:9] else f'Act-{x}-{str(fiscal_year_end)[-2:]}'
                                    )
                                    
                                    actual_value_current = sales_agg_current.pivot_table(
                                        index='PRODUCT NAME',
                                        columns='Month_Year',
                                        values='Actual',
                                        aggfunc='sum'
                                    ).reset_index().fillna(0)
                                    actual_value_current['Region'] = 'WEST'
                                except Exception as e:
                                    st.error(f"Error in sales value grouping: {e}")

                        # Merge actual value data properly
                        actual_value = pd.DataFrame({'PRODUCT NAME': result_ero_pw_value['PRODUCT NAME'], 'Region': 'WEST'})
                        
                        if not actual_value_current.empty and 'PRODUCT NAME' in actual_value_current.columns:
                            actual_value = actual_value.merge(actual_value_current, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        if actual_value_last is not None and not actual_value_last.empty:
                            actual_value = actual_value.merge(actual_value_last, on=['PRODUCT NAME', 'Region'], how='left')
                        
                        st.session_state.actual_ero_pw_value_data = actual_value
                        result_ero_pw_value = result_ero_pw_value.merge(actual_value, on=['PRODUCT NAME', 'Region'], how='left')

                        # Calculate Growth and Achievement columns for Value
                        for month in months:
                            budget_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            actual_year = str(fiscal_year_start)[-2:] if month in months[:9] else str(fiscal_year_end)[-2:]
                            ly_year = str(last_fiscal_year_start)[-2:] if month in months[:9] else str(last_fiscal_year_end)[-2:]
                            
                            budget_col = f'Budget-{month}-{budget_year}'
                            actual_col = f'Act-{month}-{actual_year}'
                            ly_col = f'LY-{month}-{ly_year}'
                            gr_col = f'Gr-{month}-{actual_year}'
                            ach_col = f'Ach-{month}-{actual_year}'
                            
                            if gr_col not in result_ero_pw_value.columns:
                                result_ero_pw_value[gr_col] = np.nan
                            if ach_col not in result_ero_pw_value.columns:
                                result_ero_pw_value[ach_col] = np.nan
                            
                            if actual_col in result_ero_pw_value.columns and ly_col in result_ero_pw_value.columns:
                                result_ero_pw_value[gr_col] = (
                                    (result_ero_pw_value[actual_col] - result_ero_pw_value[ly_col]) /
                                    result_ero_pw_value[ly_col].replace(0, np.nan) * 100
                                ).round(2)
                            
                            if budget_col in result_ero_pw_value.columns and actual_col in result_ero_pw_value.columns:
                                result_ero_pw_value[ach_col] = (
                                    (result_ero_pw_value[actual_col] /
                                     result_ero_pw_value[budget_col].replace(0, np.nan)) * 100
                                ).round(2)

                        # Calculate YTD columns for Value
                        for ytd_period, period_cols in ytd_periods.items():
                            valid_cols = [col for col in period_cols if col in result_ero_pw_value.columns]
                            if valid_cols:
                                result_ero_pw_value[ytd_period] = result_ero_pw_value[valid_cols].sum(axis=1).round(2)

                        # Calculate total row for Value
                        exclude_products = ['WEST TOTAL', 'EAST SALES', 'GRAND TOTAL']
                        valid_products = result_ero_pw_value[~result_ero_pw_value['PRODUCT NAME'].isin(exclude_products)]
                        grand_total_row = {'PRODUCT NAME': 'TOTAL SALES', 'Region': 'WEST'}
                        numeric_cols = valid_products.select_dtypes(include=[np.number]).columns
                        for col in numeric_cols:
                            grand_total_row[col] = valid_products[col].sum().round(2)
                        
                        result_ero_pw_value = result_ero_pw_value[result_ero_pw_value['PRODUCT NAME'] != 'TOTAL SALES']
                        result_ero_pw_value = pd.concat([result_ero_pw_value, pd.DataFrame([grand_total_row])], ignore_index=True)
                        
                        result_ero_pw_value = result_ero_pw_value.rename(columns={'PRODUCT NAME': 'SALES in Value'})
                        st.session_state.ero_pw_value_data = result_ero_pw_value

                        st.subheader(f"ERO-PW Monthly Budget and Actual Value (WEST) [{fiscal_year_str}]")
                        
                        try:
                            styled_df = safe_format_dataframe(result_ero_pw_value)
                            numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                            formatter = {col: "{:,.2f}" for col in numeric_cols}
                            st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                        except Exception as e:
                            st.error(f"Error displaying dataframe: {str(e)}")
                            st.dataframe(result_ero_pw_value, use_container_width=True)

                        if not result_ero_pw_value.empty:
                            output = BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                result_ero_pw_value.to_excel(writer, sheet_name='ERO_PW_Value_Analysis', index=False)
                                workbook = writer.book
                                worksheet = writer.sheets['ERO_PW_Value_Analysis']
                                header_format = workbook.add_format({
                                    'bold': True, 'text_wrap': True, 'valign': 'top',
                                    'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                                })
                                num_format = workbook.add_format({'num_format': '0.00'})
                                
                                for col_num, value in enumerate(result_ero_pw_value.columns.values):
                                    worksheet.write(0, col_num, value, header_format)
                                for col in result_ero_pw_value.select_dtypes(include=[np.number]).columns:
                                    col_idx = result_ero_pw_value.columns.get_loc(col)
                                    worksheet.set_column(col_idx, col_idx, None, num_format)
                                for i, col in enumerate(result_ero_pw_value.columns):
                                    max_len = max((result_ero_pw_value[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, max_len)
                            
                            excel_data = output.getvalue()
                            st.download_button(
                                label="⬇️ Download Budget Value as Excel",
                                data=excel_data,
                                file_name=f"ero_pw_monthly_budget_value_west_{fiscal_year_str}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_ero_pw_value_excel"
                            )
                    else:
                        st.warning("No budget value columns found.")

                with tab_ero_pw_merge:
                    if st.session_state.get('uploaded_file_auditor'):
                        try:
                            st.subheader(f"🔀 Merge Preview with Auditor Data (WEST) [{fiscal_year_str}]")
                            xls_auditor = pd.ExcelFile(st.session_state.uploaded_file_auditor)
                            auditor_sheet_names = xls_auditor.sheet_names

                            ero_pw_sheet = None
                            for sheet in auditor_sheet_names:
                                if any(term.lower() in sheet.lower() for term in ['ero-pw', 'eropw', 'west']):
                                    ero_pw_sheet = sheet
                                    break

                            if not ero_pw_sheet:
                                st.error("No ERO-PW analysis sheet found in auditor file.")
                                st.stop()

                            df_auditor = pd.read_excel(xls_auditor, sheet_name=ero_pw_sheet, header=None)

                            mt_table_headers = [
                                "SALES in Tonage", "SALES IN TONAGE", "Tonage", "TONAGE",
                                "Sales in MT", "SALES IN MT", "SALES in Ton", "Metric Tons",
                                "MT Sales", "Tonage Sales", "Sales Tonage"
                            ]

                            value_table_headers = [
                                "SALES in Value", "SALES IN VALUE", "Sales in Rs", "SALES IN RS",
                                "Value", "VALUE", "Sales Value"
                            ]

                            mt_idx, mt_data_start = extract_tables(df_auditor, mt_table_headers, is_product_analysis=True)
                            value_idx, value_data_start = extract_tables(df_auditor, value_table_headers, is_product_analysis=True)

                            auditor_ero_pw_mt_table = None
                            auditor_ero_pw_value_table = None
                            
                            if mt_idx is not None and mt_data_start is not None:
                                if value_idx is not None and value_idx > mt_idx:
                                    mt_table = df_auditor.iloc[mt_data_start:value_idx].dropna(how='all')
                                else:
                                    mt_table = df_auditor.iloc[mt_data_start:].dropna(how='all')
                                
                                if not mt_table.empty:
                                    mt_table.columns = df_auditor.iloc[mt_idx]
                                    mt_table.columns = rename_columns(mt_table.columns)
                                    mt_table = handle_duplicate_columns(mt_table)
                                    if mt_table.columns[0] != 'SALES in Tonage':
                                        mt_table = mt_table.rename(columns={mt_table.columns[0]: 'SALES in Tonage'})
                                    
                                    mt_table['SALES in Tonage'] = mt_table['SALES in Tonage'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    mt_table = mt_table[~mt_table['SALES in Tonage'].isin(['PRODUCT NAME', '', 'NAN'])]
                                    mt_table = mt_table.dropna(subset=['SALES in Tonage'])
                                    
                                    for col in mt_table.columns[1:]:
                                        mt_table[col] = pd.to_numeric(mt_table[col], errors='coerce').fillna(0)
                                    numeric_cols = mt_table.select_dtypes(include=[np.number]).columns
                                    mt_table[numeric_cols] = mt_table[numeric_cols].astype(float).round(2)
                                    auditor_ero_pw_mt_table = mt_table
                            
                            if value_idx is not None and value_data_start is not None:
                                value_table = df_auditor.iloc[value_data_start:].dropna(how='all')
                                
                                if not value_table.empty:
                                    value_table.columns = df_auditor.iloc[value_idx]
                                    value_table.columns = rename_columns(value_table.columns)
                                    value_table = handle_duplicate_columns(value_table)
                                    if value_table.columns[0] != 'SALES in Value':
                                        value_table = value_table.rename(columns={value_table.columns[0]: 'SALES in Value'})
                                    
                                    value_table['SALES in Value'] = value_table['SALES in Value'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                    
                                    value_table = value_table[~value_table['SALES in Value'].isin(['PRODUCT NAME', '', 'NAN'])]
                                    value_table = value_table[value_table['SALES in Value'].str.strip() != '']
                                    
                                    for col in value_table.columns[1:]:
                                        value_table[col] = pd.to_numeric(value_table[col], errors='coerce').fillna(0)
                                    numeric_cols = value_table.select_dtypes(include=[np.number]).columns
                                    value_table[numeric_cols] = value_table[numeric_cols].astype(float).round(2)
                                    auditor_ero_pw_value_table = value_table

                            def calculate_ytd_growth_achievement(data, product_col_name):
                                ytd_quarterlies = [
                                    ('Apr to Jun', months[:3]),
                                    ('Apr to Sep', months[:6]),
                                    ('Apr to Dec', months[:9]),
                                    ('Apr to Mar', months)
                                ]
                                
                                for quarter_name, quarter_months in ytd_quarterlies:
                                    actual_ytd_col = f'Act-YTD-{fiscal_year_str} ({quarter_name})'
                                    ly_ytd_col = f'YTD-{last_fiscal_year_str} ({quarter_name})LY'
                                    gr_ytd_col = f'Gr-YTD-{fiscal_year_str} ({quarter_name})'
                                    
                                    if actual_ytd_col in data.columns and ly_ytd_col in data.columns:
                                        data[gr_ytd_col] = (
                                            (data[actual_ytd_col] - data[ly_ytd_col]) /
                                            data[ly_ytd_col].replace(0, np.nan) * 100
                                        ).round(2)
                                    
                                    budget_ytd_col = f'YTD-{fiscal_year_str} ({quarter_name})Budget'
                                    ach_ytd_col = f'Ach-YTD-{fiscal_year_str} ({quarter_name})'
                                    
                                    if actual_ytd_col in data.columns and budget_ytd_col in data.columns:
                                        data[ach_ytd_col] = (
                                            data[actual_ytd_col] /
                                            data[budget_ytd_col].replace(0, np.nan) * 100
                                        ).round(2)
                                
                                return data

                            # Process MT (Tonnage) merge data
                            merged_mt_data = pd.DataFrame()
                            if (auditor_ero_pw_mt_table is not None and 
                                hasattr(st.session_state, 'ero_pw_analysis_data') and 
                                not st.session_state.ero_pw_analysis_data.empty):
                                
                                result_mt_for_merge = st.session_state.ero_pw_analysis_data.copy()
                                if 'SALES in Tonage' in result_mt_for_merge.columns:
                                    result_mt_for_merge = result_mt_for_merge.rename(columns={'SALES in Tonage': 'PRODUCT NAME'})
                                result_mt_for_merge['PRODUCT NAME'] = result_mt_for_merge['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                calc_products = set(result_mt_for_merge['PRODUCT NAME']) - {'TOTAL SALES', '', 'NAN'}
                                
                                if not auditor_ero_pw_mt_table.empty:
                                    merged_mt_data = auditor_ero_pw_mt_table.copy()
                                    merged_mt_data['SALES in Tonage'] = merged_mt_data['SALES in Tonage'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                else:
                                    merged_mt_data = pd.DataFrame({'SALES in Tonage': list(calc_products)})
                                
                                auditor_products = set(merged_mt_data['SALES in Tonage']) if not merged_mt_data.empty else set()
                                missing_products = calc_products - auditor_products
                                if missing_products:
                                    missing_df = pd.DataFrame({'SALES in Tonage': list(missing_products)})
                                    for col in merged_mt_data.columns[1:]:
                                        missing_df[col] = 0.0
                                    merged_mt_data = pd.concat([merged_mt_data, missing_df], ignore_index=True)
                                
                                common_columns = set(merged_mt_data.columns) & set(result_mt_for_merge.columns) - {'SALES in Tonage', 'PRODUCT NAME', 'Region'}
                                if common_columns:
                                    for col in common_columns:
                                        for product in merged_mt_data['SALES in Tonage']:
                                            if product in result_mt_for_merge['PRODUCT NAME'].values and product != 'TOTAL SALES':
                                                product_value = result_mt_for_merge.loc[result_mt_for_merge['PRODUCT NAME'] == product, col].values
                                                if len(product_value) > 0:
                                                    merged_mt_data.loc[merged_mt_data['SALES in Tonage'] == product, col] = product_value[0]
                                
                                for ytd_period, columns in ytd_periods.items():
                                    valid_cols = [col for col in columns if col in merged_mt_data.columns]
                                    if valid_cols:
                                        merged_mt_data[ytd_period] = merged_mt_data[valid_cols].sum(axis=1).round(2)
                                
                                merged_mt_data = calculate_ytd_growth_achievement(merged_mt_data, 'SALES in Tonage')
                                
                                if 'TOTAL SALES' not in merged_mt_data['SALES in Tonage'].values:
                                    total_sales_row = {'SALES in Tonage': 'TOTAL SALES'}
                                    numeric_cols = merged_mt_data.select_dtypes(include=[np.number]).columns
                                    for col in numeric_cols:
                                        total_sales_row[col] = merged_mt_data[~merged_mt_data['SALES in Tonage'].isin(['TOTAL SALES'])][col].sum().round(2)
                                    merged_mt_data = pd.concat([merged_mt_data, pd.DataFrame([total_sales_row])], ignore_index=True)
                                else:
                                    numeric_cols = merged_mt_data.select_dtypes(include=[np.number]).columns
                                    for col in numeric_cols:
                                        sum_value = merged_mt_data[~merged_mt_data['SALES in Tonage'].isin(['TOTAL SALES'])][col].sum().round(2)
                                        merged_mt_data.loc[merged_mt_data['SALES in Tonage'] == 'TOTAL SALES', col] = sum_value
                                
                                total_sales_row = merged_mt_data[merged_mt_data['SALES in Tonage'] == 'TOTAL SALES']
                                other_rows = merged_mt_data[merged_mt_data['SALES in Tonage'] != 'TOTAL SALES']
                                other_rows = other_rows.sort_values(by='SALES in Tonage')
                                merged_mt_data = pd.concat([other_rows, total_sales_row], ignore_index=True)
                                
                                # Store merged tonnage data in session state
                                st.session_state.merged_ero_pw_mt_data = merged_mt_data

                            # Process Value merge data
                            merged_value_data = pd.DataFrame()
                            if (auditor_ero_pw_value_table is not None and 
                                hasattr(st.session_state, 'ero_pw_value_data') and 
                                not st.session_state.ero_pw_value_data.empty):
                                
                                result_value_for_merge = st.session_state.ero_pw_value_data.copy()
                                if 'SALES in Value' in result_value_for_merge.columns:
                                    result_value_for_merge = result_value_for_merge.rename(columns={'SALES in Value': 'PRODUCT NAME'})
                                result_value_for_merge['PRODUCT NAME'] = result_value_for_merge['PRODUCT NAME'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                
                                calc_products = set(result_value_for_merge['PRODUCT NAME']) - {'TOTAL SALES', '', 'NAN'}
                                
                                if not auditor_ero_pw_value_table.empty:
                                    merged_value_data = auditor_ero_pw_value_table.copy()
                                    merged_value_data['SALES in Value'] = merged_value_data['SALES in Value'].replace([pd.NA, np.nan, None], '').apply(lambda x: str(x).strip().upper() if pd.notnull(x) else '')
                                else:
                                    merged_value_data = pd.DataFrame({'SALES in Value': list(calc_products)})
                                
                                auditor_products = set(merged_value_data['SALES in Value']) if not merged_value_data.empty else set()
                                missing_products = calc_products - auditor_products
                                if missing_products:
                                    missing_df = pd.DataFrame({'SALES in Value': list(missing_products)})
                                    for col in merged_value_data.columns[1:]:
                                        missing_df[col] = 0.0
                                    merged_value_data = pd.concat([merged_value_data, missing_df], ignore_index=True)
                                
                                common_columns = set(merged_value_data.columns) & set(result_value_for_merge.columns) - {'SALES in Value', 'PRODUCT NAME', 'Region'}
                                if common_columns:
                                    for col in common_columns:
                                        for product in merged_value_data['SALES in Value']:
                                            if product in result_value_for_merge['PRODUCT NAME'].values and product != 'TOTAL SALES':
                                                product_value = result_value_for_merge.loc[result_value_for_merge['PRODUCT NAME'] == product, col].values
                                                if len(product_value) > 0:
                                                    merged_value_data.loc[merged_value_data['SALES in Value'] == product, col] = product_value[0]
                                
                                for ytd_period, columns in ytd_periods.items():
                                    valid_cols = [col for col in columns if col in merged_value_data.columns]
                                    if valid_cols:
                                        merged_value_data[ytd_period] = merged_value_data[valid_cols].sum(axis=1).round(2)
                                
                                merged_value_data = calculate_ytd_growth_achievement(merged_value_data, 'SALES in Value')
                                
                                for col in merged_value_data.columns:
                                    if col != 'SALES in Value':
                                        merged_value_data[col] = pd.to_numeric(merged_value_data[col], errors='coerce').fillna(0)
                                
                                numeric_cols = merged_value_data.select_dtypes(include=[np.number]).columns
                                merged_value_data[numeric_cols] = merged_value_data[numeric_cols].astype(float).round(2)
                                
                                if 'TOTAL SALES' not in merged_value_data['SALES in Value'].values:
                                    total_sales_row = {'SALES in Value': 'TOTAL SALES'}
                                    for col in numeric_cols:
                                        total_sales_row[col] = merged_value_data[~merged_value_data['SALES in Value'].isin(['TOTAL SALES'])][col].sum().round(2)
                                    merged_value_data = pd.concat([merged_value_data, pd.DataFrame([total_sales_row])], ignore_index=True)
                                else:
                                    for col in numeric_cols:
                                        sum_value = merged_value_data[~merged_value_data['SALES in Value'].isin(['TOTAL SALES'])][col].sum().round(2)
                                        merged_value_data.loc[merged_value_data['SALES in Value'] == 'TOTAL SALES', col] = sum_value
                                
                                total_sales_row = merged_value_data[merged_value_data['SALES in Value'] == 'TOTAL SALES']
                                other_rows = merged_value_data[merged_value_data['SALES in Value'] != 'TOTAL SALES']
                                other_rows = other_rows.sort_values(by='SALES in Value')
                                merged_value_data = pd.concat([other_rows, total_sales_row], ignore_index=True)
                                
                                # Store merged value data in session state
                                st.session_state.merged_ero_pw_value_data = merged_value_data

                            # Display merged data
                            if not merged_mt_data.empty:
                                st.subheader(f"Merged Data (SALES in Tonage - WEST) [{fiscal_year_str}]")
                                try:
                                    styled_df = safe_format_dataframe(merged_mt_data)
                                    numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                                    formatter = {col: "{:,.2f}" for col in numeric_cols}
                                    st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                                except Exception as e:
                                    st.error(f"Error displaying merged MT dataframe: {str(e)}")
                                    st.dataframe(merged_mt_data, use_container_width=True)

                            if not merged_value_data.empty:
                                st.subheader(f"Merged Data (SALES in Value - WEST) [{fiscal_year_str}]")
                                try:
                                    styled_df = safe_format_dataframe(merged_value_data)
                                    numeric_cols = styled_df.select_dtypes(include=[np.number]).columns
                                    formatter = {col: "{:,.2f}" for col in numeric_cols}
                                    st.dataframe(styled_df.style.format(formatter), use_container_width=True)
                                except Exception as e:
                                    st.error(f"Error displaying merged Value dataframe: {str(e)}")
                                    st.dataframe(merged_value_data, use_container_width=True)

                            if not merged_mt_data.empty or not merged_value_data.empty:
                                output = BytesIO()
                                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                    workbook = writer.book
                                    worksheet = workbook.add_worksheet('Merged_ERO_PW_Data')
                                    title_format = workbook.add_format({
                                        'bold': True, 'align': 'center', 'valign': 'vcenter',
                                        'font_size': 14, 'font_color': '#000000'
                                    })
                                    header_format = workbook.add_format({
                                        'bold': True, 'text_wrap': True, 'valign': 'top',
                                        'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                                    })
                                    num_format = workbook.add_format({'num_format': '#,##0.00'})
                                    
                                    num_cols = max(
                                        len(merged_mt_data.columns) if not merged_mt_data.empty else 0,
                                        len(merged_value_data.columns) if not merged_value_data.empty else 0
                                    )
                                    worksheet.merge_range(2, 0, 2, num_cols - 1, f"ERO-PW SALES REVIEW FOR WEST REGION [{fiscal_year_str}]", title_format)
                                    start_row = 4
                                    
                                    if not merged_mt_data.empty:
                                        merged_mt_data.to_excel(writer, sheet_name='Merged_ERO_PW_Data', startrow=start_row, index=False)
                                        for col_num, value in enumerate(merged_mt_data.columns.values):
                                            worksheet.write(start_row, col_num, value, header_format)
                                        for col in merged_mt_data.select_dtypes(include=[np.number]).columns:
                                            col_idx = merged_mt_data.columns.get_loc(col)
                                            worksheet.set_column(col_idx, col_idx, None, num_format)
                                        for i, col in enumerate(merged_mt_data.columns):
                                            max_len = max((merged_mt_data[col].astype(str).str.len().max(), len(str(col)))) + 2
                                            worksheet.set_column(i, i, max_len)
                                        start_row += len(merged_mt_data) + 4
                                    
                                    if not merged_value_data.empty:
                                        merged_value_data.to_excel(writer, sheet_name='Merged_ERO_PW_Data', startrow=start_row, index=False)
                                        for col_num, value in enumerate(merged_value_data.columns.values):
                                            worksheet.write(start_row, col_num, value, header_format)
                                        for col in merged_value_data.select_dtypes(include=[np.number]).columns:
                                            col_idx = merged_value_data.columns.get_loc(col)
                                            worksheet.set_column(col_idx, col_idx, None, num_format)
                                        for i, col in enumerate(merged_value_data.columns):
                                            max_len = max((merged_value_data[col].astype(str).str.len().max(), len(str(col)))) + 2
                                            worksheet.set_column(i, i, max_len)
                                
                                excel_data = output.getvalue()
                                st.download_button(
                                    label="⬇️ Download Merged ERO-PW Data as Excel",
                                    data=excel_data,
                                    file_name=f"merged_ero_pw_data_west_{fiscal_year_str}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_ero_pw_merge"
                                )
                            else:
                                st.info("No valid merged data available to export.")

                        except Exception as e:
                            st.error(f"Error in merge preview: {str(e)}")
                    else:
                        st.info("ℹ Upload audit file and generate ERO-PW data first")
            else:
                st.warning("Required columns not found in budget data.")

        except Exception as e:
            st.error(f"Error processing ERO-PW analysis: {str(e)}")
    else:
        st.warning("Please ensure all required files are uploaded and sheets are selected.")

with tab7:
    st.header("📊 Sales Analysis Month-wise")
    
    if st.session_state.get('uploaded_file_auditor'):
        try:
            # Load the Excel file
            xls = pd.ExcelFile(st.session_state.uploaded_file_auditor)
            
            # Find the Sales Analysis Month Wise sheet
            sales_analysis_sheet = None
            for sheet in xls.sheet_names:
                if re.search(r'sales\s*analysis\s*month\s*wise', sheet.lower(), re.IGNORECASE):
                    sales_analysis_sheet = sheet
                    break
            
            if not sales_analysis_sheet:
                st.error("❌ No 'Sales Analysis Month Wise' sheet found in the uploaded auditor file.")
                st.write("Available sheets: " + ", ".join(xls.sheet_names))
            else:
                # Load the sheet with all columns as strings initially to avoid type issues
                df_sheet = pd.read_excel(xls, sheet_name=sales_analysis_sheet, header=None, dtype=str)
                
                # Define possible headers for the tables
                table1_possible_headers = [
                    "SALES in MT", "SALES IN MT", "Sales in MT", "SALES IN TONNAGE", "SALES IN TON",
                    "Tonnage", "TONNAGE", "Tonnage Sales", "Sales Tonnage", "Metric Tons", "MT Sales"
                ]
                table2_possible_headers = [
                    "SALES in Value", "SALES IN VALUE", "Sales in Value", "SALES IN RS", "VALUE SALES",
                    "Value", "VALUE", "Sales Value"
                ]
                
                # Extract table positions
                idx1, data_start1 = extract_tables(df_sheet, table1_possible_headers)
                idx2, data_start2 = extract_tables(df_sheet, table2_possible_headers)
                
                if idx1 is None:
                    st.error(f"❌ Could not locate SALES in MT table header. Tried: {', '.join(table1_possible_headers)}")
                    st.dataframe(df_sheet.head(10), use_container_width=True)
                    st.stop()
                
                # Extract SALES in MT table
                table1_end = idx2 if idx2 is not None and idx2 > idx1 else len(df_sheet)
                table1 = df_sheet.iloc[data_start1:table1_end].dropna(how='all')
                table1.columns = df_sheet.iloc[idx1]
                table1.columns = table1.columns.map(str)
                table1.columns = rename_columns(table1.columns)
                table1 = handle_duplicate_columns(table1)
                
                # Convert numeric columns to float, except the first column
                for col in table1.columns[1:]:  # Skip first column (likely text)
                    table1[col] = pd.to_numeric(table1[col], errors='coerce').fillna(0).astype(float)
                
                # Extract SALES in Value table
                table2 = None
                if idx2 is not None and idx2 > idx1:
                    table2 = df_sheet.iloc[data_start2:].dropna(how='all')
                    table2.columns = df_sheet.iloc[idx2]
                    table2.columns = table2.columns.map(str)
                    table2.columns = rename_columns(table2.columns)
                    table2 = handle_duplicate_columns(table2)
                    
                    # Convert numeric columns to float, except the first column
                    for col in table2.columns[1:]:
                        table2[col] = pd.to_numeric(table2[col], errors='coerce').fillna(0).astype(float)
                
                # Function to add ACCLLP row with totals from Tab3 and Tab4
                def add_accllp_row_with_totals(table, table_type):
                    """
                    Add ACCLLP row to table with combined totals from Tab3 and Tab4
                    Also add TOTAL SALES row with same values
                    table_type: 'MT' or 'Value'
                    """
                    if table is None or table.empty:
                        return table
                    
                    # Create a copy to avoid modifying original
                    updated_table = table.copy()
                    
                    # Initialize ACCLLP row with all columns from the table
                    accllp_row = {col: 0.0 for col in updated_table.columns}
                    first_col = updated_table.columns[0]
                    accllp_row[first_col] = 'ACCLLP'
                    
                    # Initialize TOTAL SALES row with same structure
                    total_sales_row = {col: 0.0 for col in updated_table.columns}
                    total_sales_row[first_col] = 'TOTAL SALES'
                    
                    # Get Tab3 totals (GRAND TOTAL row)
                    tab3_totals = {}
                    
                    if table_type == 'MT':
                        possible_keys = ['merged_region_mt_data', 'region_analysis_data', 'region_mt_data']
                        tab3_data = None
                        for key in possible_keys:
                            if st.session_state.get(key) is not None:
                                tab3_data = st.session_state[key]
                                break
                        
                        if tab3_data is not None:
                            id_col_variations = ['SALES in MT', 'REGIONS', 'SALES in Tonage']
                            id_col = None
                            for col_var in id_col_variations:
                                if col_var in tab3_data.columns:
                                    id_col = col_var
                                    break
                            
                            if id_col:
                                grand_total_rows = tab3_data[tab3_data[id_col].astype(str).str.upper() == 'GRAND TOTAL']
                                if not grand_total_rows.empty:
                                    grand_total_row = grand_total_rows.iloc[0]
                                    for col in tab3_data.columns:
                                        if col != id_col and col in updated_table.columns:
                                            try:
                                                value = pd.to_numeric(grand_total_row[col], errors='coerce')
                                                if pd.notna(value):
                                                    tab3_totals[col] = value
                                            except:
                                                pass
                    
                    elif table_type == 'Value':
                        possible_keys = ['merged_region_value_data', 'region_value_data']
                        tab3_data = None
                        for key in possible_keys:
                            if st.session_state.get(key) is not None:
                                tab3_data = st.session_state[key]
                                break
                        
                        if tab3_data is not None:
                            id_col_variations = ['SALES in Value', 'REGIONS']
                            id_col = None
                            for col_var in id_col_variations:
                                if col_var in tab3_data.columns:
                                    id_col = col_var
                                    break
                            
                            if id_col:
                                grand_total_rows = tab3_data[tab3_data[id_col].astype(str).str.upper() == 'GRAND TOTAL']
                                if not grand_total_rows.empty:
                                    grand_total_row = grand_total_rows.iloc[0]
                                    for col in tab3_data.columns:
                                        if col != id_col and col in updated_table.columns:
                                            try:
                                                value = pd.to_numeric(grand_total_row[col], errors='coerce')
                                                if pd.notna(value):
                                                    tab3_totals[col] = value
                                            except:
                                                pass
                    
                    # Get Tab4 totals (TOTAL SALES row)
                    tab4_totals = {}
                    
                    if table_type == 'MT':
                        possible_keys = ['merged_product_mt_data', 'product_mt_data']
                        tab4_data = None
                        for key in possible_keys:
                            if st.session_state.get(key) is not None:
                                tab4_data = st.session_state[key]
                                break
                        
                        if tab4_data is not None:
                            id_col_variations = ['SALES in Tonage', 'PRODUCT NAME', 'SALES in MT']
                            id_col = None
                            for col_var in id_col_variations:
                                if col_var in tab4_data.columns:
                                    id_col = col_var
                                    break
                            
                            if id_col:
                                total_sales_rows = tab4_data[tab4_data[id_col].astype(str).str.upper() == 'TOTAL SALES']
                                if not total_sales_rows.empty:
                                    total_sales_row_data = total_sales_rows.iloc[0]
                                    for col in tab4_data.columns:
                                        if col != id_col and col in updated_table.columns:
                                            try:
                                                value = pd.to_numeric(total_sales_row_data[col], errors='coerce')
                                                if pd.notna(value):
                                                    tab4_totals[col] = value
                                            except:
                                                pass
                    
                    elif table_type == 'Value':
                        possible_keys = ['merged_product_value_data', 'product_value_data']
                        tab4_data = None
                        for key in possible_keys:
                            if st.session_state.get(key) is not None:
                                tab4_data = st.session_state[key]
                                break
                        
                        if tab4_data is not None:
                            id_col_variations = ['SALES in Value', 'PRODUCT NAME']
                            id_col = None
                            for col_var in id_col_variations:
                                if col_var in tab4_data.columns:
                                    id_col = col_var
                                    break
                            
                            if id_col:
                                total_sales_rows = tab4_data[tab4_data[id_col].astype(str).str.upper() == 'TOTAL SALES']
                                if not total_sales_rows.empty:
                                    total_sales_row_data = total_sales_rows.iloc[0]
                                    for col in tab4_data.columns:
                                        if col != id_col and col in updated_table.columns:
                                            try:
                                                value = pd.to_numeric(total_sales_row_data[col], errors='coerce')
                                                if pd.notna(value):
                                                    tab4_totals[col] = value
                                            except:
                                                pass
                    
                    # Combine totals from Tab3 and Tab4 and update both ACCLLP and TOTAL SALES rows
                    all_cols = set(tab3_totals.keys()) | set(tab4_totals.keys())
                    for col in all_cols:
                        if col in updated_table.columns:
                            tab3_val = tab3_totals.get(col, 0)
                            tab4_val = tab4_totals.get(col, 0)
                            combined_val = tab3_val + tab4_val
                            if pd.isna(combined_val) or np.isinf(combined_val):
                                combined_val = 0.0
                            combined_rounded = round(combined_val, 2)
                            accllp_row[col] = combined_rounded
                            total_sales_row[col] = combined_rounded
                    
                    # Remove existing ACCLLP or TOTAL SALES rows if they exist
                    updated_table = updated_table[
                        ~updated_table[first_col].astype(str).str.upper().isin(['ACCLLP', 'TOTAL SALES'])
                    ].reset_index(drop=True)
                    
                    # Add both ACCLLP and TOTAL SALES rows
                    accllp_df = pd.DataFrame([accllp_row])
                    total_sales_df = pd.DataFrame([total_sales_row])
                    accllp_df = accllp_df[updated_table.columns]
                    total_sales_df = total_sales_df[updated_table.columns]
                    updated_table = pd.concat([updated_table, accllp_df, total_sales_df], ignore_index=True)
                    
                    return updated_table
                
                # Add ACCLLP row to both tables
                table1_with_accllp = add_accllp_row_with_totals(table1, 'MT')
                table2_with_accllp = add_accllp_row_with_totals(table2, 'Value')
                
                # Store updated tables in session state
                st.session_state.auditor_monthly_mt_table = table1_with_accllp
                st.session_state.auditor_monthly_value_table = table2_with_accllp
                
                # Display SALES in MT table
                st.subheader("SALES in MT")
                if table1_with_accllp is not None and not table1_with_accllp.empty:
                    try:
                        display_mt = table1_with_accllp.copy()
                        
                        
                        # Clean numeric columns
                        numeric_cols = display_mt.select_dtypes(include=[np.number]).columns
                        display_mt[numeric_cols] = display_mt[numeric_cols].replace([np.inf, -np.inf], 0)
                        display_mt[numeric_cols] = display_mt[numeric_cols].fillna(0)
                        
                        # Format numeric columns for display as strings
                        if len(display_mt) * len(display_mt.columns) <= 100000:
                            for col in numeric_cols:
                                display_mt[col] = display_mt[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "0.00")
                        
                        st.dataframe(display_mt, use_container_width=True)
                    except Exception as e:
                        
                        st.dataframe(table1_with_accllp, use_container_width=True)
                else:
                    st.error("❌ SALES in MT table not found or empty")
                
                # Display SALES in Value table
                st.subheader("SALES in Value")
                if table2_with_accllp is not None and not table2_with_accllp.empty:
                    try:
                        display_value = table2_with_accllp.copy()
                        
                        
                        
                        # Clean numeric columns
                        numeric_cols = display_value.select_dtypes(include=[np.number]).columns
                        display_value[numeric_cols] = display_value[numeric_cols].replace([np.inf, -np.inf], 0)
                        display_value[numeric_cols] = display_value[numeric_cols].fillna(0)
                        
                        # Format numeric columns for display as strings
                        if len(display_value) * len(display_value.columns) <= 100000:
                            for col in numeric_cols:
                                display_value[col] = display_value[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "0.00")
                        
                        st.dataframe(display_value, use_container_width=True)
                    except Exception as e:
                        st.warning(f"Formatting error in SALES in Value table: {str(e)} - displaying raw data")
                        st.dataframe(table2_with_accllp, use_container_width=True)
                else:
                    st.error("❌ SALES in Value table not found or empty")
                
                # Check what data was available for combination
                tab3_available = any(key in st.session_state for key in [
                    'merged_region_mt_data', 'merged_region_value_data', 'region_analysis_data', 'region_value_data'
                ])
                tab4_available = any(key in st.session_state for key in [
                    'merged_product_mt_data', 'merged_product_value_data', 'product_mt_data', 'product_value_data'
                ])
                
                # Single Excel download for both tables with ACCLLP
                if ((table1_with_accllp is not None and not table1_with_accllp.empty) or 
                    (table2_with_accllp is not None and not table2_with_accllp.empty)):
                    output = BytesIO()
                    try:
                        # Clean data before Excel export
                        table1_clean = table1_with_accllp.copy() if table1_with_accllp is not None else None
                        table2_clean = table2_with_accllp.copy() if table2_with_accllp is not None else None
                        
                        if table1_clean is not None:
                            numeric_cols = table1_clean.select_dtypes(include=[np.number]).columns
                            table1_clean[numeric_cols] = table1_clean[numeric_cols].replace([np.inf, -np.inf], 0)
                            table1_clean[numeric_cols] = table1_clean[numeric_cols].fillna(0)
                        
                        if table2_clean is not None:
                            numeric_cols = table2_clean.select_dtypes(include=[np.number]).columns
                            table2_clean[numeric_cols] = table2_clean[numeric_cols].replace([np.inf, -np.inf], 0)
                            table2_clean[numeric_cols] = table2_clean[numeric_cols].fillna(0)
                        
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            workbook = writer.book
                            worksheet = workbook.add_worksheet('Sales_Analysis_Monthly')
                            title_format = workbook.add_format({
                                'bold': True, 'align': 'center', 'valign': 'vcenter',
                                'font_size': 14, 'font_color': '#000000'
                            })
                            header_format = workbook.add_format({
                                'bold': True, 'text_wrap': True, 'valign': 'top',
                                'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                            })
                            num_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
                            accllp_format = workbook.add_format({
                                'bold': True, 'num_format': '#,##0.00', 'bg_color': '#FFE6CC', 'border': 1
                            })
                            text_format = workbook.add_format({'border': 1})
                            
                            start_row = 4
                            if table1_clean is not None and not table1_clean.empty:
                                table1_clean.to_excel(writer, sheet_name='Sales_Analysis_Monthly', startrow=start_row + 3, index=False)
                                worksheet.merge_range(
                                    start_row, 0, start_row, len(table1_clean.columns) - 1,
                                    "SALES IN MT - MONTH WISE (with ACCLLP & TOTAL SALES)", title_format
                                )
                                for col_num, value in enumerate(table1_clean.columns.values):
                                    worksheet.write(start_row + 3, col_num, value, header_format)
                                
                                first_col = table1_clean.columns[0]
                                for row_num in range(len(table1_clean)):
                                    is_accllp = str(table1_clean.iloc[row_num, 0]).upper() in ['ACCLLP', 'TOTAL SALES']
                                    for col_num in range(len(table1_clean.columns)):
                                        value = table1_clean.iloc[row_num, col_num]
                                        if col_num > 0 and pd.api.types.is_numeric_dtype(table1_clean.iloc[:, col_num]):
                                            if pd.isna(value) or np.isinf(value):
                                                value = 0.0
                                            else:
                                                value = float(value)
                                        fmt = accllp_format if is_accllp else (text_format if col_num == 0 else num_format)
                                        worksheet.write(start_row + 4 + row_num, col_num, value, fmt)
                                
                                for i, col in enumerate(table1_clean.columns):
                                    max_len = max((table1_clean[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, min(max_len, 25))
                                start_row += len(table1_clean) + 7
                            
                            if table2_clean is not None and not table2_clean.empty:
                                table2_clean.to_excel(writer, sheet_name='Sales_Analysis_Monthly', startrow=start_row + 3, index=False)
                                worksheet.merge_range(
                                    start_row, 0, start_row, len(table2_clean.columns) - 1,
                                    "SALES IN VALUE - MONTH WISE (with ACCLLP)", title_format
                                )
                                for col_num, value in enumerate(table2_clean.columns.values):
                                    worksheet.write(start_row + 3, col_num, value, header_format)
                                
                                first_col = table2_clean.columns[0]
                                for row_num in range(len(table2_clean)):
                                    is_accllp = str(table2_clean.iloc[row_num, 0]).upper() in ['ACCLLP', 'TOTAL SALES']
                                    for col_num in range(len(table2_clean.columns)):
                                        value = table2_clean.iloc[row_num, col_num]
                                        if col_num > 0 and pd.api.types.is_numeric_dtype(table2_clean.iloc[:, col_num]):
                                            if pd.isna(value) or np.isinf(value):
                                                value = 0.0
                                            else:
                                                value = float(value)
                                        fmt = accllp_format if is_accllp else (text_format if col_num == 0 else num_format)
                                        worksheet.write(start_row + 4 + row_num, col_num, value, fmt)
                                
                                for i, col in enumerate(table2_clean.columns):
                                    max_len = max((table2_clean[col].astype(str).str.len().max(), len(str(col)))) + 2
                                    worksheet.set_column(i, i, min(max_len, 25))
                            
                        output.seek(0)
                        excel_data = output.getvalue()
                        st.download_button(
                            label="⬇️ Download Sales Analysis with ACCLLP Totals",
                            data=excel_data,
                            file_name="sales_analysis_monthly_with_accllp_totals.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="sales_analysis_download_accllp_tab7"
                        )
                    except Exception as e:
                        st.error(f"Error generating Excel file: {str(e)}")
                        st.info("💡 This usually happens with invalid numeric values. The data is still displayed correctly above.")
                else:
                    st.info("ℹ️ No valid tables available to export.")
                
        except Exception as e:
            st.error(f"Error processing Sales Analysis Month Wise sheet: {str(e)}")
    else:
        st.info("ℹ️ Please upload the Auditor file first")

with tab8:
    st.header("📊 Combined Excel Data Export")
    st.write("This tab combines all merged data from different analysis tabs into a single Excel file with multiple sheets.")
    
    # Initialize session state for merged data if not exists
    if 'merged_data_storage' not in st.session_state:
        st.session_state.merged_data_storage = {
            'sales_analysis_mt': None,
            'sales_analysis_value': None,
            'region_mt': None,
            'region_value': None,
            'product_mt': None,
            'product_value': None,
            'ts_pw_mt': None,
            'ts_pw_value': None,
            'ero_pw_mt': None,
            'ero_pw_value': None
        }
    
    def auto_store_merge_data():
        """Automatically store merged data from all tabs in session state"""
        stored_count = 0
        
        # Tab7 - Sales Analysis Month-wise data
        if hasattr(st.session_state, 'auditor_monthly_mt_table') and st.session_state.auditor_monthly_mt_table is not None:
            st.session_state.merged_data_storage['sales_analysis_mt'] = st.session_state.auditor_monthly_mt_table.copy()
            stored_count += 1
        
        if hasattr(st.session_state, 'auditor_monthly_value_table') and st.session_state.auditor_monthly_value_table is not None:
            st.session_state.merged_data_storage['sales_analysis_value'] = st.session_state.auditor_monthly_value_table.copy()
            stored_count += 1
        
        # Tab3 - Region Analysis merged data
        region_mt_sources = [
            'region_merged_mt_data', 'merged_region_mt_data', 'region_analysis_merged_mt',
            'auditor_mt_table_merged', 'region_mt_merged'
        ]
        region_value_sources = [
            'region_merged_value_data', 'merged_region_value_data', 'region_analysis_merged_value',
            'auditor_value_table_merged', 'region_value_merged'
        ]
        
        for source in region_mt_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['region_mt'] = data.copy()
                    stored_count += 1
                    break
        
        for source in region_value_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['region_value'] = data.copy()
                    stored_count += 1
                    break
        
        # Tab4 - Product Analysis merged data
        product_mt_sources = [
            'product_merged_mt_data', 'merged_product_mt_data', 'product_analysis_merged_mt',
            'product_mt_merged'
        ]
        product_value_sources = [
            'product_merged_value_data', 'merged_product_value_data', 'product_analysis_merged_value',
            'product_value_merged'
        ]
        
        for source in product_mt_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['product_mt'] = data.copy()
                    stored_count += 1
                    break
        
        for source in product_value_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['product_value'] = data.copy()
                    stored_count += 1
                    break
        
        # Tab5 - TS-PW Analysis merged data
        ts_pw_mt_sources = [
            'ts_pw_merged_mt_data', 'merged_ts_pw_mt_data', 'ts_pw_analysis_merged_mt',
            'ts_pw_mt_merged'
        ]
        ts_pw_value_sources = [
            'ts_pw_merged_value_data', 'merged_ts_pw_value_data', 'ts_pw_analysis_merged_value',
            'ts_pw_value_merged'
        ]
        
        for source in ts_pw_mt_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['ts_pw_mt'] = data.copy()
                    stored_count += 1
                    break
        
        for source in ts_pw_value_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['ts_pw_value'] = data.copy()
                    stored_count += 1
                    break
        
        # Tab6 - ERO-PW Analysis merged data
        ero_pw_mt_sources = [
            'ero_pw_merged_mt_data', 'merged_ero_pw_mt_data', 'ero_pw_analysis_merged_mt',
            'ero_pw_mt_merged'
        ]
        ero_pw_value_sources = [
            'ero_pw_merged_value_data', 'merged_ero_pw_value_data', 'ero_pw_analysis_merged_value',
            'ero_pw_value_merged'
        ]
        
        for source in ero_pw_mt_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['ero_pw_mt'] = data.copy()
                    stored_count += 1
                    break
        
        for source in ero_pw_value_sources:
            if hasattr(st.session_state, source) and getattr(st.session_state, source) is not None:
                data = getattr(st.session_state, source)
                if not data.empty:
                    st.session_state.merged_data_storage['ero_pw_value'] = data.copy()
                    stored_count += 1
                    break
        
        return stored_count
    
    def create_combined_excel():
        """Create combined Excel file with all merged data"""
        try:
            output = BytesIO()
            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                workbook = writer.book
                
                # Define formats
                title_format = workbook.add_format({
                    'bold': True, 'align': 'center', 'valign': 'vcenter',
                    'font_size': 16, 'font_color': '#000000', 'bg_color': '#D9E1F2'
                })
                header_format = workbook.add_format({
                    'bold': True, 'text_wrap': True, 'valign': 'top', 'align': 'center',
                    'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
                })
                num_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
                text_format = workbook.add_format({'border': 1, 'valign': 'vcenter'})
                total_format = workbook.add_format({
                    'bold': True, 'num_format': '#,##0.00', 'bg_color': '#E2EFDA', 'border': 1
                })
                accllp_format = workbook.add_format({
                    'bold': True, 'num_format': '#,##0.00', 'bg_color': '#90EE90', 'border': 1
                })
                
                def clean_dataframe_for_excel(df):
                    """Clean DataFrame to remove NaN, INF values and ensure proper formatting"""
                    if df is None or df.empty:
                        return df
                    
                    df_clean = df.copy()
                    
                    # Replace NaN and INF values with 0 or empty string
                    for col in df_clean.columns:
                        if df_clean[col].dtype in ['float64', 'int64', 'float32', 'int32']:
                            df_clean[col] = df_clean[col].replace([np.nan, np.inf, -np.inf], 0)
                            df_clean[col] = df_clean[col].apply(lambda x: 0 if not np.isfinite(x) else x)
                        else:
                            df_clean[col] = df_clean[col].fillna('')
                            df_clean[col] = df_clean[col].astype(str).replace(['nan', 'NaN', 'inf', '-inf'], '')
                    
                    return df_clean
                
                sheets_created = 0
                
                # Sheet 1: Sales Analysis Month wise
                if (st.session_state.merged_data_storage['sales_analysis_mt'] is not None or 
                    st.session_state.merged_data_storage['sales_analysis_value'] is not None):
                    
                    worksheet = workbook.add_worksheet('Sales Analysis Month wise')
                    start_row = 2
                    
                    if st.session_state.merged_data_storage['sales_analysis_mt'] is not None:
                        mt_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['sales_analysis_mt'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(mt_data.columns)-1,
                                            "SALES IN MT - MONTH WISE ANALYSIS", title_format)
                        start_row += 2
                        
                        mt_data.to_excel(writer, sheet_name='Sales Analysis Month wise', 
                                       startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(mt_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(mt_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(mt_data.iloc[row_idx, 0]).strip().upper()
                            is_special = (first_col_val in ['ACCLLP', 'TOTAL SALES', 'GRAND TOTAL', 'TOTALSALES'] 
                                        or 'TOTAL' in first_col_val)
                            
                            for col_idx in range(len(mt_data.columns)):
                                value = mt_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                if col_idx == 0:
                                    fmt = total_format if is_special else text_format
                                else:
                                    fmt = accllp_format if first_col_val == 'ACCLLP' else (total_format if is_special else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                        
                        start_row += len(mt_data) + 3
                    
                    if st.session_state.merged_data_storage['sales_analysis_value'] is not None:
                        value_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['sales_analysis_value'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(value_data.columns)-1,
                                            "SALES IN VALUE - MONTH WISE ANALYSIS", title_format)
                        start_row += 2
                        
                        value_data.to_excel(writer, sheet_name='Sales Analysis Month wise', 
                                          startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(value_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(value_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(value_data.iloc[row_idx, 0]).strip().upper()
                            is_special = (first_col_val in ['ACCLLP', 'TOTAL SALES', 'GRAND TOTAL', 'TOTALSALES'] 
                                        or 'TOTAL' in first_col_val)
                            
                            for col_idx in range(len(value_data.columns)):
                                value = value_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                if col_idx == 0:
                                    fmt = total_format if is_special else text_format
                                else:
                                    fmt = accllp_format if first_col_val == 'ACCLLP' else (total_format if is_special else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                    
                    sheets_created += 1
                
                # Sheet 2: Region wise analysis
                if (st.session_state.merged_data_storage['region_mt'] is not None or 
                    st.session_state.merged_data_storage['region_value'] is not None):
                    
                    worksheet = workbook.add_worksheet('Region wise analysis')
                    start_row = 2
                    
                    if st.session_state.merged_data_storage['region_mt'] is not None:
                        mt_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['region_mt'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(mt_data.columns)-1,
                                            "REGION WISE SALES - TONNAGE DATA", title_format)
                        start_row += 2
                        
                        mt_data.to_excel(writer, sheet_name='Region wise analysis', 
                                       startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(mt_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(mt_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(mt_data.iloc[row_idx, 0]).strip().upper()
                            is_total = any(total_word in first_col_val for total_word in 
                                         ['TOTAL', 'GRAND', 'NORTH', 'WEST', 'SALES'])
                            
                            for col_idx in range(len(mt_data.columns)):
                                value = mt_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                        
                        start_row += len(mt_data) + 3
                    
                    if st.session_state.merged_data_storage['region_value'] is not None:
                        value_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['region_value'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(value_data.columns)-1,
                                            "REGION WISE SALES - VALUE DATA", title_format)
                        start_row += 2
                        
                        value_data.to_excel(writer, sheet_name='Region wise analysis', 
                                          startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(value_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(value_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(value_data.iloc[row_idx, 0]).strip().upper()
                            is_total = any(total_word in first_col_val for total_word in 
                                         ['TOTAL', 'GRAND', 'NORTH', 'WEST', 'SALES'])
                            
                            for col_idx in range(len(value_data.columns)):
                                value = value_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                    
                    sheets_created += 1
                
                # Sheet 3: Product wise analysis
                if (st.session_state.merged_data_storage['product_mt'] is not None or 
                    st.session_state.merged_data_storage['product_value'] is not None):
                    
                    worksheet = workbook.add_worksheet('Product wise analysis')
                    start_row = 2
                    
                    if st.session_state.merged_data_storage['product_mt'] is not None:
                        mt_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['product_mt'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(mt_data.columns)-1,
                                            "PRODUCT WISE SALES - TONNAGE DATA", title_format)
                        start_row += 2
                        
                        mt_data.to_excel(writer, sheet_name='Product wise analysis', 
                                       startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(mt_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(mt_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(mt_data.iloc[row_idx, 0]).strip().upper()
                            is_total = 'TOTAL' in first_col_val
                            
                            for col_idx in range(len(mt_data.columns)):
                                value = mt_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                        
                        start_row += len(mt_data) + 3
                    
                    if st.session_state.merged_data_storage['product_value'] is not None:
                        value_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['product_value'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(value_data.columns)-1,
                                            "PRODUCT WISE SALES - VALUE DATA", title_format)
                        start_row += 2
                        
                        value_data.to_excel(writer, sheet_name='Product wise analysis', 
                                          startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(value_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(value_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(value_data.iloc[row_idx, 0]).strip().upper()
                            is_total = 'TOTAL' in first_col_val
                            
                            for col_idx in range(len(value_data.columns)):
                                value = value_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                    
                    sheets_created += 1
                
                # Sheet 4: TS-PW
                if (st.session_state.merged_data_storage['ts_pw_mt'] is not None or 
                    st.session_state.merged_data_storage['ts_pw_value'] is not None):
                    
                    worksheet = workbook.add_worksheet('TS-PW')
                    start_row = 2
                    
                    if st.session_state.merged_data_storage['ts_pw_mt'] is not None:
                        mt_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['ts_pw_mt'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(mt_data.columns)-1,
                                            "TS-PW SALES - TONNAGE DATA (NORTH)", title_format)
                        start_row += 2
                        
                        mt_data.to_excel(writer, sheet_name='TS-PW', 
                                       startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(mt_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(mt_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(mt_data.iloc[row_idx, 0]).strip().upper()
                            is_total = 'TOTAL' in first_col_val
                            
                            for col_idx in range(len(mt_data.columns)):
                                value = mt_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                        
                        start_row += len(mt_data) + 3
                    
                    if st.session_state.merged_data_storage['ts_pw_value'] is not None:
                        value_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['ts_pw_value'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(value_data.columns)-1,
                                            "TS-PW SALES - VALUE DATA (NORTH)", title_format)
                        start_row += 2
                        
                        value_data.to_excel(writer, sheet_name='TS-PW', 
                                          startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(value_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(value_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(value_data.iloc[row_idx, 0]).strip().upper()
                            is_total = 'TOTAL' in first_col_val
                            
                            for col_idx in range(len(value_data.columns)):
                                value = value_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                    
                    sheets_created += 1
                
                # Sheet 5: ERO-PW
                if (st.session_state.merged_data_storage['ero_pw_mt'] is not None or 
                    st.session_state.merged_data_storage['ero_pw_value'] is not None):
                    
                    worksheet = workbook.add_worksheet('ERO-PW')
                    start_row = 2
                    
                    if st.session_state.merged_data_storage['ero_pw_mt'] is not None:
                        mt_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['ero_pw_mt'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(mt_data.columns)-1,
                                            "ERO-PW SALES - TONNAGE DATA (WEST)", title_format)
                        start_row += 2
                        
                        mt_data.to_excel(writer, sheet_name='ERO-PW', 
                                       startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(mt_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(mt_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(mt_data.iloc[row_idx, 0]).strip().upper()
                            is_total = 'TOTAL' in first_col_val
                            
                            for col_idx in range(len(mt_data.columns)):
                                value = mt_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                        
                        start_row += len(mt_data) + 3
                    
                    if st.session_state.merged_data_storage['ero_pw_value'] is not None:
                        value_data = clean_dataframe_for_excel(st.session_state.merged_data_storage['ero_pw_value'])
                        
                        worksheet.merge_range(start_row, 0, start_row, len(value_data.columns)-1,
                                            "ERO-PW SALES - VALUE DATA (WEST)", title_format)
                        start_row += 2
                        
                        value_data.to_excel(writer, sheet_name='ERO-PW', 
                                          startrow=start_row, index=False)
                        
                        for col_num, value in enumerate(value_data.columns):
                            worksheet.write(start_row, col_num, value, header_format)
                        
                        for row_idx in range(len(value_data)):
                            excel_row = start_row + 1 + row_idx
                            first_col_val = str(value_data.iloc[row_idx, 0]).strip().upper()
                            is_total = 'TOTAL' in first_col_val
                            
                            for col_idx in range(len(value_data.columns)):
                                value = value_data.iloc[row_idx, col_idx]
                                
                                if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                                    safe_value = 0 if col_idx > 0 else ''
                                else:
                                    try:
                                        if col_idx > 0:
                                            safe_value = float(value) if value != '' else 0
                                            if not np.isfinite(safe_value):
                                                safe_value = 0
                                        else:
                                            safe_value = str(value)
                                    except (ValueError, TypeError, OverflowError):
                                        safe_value = 0 if col_idx > 0 else str(value)
                                
                                fmt = total_format if is_total else (text_format if col_idx == 0 else num_format)
                                worksheet.write(excel_row, col_idx, safe_value, fmt)
                    
                    sheets_created += 1
                
                # Auto-adjust column widths for all sheets
                for sheet_name in workbook.worksheets():
                    worksheet = sheet_name
                    for col_idx in range(50):
                        worksheet.set_column(col_idx, col_idx, 15)
            
            output.seek(0)
            return output.getvalue(), sheets_created
            
        except Exception as e:
            st.error(f"Error creating combined Excel file: {str(e)}")
            return None, 0
    
    # Auto-store merged data
    stored_count = auto_store_merge_data()
    
    # Display current status
    st.subheader("📋 Available Data Summary")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Sales Analysis (Tab7):**")
        mt_status = "✅ Available" if st.session_state.merged_data_storage['sales_analysis_mt'] is not None else "❌ Not Available"
        value_status = "✅ Available" if st.session_state.merged_data_storage['sales_analysis_value'] is not None else "❌ Not Available"
        st.write(f"- MT Data: {mt_status}")
        st.write(f"- Value Data: {value_status}")
        
        st.write("**Region Analysis (Tab3):**")
        mt_status = "✅ Available" if st.session_state.merged_data_storage['region_mt'] is not None else "❌ Not Available"
        value_status = "✅ Available" if st.session_state.merged_data_storage['region_value'] is not None else "❌ Not Available"
        st.write(f"- MT Data: {mt_status}")
        st.write(f"- Value Data: {value_status}")
        
        st.write("**Product Analysis (Tab4):**")
        mt_status = "✅ Available" if st.session_state.merged_data_storage['product_mt'] is not None else "❌ Not Available"
        value_status = "✅ Available" if st.session_state.merged_data_storage['product_value'] is not None else "❌ Not Available"
        st.write(f"- MT Data: {mt_status}")
        st.write(f"- Value Data: {value_status}")
    
    with col2:
        st.write("**TS-PW Analysis (Tab5):**")
        mt_status = "✅ Available" if st.session_state.merged_data_storage['ts_pw_mt'] is not None else "❌ Not Available"
        value_status = "✅ Available" if st.session_state.merged_data_storage['ts_pw_value'] is not None else "❌ Not Available"
        st.write(f"- MT Data: {mt_status}")
        st.write(f"- Value Data: {value_status}")
        
        st.write("**ERO-PW Analysis (Tab6):**")
        mt_status = "✅ Available" if st.session_state.merged_data_storage['ero_pw_mt'] is not None else "❌ Not Available"
        value_status = "✅ Available" if st.session_state.merged_data_storage['ero_pw_value'] is not None else "❌ Not Available"
        st.write(f"- MT Data: {mt_status}")
        st.write(f"- Value Data: {value_status}")
    
    # Manual refresh button
    if st.button("🔄 Refresh Data from All Tabs", type="secondary"):
        with st.spinner("Refreshing data from all tabs..."):
            stored_count = auto_store_merge_data()
            if stored_count > 0:
                st.success(f"✅ Successfully refreshed {stored_count} datasets!")
                st.rerun()
            else:
                st.warning("⚠️ No merged data found to refresh. Please generate merged data in respective tabs first.")
    
    # Generate combined Excel
    available_datasets = sum(1 for data in st.session_state.merged_data_storage.values() if data is not None)
    
    if available_datasets > 0:
        st.subheader("📄 Generate Combined Excel File")
        st.info(f"📊 Found {available_datasets} datasets ready for export across multiple sheets")
        
        # Show expected sheets
        expected_sheets = []
        if (st.session_state.merged_data_storage['sales_analysis_mt'] is not None or 
            st.session_state.merged_data_storage['sales_analysis_value'] is not None):
            expected_sheets.append("Sheet 1: Sales Analysis Month wise")
        
        if (st.session_state.merged_data_storage['region_mt'] is not None or 
            st.session_state.merged_data_storage['region_value'] is not None):
            expected_sheets.append("Sheet 2: Region wise analysis")
        
        if (st.session_state.merged_data_storage['product_mt'] is not None or 
            st.session_state.merged_data_storage['product_value'] is not None):
            expected_sheets.append("Sheet 3: Product wise analysis")
        
        if (st.session_state.merged_data_storage['ts_pw_mt'] is not None or 
            st.session_state.merged_data_storage['ts_pw_value'] is not None):
            expected_sheets.append("Sheet 4: TS-PW")
        
        if (st.session_state.merged_data_storage['ero_pw_mt'] is not None or 
            st.session_state.merged_data_storage['ero_pw_value'] is not None):
            expected_sheets.append("Sheet 5: ERO-PW")
        
        if expected_sheets:
            st.write("**Expected Excel Sheets:**")
            for sheet in expected_sheets:
                st.write(f"- {sheet}")
        
        # Generate Excel button
        if st.button("📊 Generate Combined Excel File", type="primary", use_container_width=True):
            with st.spinner("Creating combined Excel file with all merged data..."):
                excel_data, sheets_created = create_combined_excel()
                
                if excel_data and sheets_created > 0:
                    st.success(f"✅ Excel file created successfully with {sheets_created} sheets!")
                    
                    # Show data summary
                    st.write("**📋 Excel File Contents:**")
                    sheet_contents = []
                    
                    if st.session_state.merged_data_storage['sales_analysis_mt'] is not None:
                        mt_rows = len(st.session_state.merged_data_storage['sales_analysis_mt'])
                        sheet_contents.append(f"- Sales Analysis MT: {mt_rows} rows")
                    
                    if st.session_state.merged_data_storage['sales_analysis_value'] is not None:
                        value_rows = len(st.session_state.merged_data_storage['sales_analysis_value'])
                        sheet_contents.append(f"- Sales Analysis Value: {value_rows} rows")
                    
                    if st.session_state.merged_data_storage['region_mt'] is not None:
                        region_mt_rows = len(st.session_state.merged_data_storage['region_mt'])
                        sheet_contents.append(f"- Region Analysis MT: {region_mt_rows} rows")
                    
                    if st.session_state.merged_data_storage['region_value'] is not None:
                        region_value_rows = len(st.session_state.merged_data_storage['region_value'])
                        sheet_contents.append(f"- Region Analysis Value: {region_value_rows} rows")
                    
                    if st.session_state.merged_data_storage['product_mt'] is not None:
                        product_mt_rows = len(st.session_state.merged_data_storage['product_mt'])
                        sheet_contents.append(f"- Product Analysis MT: {product_mt_rows} rows")
                    
                    if st.session_state.merged_data_storage['product_value'] is not None:
                        product_value_rows = len(st.session_state.merged_data_storage['product_value'])
                        sheet_contents.append(f"- Product Analysis Value: {product_value_rows} rows")
                    
                    if st.session_state.merged_data_storage['ts_pw_mt'] is not None:
                        ts_pw_mt_rows = len(st.session_state.merged_data_storage['ts_pw_mt'])
                        sheet_contents.append(f"- TS-PW Analysis MT: {ts_pw_mt_rows} rows")
                    
                    if st.session_state.merged_data_storage['ts_pw_value'] is not None:
                        ts_pw_value_rows = len(st.session_state.merged_data_storage['ts_pw_value'])
                        sheet_contents.append(f"- TS-PW Analysis Value: {ts_pw_value_rows} rows")
                    
                    if st.session_state.merged_data_storage['ero_pw_mt'] is not None:
                        ero_pw_mt_rows = len(st.session_state.merged_data_storage['ero_pw_mt'])
                        sheet_contents.append(f"- ERO-PW Analysis MT: {ero_pw_mt_rows} rows")
                    
                    if st.session_state.merged_data_storage['ero_pw_value'] is not None:
                        ero_pw_value_rows = len(st.session_state.merged_data_storage['ero_pw_value'])
                        sheet_contents.append(f"- ERO-PW Analysis Value: {ero_pw_value_rows} rows")
                    
                    for content in sheet_contents:
                        st.write(content)
                    
                    # Download button
                    st.download_button(
                        label="⬇️ Download Complete Auditor Excel File",
                        data=excel_data,
                        file_name="Auditor Format.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="combined_excel_download",
                        use_container_width=True
                    )
                    
            
                
                else:
                    st.error("❌ Failed to create Excel file. Please check if merged data is available.")
    
    else:
        st.warning("⚠️ **No merged data available for export**")
        st.info("📝 **To generate combined Excel file:**")
        st.write("1. Go to **Tab3 (Region Analysis)** → Generate data → Go to 'Merge Preview' tab")
        st.write("2. Go to **Tab4 (Product Analysis)** → Generate data → Go to 'Merge Preview' tab") 
        st.write("3. Go to **Tab5 (TS-PW Analysis)** → Generate data → Go to 'Merge Preview' tab")
        st.write("4. Go to **Tab6 (ERO-PW Analysis)** → Generate data → Go to 'Merge Preview' tab")
        st.write("5. Go to **Tab7 (Sales Analysis)** → Process the monthly analysis")
        st.write("6. Return to this tab and click 'Refresh Data' then 'Generate Combined Excel'")
        
        # Debug information
        with st.expander("🔍 Debug Information", expanded=False):
            st.write("**Session State Keys Related to Merged Data:**")
            merged_keys = [key for key in st.session_state.keys() if 'merge' in key.lower() or 'auditor' in key.lower()]
            if merged_keys:
                for key in sorted(merged_keys):
                    value = getattr(st.session_state, key, None)
                    if value is not None:
                        if hasattr(value, 'shape'):
                            st.write(f"- {key}: DataFrame with shape {value.shape}")
                        else:
                            st.write(f"- {key}: {type(value)}")
                    else:
                        st.write(f"- {key}: None")
            else:
                st.write("No merged data keys found in session state")
            
            st.write("**Current Storage Status:**")
            for key, value in st.session_state.merged_data_storage.items():
                status = f"✅ {value.shape}" if value is not None else "❌ None"
                st.write(f"- {key}: {status}")
