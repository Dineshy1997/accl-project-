import streamlit as st
import pandas as pd
import numpy as np
import io
import os
import json
import datetime
import pickle
import openpyxl
from openpyxl.utils import get_column_letter

DATA_DIR = "app_data"
METADATA_PATH = os.path.join(DATA_DIR, "metadata.pickle")
BRANCH_MAPPING_PATH = os.path.join(DATA_DIR, "branch_mappings.pickle")
REGION_MAPPING_PATH = os.path.join(DATA_DIR, "region_mappings.pickle")
COMPANY_MAPPING_PATH = os.path.join(DATA_DIR, "company_mappings.pickle")
BACKUP_PATH = os.path.join(DATA_DIR, "backup.json")

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def to_excel_buffer(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output

def init_session_state():
    if 'initialized' not in st.session_state:
        st.session_state.initialized = True
        st.session_state.budget_df = None
        st.session_state.processed_budget = None
        st.session_state.processed_sales = None
        st.session_state.processed_os = None  # Added OS processing state
        st.session_state.customer_col = None
        st.session_state.exec_col = None
        st.session_state.product_col = None
        st.session_state.customer_name_col = None
        st.session_state.exec_code_col = None
        st.session_state.executives = []
        st.session_state.executive_codes = {}
        st.session_state.product_groups = []
        st.session_state.customer_codes = {}
        st.session_state.customer_names = {}
        st.session_state.unmapped_customers = []
        st.session_state.branch_exec_mapping = {}
        st.session_state.region_branch_mapping = {}
        st.session_state.company_product_mapping = {}
        st.session_state.update_logs = {
            'executive_management': [],
            'branch_region_mapping': [],
            'company_product_mapping': []
        }
        load_all_mappings()

def save_metadata():
    ensure_data_dir()
    metadata = {
        "executives": st.session_state.executives,
        "executive_codes": st.session_state.executive_codes,
        "product_groups": st.session_state.product_groups,
        "customer_codes": st.session_state.customer_codes,
        "customer_names": st.session_state.customer_names,
        "unmapped_customers": st.session_state.unmapped_customers,
        "update_logs": st.session_state.update_logs
    }
    with open(METADATA_PATH, 'wb') as f:
        pickle.dump(metadata, f)

def load_metadata():
    if os.path.exists(METADATA_PATH):
        try:
            with open(METADATA_PATH, 'rb') as f:
                metadata = pickle.load(f)
                st.session_state.executives = metadata.get("executives", [])
                st.session_state.executive_codes = metadata.get("executive_codes", {})
                st.session_state.product_groups = metadata.get("product_groups", [])
                st.session_state.customer_codes = metadata.get("customer_codes", {})
                st.session_state.customer_names = metadata.get("customer_names", {})
                st.session_state.unmapped_customers = metadata.get("unmapped_customers", [])
                st.session_state.update_logs = metadata.get("update_logs", {
                    'executive_management': [],
                    'branch_region_mapping': [],
                    'company_product_mapping': []
                })
            return True
        except Exception as e:
            st.error(f"Error loading metadata: {e}")
    return False

def save_branch_mappings():
    ensure_data_dir()
    with open(BRANCH_MAPPING_PATH, 'wb') as f:
        pickle.dump(st.session_state.branch_exec_mapping, f)

def load_branch_mappings():
    if os.path.exists(BRANCH_MAPPING_PATH):
        try:
            with open(BRANCH_MAPPING_PATH, 'rb') as f:
                st.session_state.branch_exec_mapping = pickle.load(f)
            return True
        except Exception as e:
            st.error(f"Error loading branch mappings: {e}")
    return False

def save_region_mappings():
    ensure_data_dir()
    with open(REGION_MAPPING_PATH, 'wb') as f:
        pickle.dump(st.session_state.region_branch_mapping, f)

def load_region_mappings():
    if os.path.exists(REGION_MAPPING_PATH):
        try:
            with open(REGION_MAPPING_PATH, 'rb') as f:
                st.session_state.region_branch_mapping = pickle.load(f)
            return True
        except Exception as e:
            st.error(f"Error loading region mappings: {e}")
    return False

def save_company_mappings():
    ensure_data_dir()
    with open(COMPANY_MAPPING_PATH, 'wb') as f:
        pickle.dump(st.session_state.company_product_mapping, f)

def load_company_mappings():
    if os.path.exists(COMPANY_MAPPING_PATH):
        try:
            with open(COMPANY_MAPPING_PATH, 'rb') as f:
                st.session_state.company_product_mapping = pickle.load(f)
            return True
        except Exception as e:
            st.error(f"Error loading company mappings: {e}")
    return False

def save_all_mappings():
    save_metadata()
    save_branch_mappings()
    save_region_mappings()
    save_company_mappings()
    return True

def load_all_mappings():
    load_metadata()
    load_branch_mappings()
    load_region_mappings()
    load_company_mappings()
    return True

def reset_all_mappings():
    st.session_state.executives = []
    st.session_state.executive_codes = {}
    st.session_state.product_groups = []
    st.session_state.customer_codes = {}
    st.session_state.customer_names = {}
    st.session_state.unmapped_customers = []
    st.session_state.branch_exec_mapping = {}
    st.session_state.region_branch_mapping = {}
    st.session_state.company_product_mapping = {}
    st.session_state.update_logs = {
        'executive_management': [],
        'branch_region_mapping': [],
        'company_product_mapping': []
    }
    save_all_mappings()
    return True

def export_all_mappings():
    ensure_data_dir()
    all_data = {
        "metadata": {
            "executives": st.session_state.executives,
            "executive_codes": st.session_state.executive_codes,
            "product_groups": st.session_state.product_groups,
            "customer_codes": st.session_state.customer_codes,
            "customer_names": st.session_state.customer_names,
            "unmapped_customers": st.session_state.unmapped_customers
        },
        "branch_exec_mapping": st.session_state.branch_exec_mapping,
        "region_branch_mapping": st.session_state.region_branch_mapping,
        "company_product_mapping": st.session_state.company_product_mapping
    }
    json_data = json.dumps(all_data, indent=4)
    st.download_button(
        "Download Backup File",
        json_data,
        "executive_mapping_backup.json",
        "application/json",
        key="download_backup"
    )

def import_mappings_from_file(file):
    try:
        file_content = file.read()
        data = json.loads(file_content)
        metadata = data.get("metadata", {})
        st.session_state.executives = metadata.get("executives", [])
        st.session_state.executive_codes = metadata.get("executive_codes", {})
        st.session_state.product_groups = metadata.get("product_groups", [])
        st.session_state.customer_codes = metadata.get("customer_codes", {})
        st.session_state.customer_names = metadata.get("customer_names", {})
        st.session_state.unmapped_customers = metadata.get("unmapped_customers", [])
        st.session_state.branch_exec_mapping = data.get("branch_exec_mapping", {})
        st.session_state.region_branch_mapping = data.get("region_branch_mapping", {})
        st.session_state.company_product_mapping = data.get("company_product_mapping", {})
        save_all_mappings()
        st.success("Successfully restored all mappings from backup file")
        return True
    except Exception as e:
        st.error(f"Error importing mappings: {e}")
        return False

def log_update(log_type, action, details):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.update_logs[log_type].append({
        "timestamp": timestamp,
        "action": action,
        "details": details
    })
    save_metadata()

def reprocess_budget_data():
    if st.session_state.budget_df is not None and st.session_state.customer_col:
        processed_budget = process_budget_file(
            st.session_state.budget_df, 
            st.session_state.customer_col
        )
        st.session_state.processed_budget = processed_budget
        st.sidebar.success("Budget file reprocessed with current mappings!")
    else:
        st.sidebar.warning("No budget data available to reprocess")

def extract_customer_executive_mapping(df, customer_col, exec_col, name_col=None):
    customer_exec_map = {}
    customer_name_map = {}
    for _, row in df.iterrows():
        if pd.notna(row[customer_col]) and pd.notna(row[exec_col]):
            customer_code = str(row[customer_col]).strip()
            exec_name = str(row[exec_col]).strip()
            customer_exec_map[customer_code] = exec_name
            if name_col and pd.notna(row[name_col]):
                customer_name = str(row[name_col]).strip()
                customer_name_map[customer_code] = customer_name
    return customer_exec_map, customer_name_map

def extract_data_from_budget(budget_df):
    executives = []
    executive_codes = {}
    if 'exec_col' in st.session_state and st.session_state.exec_col:
        exec_col = st.session_state.exec_col
        if exec_col in budget_df.columns:
            exec_data = budget_df[budget_df[exec_col].notna()][exec_col].unique()
            executives = [str(exec_name).strip() for exec_name in exec_data]
    product_groups = []
    if 'product_col' in st.session_state and st.session_state.product_col:
        product_col = st.session_state.product_col
        if product_col and product_col in budget_df.columns:
            product_data = budget_df[budget_df[product_col].notna()][product_col].unique()
            product_groups = [str(product).strip() for product in product_data]
    results = {
        "executives": pd.DataFrame({"Executive Name": executives}),
        "executive_codes": executive_codes,
        "product_groups": pd.DataFrame({"Product Group": product_groups})
    }
    return results

def get_sheet_names(file):
    try:
        excel_file = pd.ExcelFile(file)
        return excel_file.sheet_names
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        return []

def get_sheet_preview(file, sheet_name, header_row=0):
    try:
        df = pd.read_excel(file, sheet_name=sheet_name, header=header_row)
        return df
    except Exception as e:
        st.error(f"Error loading sheet {sheet_name}: {e}")
        return None

def get_customer_codes_for_executive(exec_name):
    customer_codes = []
    for code, executive in st.session_state.customer_codes.items():
        if executive == exec_name:
            customer_codes.append(code)
    return customer_codes

def remove_executive(exec_name):
    if exec_name in st.session_state.executives:
        customer_codes = get_customer_codes_for_executive(exec_name)
        st.session_state.executives.remove(exec_name)
        if exec_name in st.session_state.executive_codes:
            del st.session_state.executive_codes[exec_name]
        for branch, execs in st.session_state.branch_exec_mapping.items():
            if exec_name in execs:
                st.session_state.branch_exec_mapping[branch].remove(exec_name)
        for code in customer_codes:
            if code in st.session_state.customer_codes:
                del st.session_state.customer_codes[code]
                if code not in st.session_state.unmapped_customers:
                    st.session_state.unmapped_customers.append(code)
        save_all_mappings()
        log_update('executive_management', 'Remove Executive', f"Removed executive {exec_name} and unmapped {len(customer_codes)} customers")
        return True, len(customer_codes)
    return False, 0

def remove_branch(branch_name):
    if branch_name in st.session_state.branch_exec_mapping:
        del st.session_state.branch_exec_mapping[branch_name]
        for region, branches in st.session_state.region_branch_mapping.items():
            if branch_name in branches:
                st.session_state.region_branch_mapping[region].remove(branch_name)
        save_all_mappings()
        log_update('branch_region_mapping', 'Remove Branch', f"Removed branch {branch_name}")
        return True
    return False

def remove_region(region_name):
    if region_name in st.session_state.region_branch_mapping:
        del st.session_state.region_branch_mapping[region_name]
        save_all_mappings()
        log_update('branch_region_mapping', 'Remove Region', f"Removed region {region_name}")
        return True
    return False

def remove_product_group(product_name):
    if product_name in st.session_state.product_groups:
        st.session_state.product_groups.remove(product_name)
        for company, products in st.session_state.company_product_mapping.items():
            if product_name in products:
                st.session_state.company_product_mapping[company].remove(product_name)
        save_all_mappings()
        log_update('company_product_mapping', 'Remove Product Group', f"Removed product group {product_name}")
        return True
    return False

def remove_company_group(company_name):
    if company_name in st.session_state.company_product_mapping:
        del st.session_state.company_product_mapping[company_name]
        save_all_mappings()
        log_update('company_product_mapping', 'Remove Company Group', f"Removed company group {company_name}")
        return True
    return False

def remove_customer_codes(exec_name, codes):
    count = 0
    for code in codes:
        if code in st.session_state.customer_codes and st.session_state.customer_codes[code] == exec_name:
            del st.session_state.customer_codes[code]
            if code not in st.session_state.unmapped_customers:
                st.session_state.unmapped_customers.append(code)
            count += 1
    if count > 0:
        save_metadata()
    return count

def assign_customer_codes(exec_name, codes):
    count = 0
    for code in codes:
        st.session_state.customer_codes[code] = exec_name
        if code in st.session_state.unmapped_customers:
            st.session_state.unmapped_customers.remove(code)
        count += 1
    if count > 0:
        save_metadata()
    return count

def get_customer_info_string(code):
    name = st.session_state.customer_names.get(code, "")
    if name:
        return f"{code} - {name}"
    else:
        return code

def get_branches_for_executive(exec_name):
    branches = []
    for branch, execs in st.session_state.branch_exec_mapping.items():
        if exec_name in execs:
            branches.append(branch)
    return ", ".join(sorted(branches)) if branches else ""

def get_region_for_branch(branch_name):
    for region, branches in st.session_state.region_branch_mapping.items():
        if branch_name in branches:
            return region
    return ""

def get_company_for_product(product_name):
    for company, products in st.session_state.company_product_mapping.items():
        if product_name in products:
            return company
    return ""

def extract_executive_customer_from_file(df, exec_col, cust_col, exec_code_col="None", cust_name_col="None", add_all_execs=True):
    relationships = {}
    exec_codes = {}
    cust_names = {}
    all_execs = set()
    for _, row in df.iterrows():
        if pd.notna(row[exec_col]) and pd.notna(row[cust_col]):
            exec_name = str(row[exec_col]).strip()
            cust_code = str(row[cust_col]).strip()
            relationships[cust_code] = exec_name
            all_execs.add(exec_name)
            if exec_code_col != "None" and pd.notna(row[exec_code_col]):
                exec_code = str(row[exec_code_col]).strip()
                exec_codes[exec_name] = exec_code
            if cust_name_col != "None" and pd.notna(row[cust_name_col]):
                cust_name = str(row[cust_name_col]).strip()
                cust_names[cust_code] = cust_name
        elif add_all_execs and pd.notna(row[exec_col]):
            exec_name = str(row[exec_col]).strip()
            all_execs.add(exec_name)
            if exec_code_col != "None" and pd.notna(row[exec_code_col]):
                exec_code = str(row[exec_code_col]).strip()
                exec_codes[exec_name] = exec_code
    return relationships, exec_codes, cust_names

def process_budget_file(budget_df, customer_col):
    processed_df = budget_df.copy()
    exec_col = st.session_state.exec_col if st.session_state.exec_col in processed_df.columns else None
    exec_code_col = st.session_state.exec_code_col if st.session_state.exec_code_col and st.session_state.exec_code_col in processed_df.columns else None
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    if st.session_state.product_col and "Company Group" not in processed_df.columns:
        processed_df["Company Group"] = ""
    for idx, row in processed_df.iterrows():
        if pd.notna(row[customer_col]):
            customer_code = str(row[customer_col]).strip()
            if customer_code in st.session_state.customer_codes:
                exec_name = st.session_state.customer_codes[customer_code]
                if exec_col:
                    processed_df.at[idx, exec_col] = exec_name
                if exec_code_col and exec_name in st.session_state.executive_codes:
                    processed_df.at[idx, exec_code_col] = st.session_state.executive_codes[exec_name]
                branch = get_branches_for_executive(exec_name)
                processed_df.at[idx, "Branch"] = branch
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    processed_df.at[idx, "Region"] = region
            if st.session_state.product_col and pd.notna(row[st.session_state.product_col]):
                product_name = str(row[st.session_state.product_col]).strip()
                company_group = get_company_for_product(product_name)
                if company_group:
                    processed_df.at[idx, "Company Group"] = company_group
    return processed_df

def process_sales_file(sales_df, exec_code_col, product_col=None, exec_name_col=None, unit_col=None, quantity_col=None, value_col=None):
    processed_df = sales_df.copy()
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    if "Company Group" not in processed_df.columns:
        processed_df["Company Group"] = ""
    
    # Add new columns for converted values
    if unit_col and quantity_col and unit_col != "None" and quantity_col != "None":
        processed_df["Actual Quantity"] = ""
    if value_col and value_col != "None":
        processed_df["Value"] = ""
    
    for idx, row in processed_df.iterrows():
        if pd.notna(row[exec_code_col]):
            exec_code = str(row[exec_code_col]).strip()
            exec_name = None
            for name, code in st.session_state.executive_codes.items():
                if str(code).strip() == exec_code:
                    exec_name = name
                    break
            if exec_name and exec_name_col and exec_name_col in processed_df.columns:
                processed_df.at[idx, exec_name_col] = exec_name
                branch = get_branches_for_executive(exec_name)
                processed_df.at[idx, "Branch"] = branch
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    processed_df.at[idx, "Region"] = region
        
        if product_col and pd.notna(row[product_col]):
            product_name = str(row[product_col]).strip()
            company_group = get_company_for_product(product_name)
            if company_group:
                processed_df.at[idx, "Company Group"] = company_group
        
        # Process unit conversions
        if unit_col and quantity_col and unit_col != "None" and quantity_col != "None" and pd.notna(row[unit_col]) and pd.notna(row[quantity_col]):
            unit = str(row[unit_col]).strip().upper()
            try:
                quantity = float(row[quantity_col]) if isinstance(row[quantity_col], (int, float, str)) else 0
            except (ValueError, TypeError):
                quantity = 0
            
            if unit == "MT":
                actual_quantity = quantity
            elif unit in ["KGS", "NOS"]:
                actual_quantity = quantity / 1000
            else:
                actual_quantity = quantity  # Default case
            
            processed_df.at[idx, "Actual Quantity"] = actual_quantity
        
        # Process value conversion
        if value_col and value_col != "None" and pd.notna(row[value_col]):
            try:
                value = float(row[value_col]) if isinstance(row[value_col], (int, float, str)) else 0
            except (ValueError, TypeError):
                value = 0
            converted_value = value / 100000  # Convert to lakhs
            processed_df.at[idx, "Value"] = converted_value
    
    return processed_df

def process_os_file(os_df, exec_code_col):
    processed_df = os_df.copy()
    
    # Add Branch and Region columns
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    
    for idx, row in processed_df.iterrows():
        if pd.notna(row[exec_code_col]):
            exec_code = str(row[exec_code_col]).strip()
            exec_name = None
            
            # Find executive name from executive code
            for name, code in st.session_state.executive_codes.items():
                if str(code).strip() == exec_code:
                    exec_name = name
                    break
            
            if exec_name:
                # Get branch for this executive
                branch = get_branches_for_executive(exec_name)
                processed_df.at[idx, "Branch"] = branch
                
                # Get region for the branch (only if single branch)
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    processed_df.at[idx, "Region"] = region
    
    return processed_df

def main():
    st.set_page_config(
        page_title="Executive Mapping Admin Portal",
        page_icon="🔧",
        layout="wide"
    )
    init_session_state()
    st.title("Executive Mapping Administration Portal")
    st.write("This portal allows you to manage executive and branch mappings for budget and sales data.")
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Data Import", 
        "Executive Management", 
        "Branch & Region Mapping", 
        "Company-Product Mapping",
        "Backup & Restore",
        "Consolidated Data View",
        "Data Processing & Download"
    ])
    
    with tab1:
        st.header("Budget Data Import")
        st.write("Upload your budget file to extract customer code to executive mappings.")
        budget_file = st.file_uploader("Upload Budget File (Excel)", type=['xlsx', 'xls'], key="budget_file")
        if budget_file is not None:
            budget_file_copy = io.BytesIO(budget_file.getvalue())
            sheet_names = get_sheet_names(budget_file_copy)
            if sheet_names:
                default_sheet = "Consolidate" if "Consolidate" in sheet_names else sheet_names[0]
                selected_sheet = st.selectbox("Select Sheet:", sheet_names, index=sheet_names.index(default_sheet) if default_sheet in sheet_names else 0, key="budget_sheet")
                header_row = st.number_input("Select Header Row (0-based index):", min_value=0, value=1, key="budget_header")
                budget_df = get_sheet_preview(budget_file_copy, selected_sheet, header_row)
                if budget_df is not None:
                    st.write("Preview of Budget Data:")
                    st.dataframe(budget_df.head())
                    st.session_state.budget_df = budget_df
                    with st.container(border=True):
                        st.subheader("Select Columns for Mapping")
                        col1, col2 = st.columns(2)
                        with col1:
                            customer_col_options = list(budget_df.columns)
                            default_customer_col = "SL Code" if "SL Code" in customer_col_options else customer_col_options[0]
                            customer_col = st.selectbox("Select Customer Code Column:", customer_col_options, 
                                                      index=customer_col_options.index(default_customer_col) if default_customer_col in customer_col_options else 0, 
                                                      key="budget_customer_col")
                            default_exec_col = "Executive Name" if "Executive Name" in customer_col_options else customer_col_options[0]
                            budget_exec_col = st.selectbox("Select Executive Column:", customer_col_options, 
                                                         index=customer_col_options.index(default_exec_col) if default_exec_col in customer_col_options else 0, 
                                                         key="budget_exec_col")
                        with col2:
                            customer_name_col_options = [""] + list(budget_df.columns)
                            default_customer_name_col = "Party Name" if "Party Name" in customer_name_col_options else ""
                            customer_name_col = st.selectbox("Select Customer Name Column:", customer_name_col_options, 
                                                           index=customer_name_col_options.index(default_customer_name_col) if default_customer_name_col in customer_name_col_options else 0, 
                                                           key="budget_customer_name_col")
                            exec_code_col_options = [""] + list(budget_df.columns)
                            default_exec_code_col = "Executive Code" if "Executive Code" in exec_code_col_options else ""
                            exec_code_col = st.selectbox("Select Executive Code Column:", exec_code_col_options, 
                                                       index=exec_code_col_options.index(default_exec_code_col) if default_exec_code_col in exec_code_col_options else 0, 
                                                       key="budget_exec_code_col")
                        product_col_options = [""] + list(budget_df.columns)
                        default_product_col = "Product Group" if "Product Group" in product_col_options else ""
                        product_col = st.selectbox("Select Product Group Column:", product_col_options, 
                                                 index=product_col_options.index(default_product_col) if default_product_col in product_col_options else 0, 
                                                 key="budget_product_col")
                        if st.button("Extract Data from Budget File", key="extract_budget_button"):
                            if customer_col and budget_exec_col:
                                st.session_state.customer_col = customer_col
                                st.session_state.exec_col = budget_exec_col
                                st.session_state.product_col = product_col if product_col and product_col in budget_df.columns else None
                                st.session_state.customer_name_col = customer_name_col if customer_name_col else None
                                st.session_state.exec_code_col = exec_code_col if exec_code_col else None
                                customer_exec_map, customer_name_map = extract_customer_executive_mapping(budget_df, customer_col, budget_exec_col, customer_name_col)
                                st.session_state.customer_codes = customer_exec_map
                                if customer_name_map:
                                    st.session_state.customer_names.update(customer_name_map)
                                    log_update('executive_management', 'Extract Customer Names', 
                                              f"Extracted {len(customer_name_map)} customer names")
                                extracted_data = extract_data_from_budget(budget_df=budget_df)
                                import streamlit as st
import pandas as pd
import numpy as np
import io
import os
import json
import datetime
import pickle
import openpyxl
from openpyxl.utils import get_column_letter

DATA_DIR = "app_data"
METADATA_PATH = os.path.join(DATA_DIR, "metadata.pickle")
BRANCH_MAPPING_PATH = os.path.join(DATA_DIR, "branch_mappings.pickle")
REGION_MAPPING_PATH = os.path.join(DATA_DIR, "region_mappings.pickle")
COMPANY_MAPPING_PATH = os.path.join(DATA_DIR, "company_mappings.pickle")
BACKUP_PATH = os.path.join(DATA_DIR, "backup.json")

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def to_excel_buffer(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output

def init_session_state():
    if 'initialized' not in st.session_state:
        st.session_state.initialized = True
        st.session_state.budget_df = None
        st.session_state.processed_budget = None
        st.session_state.processed_sales = None
        st.session_state.processed_os = None  # Added OS processing state
        st.session_state.customer_col = None
        st.session_state.exec_col = None
        st.session_state.product_col = None
        st.session_state.customer_name_col = None
        st.session_state.exec_code_col = None
        st.session_state.executives = []
        st.session_state.executive_codes = {}
        st.session_state.product_groups = []
        st.session_state.customer_codes = {}
        st.session_state.customer_names = {}
        st.session_state.unmapped_customers = []
        st.session_state.branch_exec_mapping = {}
        st.session_state.region_branch_mapping = {}
        st.session_state.company_product_mapping = {}
        st.session_state.update_logs = {
            'executive_management': [],
            'branch_region_mapping': [],
            'company_product_mapping': []
        }
        load_all_mappings()

def save_metadata():
    ensure_data_dir()
    metadata = {
        "executives": st.session_state.executives,
        "executive_codes": st.session_state.executive_codes,
        "product_groups": st.session_state.product_groups,
        "customer_codes": st.session_state.customer_codes,
        "customer_names": st.session_state.customer_names,
        "unmapped_customers": st.session_state.unmapped_customers,
        "update_logs": st.session_state.update_logs
    }
    with open(METADATA_PATH, 'wb') as f:
        pickle.dump(metadata, f)

def load_metadata():
    if os.path.exists(METADATA_PATH):
        try:
            with open(METADATA_PATH, 'rb') as f:
                metadata = pickle.load(f)
                st.session_state.executives = metadata.get("executives", [])
                st.session_state.executive_codes = metadata.get("executive_codes", {})
                st.session_state.product_groups = metadata.get("product_groups", [])
                st.session_state.customer_codes = metadata.get("customer_codes", {})
                st.session_state.customer_names = metadata.get("customer_names", {})
                st.session_state.unmapped_customers = metadata.get("unmapped_customers", [])
                st.session_state.update_logs = metadata.get("update_logs", {
                    'executive_management': [],
                    'branch_region_mapping': [],
                    'company_product_mapping': []
                })
            return True
        except Exception as e:
            st.error(f"Error loading metadata: {e}")
    return False

def save_branch_mappings():
    ensure_data_dir()
    with open(BRANCH_MAPPING_PATH, 'wb') as f:
        pickle.dump(st.session_state.branch_exec_mapping, f)

def load_branch_mappings():
    if os.path.exists(BRANCH_MAPPING_PATH):
        try:
            with open(BRANCH_MAPPING_PATH, 'rb') as f:
                st.session_state.branch_exec_mapping = pickle.load(f)
            return True
        except Exception as e:
            st.error(f"Error loading branch mappings: {e}")
    return False

def save_region_mappings():
    ensure_data_dir()
    with open(REGION_MAPPING_PATH, 'wb') as f:
        pickle.dump(st.session_state.region_branch_mapping, f)

def load_region_mappings():
    if os.path.exists(REGION_MAPPING_PATH):
        try:
            with open(REGION_MAPPING_PATH, 'rb') as f:
                st.session_state.region_branch_mapping = pickle.load(f)
            return True
        except Exception as e:
            st.error(f"Error loading region mappings: {e}")
    return False

def save_company_mappings():
    ensure_data_dir()
    with open(COMPANY_MAPPING_PATH, 'wb') as f:
        pickle.dump(st.session_state.company_product_mapping, f)

def load_company_mappings():
    if os.path.exists(COMPANY_MAPPING_PATH):
        try:
            with open(COMPANY_MAPPING_PATH, 'rb') as f:
                st.session_state.company_product_mapping = pickle.load(f)
            return True
        except Exception as e:
            st.error(f"Error loading company mappings: {e}")
    return False

def save_all_mappings():
    save_metadata()
    save_branch_mappings()
    save_region_mappings()
    save_company_mappings()
    return True

def load_all_mappings():
    load_metadata()
    load_branch_mappings()
    load_region_mappings()
    load_company_mappings()
    return True

def reset_all_mappings():
    st.session_state.executives = []
    st.session_state.executive_codes = {}
    st.session_state.product_groups = []
    st.session_state.customer_codes = {}
    st.session_state.customer_names = {}
    st.session_state.unmapped_customers = []
    st.session_state.branch_exec_mapping = {}
    st.session_state.region_branch_mapping = {}
    st.session_state.company_product_mapping = {}
    st.session_state.update_logs = {
        'executive_management': [],
        'branch_region_mapping': [],
        'company_product_mapping': []
    }
    save_all_mappings()
    return True

def export_all_mappings():
    ensure_data_dir()
    all_data = {
        "metadata": {
            "executives": st.session_state.executives,
            "executive_codes": st.session_state.executive_codes,
            "product_groups": st.session_state.product_groups,
            "customer_codes": st.session_state.customer_codes,
            "customer_names": st.session_state.customer_names,
            "unmapped_customers": st.session_state.unmapped_customers
        },
        "branch_exec_mapping": st.session_state.branch_exec_mapping,
        "region_branch_mapping": st.session_state.region_branch_mapping,
        "company_product_mapping": st.session_state.company_product_mapping
    }
    json_data = json.dumps(all_data, indent=4)
    st.download_button(
        "Download Backup File",
        json_data,
        "executive_mapping_backup.json",
        "application/json",
        key="download_backup"
    )

def import_mappings_from_file(file):
    try:
        file_content = file.read()
        data = json.loads(file_content)
        metadata = data.get("metadata", {})
        st.session_state.executives = metadata.get("executives", [])
        st.session_state.executive_codes = metadata.get("executive_codes", {})
        st.session_state.product_groups = metadata.get("product_groups", [])
        st.session_state.customer_codes = metadata.get("customer_codes", {})
        st.session_state.customer_names = metadata.get("customer_names", {})
        st.session_state.unmapped_customers = metadata.get("unmapped_customers", [])
        st.session_state.branch_exec_mapping = data.get("branch_exec_mapping", {})
        st.session_state.region_branch_mapping = data.get("region_branch_mapping", {})
        st.session_state.company_product_mapping = data.get("company_product_mapping", {})
        save_all_mappings()
        st.success("Successfully restored all mappings from backup file")
        return True
    except Exception as e:
        st.error(f"Error importing mappings: {e}")
        return False

def log_update(log_type, action, details):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.update_logs[log_type].append({
        "timestamp": timestamp,
        "action": action,
        "details": details
    })
    save_metadata()

def reprocess_budget_data():
    if st.session_state.budget_df is not None and st.session_state.customer_col:
        processed_budget = process_budget_file(
            st.session_state.budget_df, 
            st.session_state.customer_col
        )
        st.session_state.processed_budget = processed_budget
        st.sidebar.success("Budget file reprocessed with current mappings!")
    else:
        st.sidebar.warning("No budget data available to reprocess")

def extract_customer_executive_mapping(df, customer_col, exec_col, name_col=None):
    customer_exec_map = {}
    customer_name_map = {}
    for _, row in df.iterrows():
        if pd.notna(row[customer_col]) and pd.notna(row[exec_col]):
            customer_code = str(row[customer_col]).strip()
            exec_name = str(row[exec_col]).strip()
            customer_exec_map[customer_code] = exec_name
            if name_col and pd.notna(row[name_col]):
                customer_name = str(row[name_col]).strip()
                customer_name_map[customer_code] = customer_name
    return customer_exec_map, customer_name_map

def extract_data_from_budget(budget_df):
    executives = []
    executive_codes = {}
    if 'exec_col' in st.session_state and st.session_state.exec_col:
        exec_col = st.session_state.exec_col
        if exec_col in budget_df.columns:
            exec_data = budget_df[budget_df[exec_col].notna()][exec_col].unique()
            executives = [str(exec_name).strip() for exec_name in exec_data]
    product_groups = []
    if 'product_col' in st.session_state and st.session_state.product_col:
        product_col = st.session_state.product_col
        if product_col and product_col in budget_df.columns:
            product_data = budget_df[budget_df[product_col].notna()][product_col].unique()
            product_groups = [str(product).strip() for product in product_data]
    results = {
        "executives": pd.DataFrame({"Executive Name": executives}),
        "executive_codes": executive_codes,
        "product_groups": pd.DataFrame({"Product Group": product_groups})
    }
    return results

def get_sheet_names(file):
    try:
        excel_file = pd.ExcelFile(file)
        return excel_file.sheet_names
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        return []

def get_sheet_preview(file, sheet_name, header_row=0):
    try:
        df = pd.read_excel(file, sheet_name=sheet_name, header=header_row)
        return df
    except Exception as e:
        st.error(f"Error loading sheet {sheet_name}: {e}")
        return None

def get_customer_codes_for_executive(exec_name):
    customer_codes = []
    for code, executive in st.session_state.customer_codes.items():
        if executive == exec_name:
            customer_codes.append(code)
    return customer_codes

def remove_executive(exec_name):
    if exec_name in st.session_state.executives:
        customer_codes = get_customer_codes_for_executive(exec_name)
        st.session_state.executives.remove(exec_name)
        if exec_name in st.session_state.executive_codes:
            del st.session_state.executive_codes[exec_name]
        for branch, execs in st.session_state.branch_exec_mapping.items():
            if exec_name in execs:
                st.session_state.branch_exec_mapping[branch].remove(exec_name)
        for code in customer_codes:
            if code in st.session_state.customer_codes:
                del st.session_state.customer_codes[code]
                if code not in st.session_state.unmapped_customers:
                    st.session_state.unmapped_customers.append(code)
        save_all_mappings()
        log_update('executive_management', 'Remove Executive', f"Removed executive {exec_name} and unmapped {len(customer_codes)} customers")
        return True, len(customer_codes)
    return False, 0

def remove_branch(branch_name):
    if branch_name in st.session_state.branch_exec_mapping:
        del st.session_state.branch_exec_mapping[branch_name]
        for region, branches in st.session_state.region_branch_mapping.items():
            if branch_name in branches:
                st.session_state.region_branch_mapping[region].remove(branch_name)
        save_all_mappings()
        log_update('branch_region_mapping', 'Remove Branch', f"Removed branch {branch_name}")
        return True
    return False

def remove_region(region_name):
    if region_name in st.session_state.region_branch_mapping:
        del st.session_state.region_branch_mapping[region_name]
        save_all_mappings()
        log_update('branch_region_mapping', 'Remove Region', f"Removed region {region_name}")
        return True
    return False

def remove_product_group(product_name):
    if product_name in st.session_state.product_groups:
        st.session_state.product_groups.remove(product_name)
        for company, products in st.session_state.company_product_mapping.items():
            if product_name in products:
                st.session_state.company_product_mapping[company].remove(product_name)
        save_all_mappings()
        log_update('company_product_mapping', 'Remove Product Group', f"Removed product group {product_name}")
        return True
    return False

def remove_company_group(company_name):
    if company_name in st.session_state.company_product_mapping:
        del st.session_state.company_product_mapping[company_name]
        save_all_mappings()
        log_update('company_product_mapping', 'Remove Company Group', f"Removed company group {company_name}")
        return True
    return False

def remove_customer_codes(exec_name, codes):
    count = 0
    for code in codes:
        if code in st.session_state.customer_codes and st.session_state.customer_codes[code] == exec_name:
            del st.session_state.customer_codes[code]
            if code not in st.session_state.unmapped_customers:
                st.session_state.unmapped_customers.append(code)
            count += 1
    if count > 0:
        save_metadata()
    return count

def assign_customer_codes(exec_name, codes):
    count = 0
    for code in codes:
        st.session_state.customer_codes[code] = exec_name
        if code in st.session_state.unmapped_customers:
            st.session_state.unmapped_customers.remove(code)
        count += 1
    if count > 0:
        save_metadata()
    return count

def get_customer_info_string(code):
    name = st.session_state.customer_names.get(code, "")
    if name:
        return f"{code} - {name}"
    else:
        return code

def get_branches_for_executive(exec_name):
    branches = []
    for branch, execs in st.session_state.branch_exec_mapping.items():
        if exec_name in execs:
            branches.append(branch)
    return ", ".join(sorted(branches)) if branches else ""

def get_region_for_branch(branch_name):
    for region, branches in st.session_state.region_branch_mapping.items():
        if branch_name in branches:
            return region
    return ""

def get_company_for_product(product_name):
    for company, products in st.session_state.company_product_mapping.items():
        if product_name in products:
            return company
    return ""

def extract_executive_customer_from_file(df, exec_col, cust_col, exec_code_col="None", cust_name_col="None", add_all_execs=True):
    relationships = {}
    exec_codes = {}
    cust_names = {}
    all_execs = set()
    for _, row in df.iterrows():
        if pd.notna(row[exec_col]) and pd.notna(row[cust_col]):
            exec_name = str(row[exec_col]).strip()
            cust_code = str(row[cust_col]).strip()
            relationships[cust_code] = exec_name
            all_execs.add(exec_name)
            if exec_code_col != "None" and pd.notna(row[exec_code_col]):
                exec_code = str(row[exec_code_col]).strip()
                exec_codes[exec_name] = exec_code
            if cust_name_col != "None" and pd.notna(row[cust_name_col]):
                cust_name = str(row[cust_name_col]).strip()
                cust_names[cust_code] = cust_name
        elif add_all_execs and pd.notna(row[exec_col]):
            exec_name = str(row[exec_col]).strip()
            all_execs.add(exec_name)
            if exec_code_col != "None" and pd.notna(row[exec_code_col]):
                exec_code = str(row[exec_code_col]).strip()
                exec_codes[exec_name] = exec_code
    return relationships, exec_codes, cust_names

def process_budget_file(budget_df, customer_col):
    processed_df = budget_df.copy()
    exec_col = st.session_state.exec_col if st.session_state.exec_col in processed_df.columns else None
    exec_code_col = st.session_state.exec_code_col if st.session_state.exec_code_col and st.session_state.exec_code_col in processed_df.columns else None
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    if st.session_state.product_col and "Company Group" not in processed_df.columns:
        processed_df["Company Group"] = ""
    for idx, row in processed_df.iterrows():
        if pd.notna(row[customer_col]):
            customer_code = str(row[customer_col]).strip()
            if customer_code in st.session_state.customer_codes:
                exec_name = st.session_state.customer_codes[customer_code]
                if exec_col:
                    processed_df.at[idx, exec_col] = exec_name
                if exec_code_col and exec_name in st.session_state.executive_codes:
                    processed_df.at[idx, exec_code_col] = st.session_state.executive_codes[exec_name]
                branch = get_branches_for_executive(exec_name)
                processed_df.at[idx, "Branch"] = branch
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    processed_df.at[idx, "Region"] = region
            if st.session_state.product_col and pd.notna(row[st.session_state.product_col]):
                product_name = str(row[st.session_state.product_col]).strip()
                company_group = get_company_for_product(product_name)
                if company_group:
                    processed_df.at[idx, "Company Group"] = company_group
    return processed_df

def process_sales_file(sales_df, exec_code_col, product_col=None, exec_name_col=None, unit_col=None, quantity_col=None, value_col=None):
    processed_df = sales_df.copy()
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    if "Company Group" not in processed_df.columns:
        processed_df["Company Group"] = ""
    
    # Add new columns for converted values
    if unit_col and quantity_col and unit_col != "None" and quantity_col != "None":
        processed_df["Actual Quantity"] = ""
    if value_col and value_col != "None":
        processed_df["Value"] = ""
    
    for idx, row in processed_df.iterrows():
        if pd.notna(row[exec_code_col]):
            exec_code = str(row[exec_code_col]).strip()
            exec_name = None
            for name, code in st.session_state.executive_codes.items():
                if str(code).strip() == exec_code:
                    exec_name = name
                    break
            if exec_name and exec_name_col and exec_name_col in processed_df.columns:
                processed_df.at[idx, exec_name_col] = exec_name
                branch = get_branches_for_executive(exec_name)
                processed_df.at[idx, "Branch"] = branch
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    processed_df.at[idx, "Region"] = region
        
        if product_col and pd.notna(row[product_col]):
            product_name = str(row[product_col]).strip()
            company_group = get_company_for_product(product_name)
            if company_group:
                processed_df.at[idx, "Company Group"] = company_group
        
        # Process unit conversions
        if unit_col and quantity_col and unit_col != "None" and quantity_col != "None" and pd.notna(row[unit_col]) and pd.notna(row[quantity_col]):
            unit = str(row[unit_col]).strip().upper()
            try:
                quantity = float(row[quantity_col]) if isinstance(row[quantity_col], (int, float, str)) else 0
            except (ValueError, TypeError):
                quantity = 0
            
            if unit == "MT":
                actual_quantity = quantity
            elif unit in ["KGS", "NOS"]:
                actual_quantity = quantity / 1000
            else:
                actual_quantity = quantity  # Default case
            
            processed_df.at[idx, "Actual Quantity"] = actual_quantity
        
        # Process value conversion
        if value_col and value_col != "None" and pd.notna(row[value_col]):
            try:
                value = float(row[value_col]) if isinstance(row[value_col], (int, float, str)) else 0
            except (ValueError, TypeError):
                value = 0
            converted_value = value / 100000  # Convert to lakhs
            processed_df.at[idx, "Value"] = converted_value
    
    return processed_df

def process_os_file(os_df, exec_code_col):
    processed_df = os_df.copy()
    
    # Add Branch and Region columns
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    
    for idx, row in processed_df.iterrows():
        if pd.notna(row[exec_code_col]):
            exec_code = str(row[exec_code_col]).strip()
            exec_name = None
            
            # Find executive name from executive code
            for name, code in st.session_state.executive_codes.items():
                if str(code).strip() == exec_code:
                    exec_name = name
                    break
            
            if exec_name:
                # Get branch for this executive
                branch = get_branches_for_executive(exec_name)
                processed_df.at[idx, "Branch"] = branch
                
                # Get region for the branch (only if single branch)
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    processed_df.at[idx, "Region"] = region
    
    return processed_df

def main():
    st.set_page_config(
        page_title="Executive Mapping Admin Portal",
        page_icon="🔧",
        layout="wide"
    )
    init_session_state()
    st.title("Executive Mapping Administration Portal")
    st.write("This portal allows you to manage executive and branch mappings for budget and sales data.")
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Data Import", 
        "Executive Management", 
        "Branch & Region Mapping", 
        "Company-Product Mapping",
        "Backup & Restore",
        "Consolidated Data View",
        "Data Processing & Download"
    ])
    
    with tab1:
        st.header("Budget Data Import")
        st.write("Upload your budget file to extract customer code to executive mappings.")
        budget_file = st.file_uploader("Upload Budget File (Excel)", type=['xlsx', 'xls'], key="budget_file")
        if budget_file is not None:
            budget_file_copy = io.BytesIO(budget_file.getvalue())
            sheet_names = get_sheet_names(budget_file_copy)
            if sheet_names:
                default_sheet = "Consolidate" if "Consolidate" in sheet_names else sheet_names[0]
                selected_sheet = st.selectbox("Select Sheet:", sheet_names, index=sheet_names.index(default_sheet) if default_sheet in sheet_names else 0, key="budget_sheet")
                header_row = st.number_input("Select Header Row (0-based index):", min_value=0, value=1, key="budget_header")
                budget_df = get_sheet_preview(budget_file_copy, selected_sheet, header_row)
                if budget_df is not None:
                    st.write("Preview of Budget Data:")
                    st.dataframe(budget_df.head())
                    st.session_state.budget_df = budget_df
                    with st.container(border=True):
                        st.subheader("Select Columns for Mapping")
                        col1, col2 = st.columns(2)
                        with col1:
                            customer_col_options = list(budget_df.columns)
                            default_customer_col = "SL Code" if "SL Code" in customer_col_options else customer_col_options[0]
                            customer_col = st.selectbox("Select Customer Code Column:", customer_col_options, 
                                                      index=customer_col_options.index(default_customer_col) if default_customer_col in customer_col_options else 0, 
                                                      key="budget_customer_col")
                            default_exec_col = "Executive Name" if "Executive Name" in customer_col_options else customer_col_options[0]
                            budget_exec_col = st.selectbox("Select Executive Column:", customer_col_options, 
                                                         index=customer_col_options.index(default_exec_col) if default_exec_col in customer_col_options else 0, 
                                                         key="budget_exec_col")
                        with col2:
                            customer_name_col_options = [""] + list(budget_df.columns)
                            default_customer_name_col = "Party Name" if "Party Name" in customer_name_col_options else ""
                            customer_name_col = st.selectbox("Select Customer Name Column:", customer_name_col_options, 
                                                           index=customer_name_col_options.index(default_customer_name_col) if default_customer_name_col in customer_name_col_options else 0, 
                                                           key="budget_customer_name_col")
                            exec_code_col_options = [""] + list(budget_df.columns)
                            default_exec_code_col = "Executive Code" if "Executive Code" in exec_code_col_options else ""
                            exec_code_col = st.selectbox("Select Executive Code Column:", exec_code_col_options, 
                                                       index=exec_code_col_options.index(default_exec_code_col) if default_exec_code_col in exec_code_col_options else 0, 
                                                       key="budget_exec_code_col")
                        product_col_options = [""] + list(budget_df.columns)
                        default_product_col = "Product Group" if "Product Group" in product_col_options else ""
                        product_col = st.selectbox("Select Product Group Column:", product_col_options, 
                                                 index=product_col_options.index(default_product_col) if default_product_col in product_col_options else 0, 
                                                 key="budget_product_col")
                        if st.button("Extract Data from Budget File", key="extract_budget_button"):
                            if customer_col and budget_exec_col:
                                st.session_state.customer_col = customer_col
                                st.session_state.exec_col = budget_exec_col
                                st.session_state.product_col = product_col if product_col and product_col in budget_df.columns else None
                                st.session_state.customer_name_col = customer_name_col if customer_name_col else None
                                st.session_state.exec_code_col = exec_code_col if exec_code_col else None
                                customer_exec_map, customer_name_map = extract_customer_executive_mapping(budget_df, customer_col, budget_exec_col, customer_name_col)
                                st.session_state.customer_codes = customer_exec_map
                                if customer_name_map:
                                    st.session_state.customer_names.update(customer_name_map)
                                    log_update('executive_management', 'Extract Customer Names', 
                                              f"Extracted {len(customer_name_map)} customer names")
                                extracted_data = extract_data_from_budget(budget_df=budget_df)
                                st.session_state.executives = extracted_data["executives"]["Executive Name"].tolist()
                                st.session_state.executive_codes = extracted_data["executive_codes"]
                                st.session_state.product_groups = extracted_data["product_groups"]["Product Group"].tolist()
                                if exec_code_col and exec_code_col != "":
                                    for _, row in budget_df.iterrows():
                                        if pd.notna(row[budget_exec_col]) and pd.notna(row[exec_code_col]):
                                            st.session_state.executive_codes[row[budget_exec_col]] = row[exec_code_col]
                                save_metadata()
                                log_update('executive_management', 'Extract Budget Data',
                                          f"Extracted {len(customer_exec_map)} customer-executive mappings and {len(extracted_data['executives'])} executives")
                                log_update('company_product_mapping', 'Extract Product Groups from Budget',
                                         f"Extracted {len(extracted_data['product_groups'])} product groups")
                                st.success(f"Extracted {len(customer_exec_map)} customer-executive mappings and {len(extracted_data['executives'])} executives from budget file")
                            else:
                                st.warning("Please select both Customer Code and Executive columns.")
        st.markdown("---")
        st.subheader("Current Data Summary")
        summary_col1, summary_col2, summary_col3 = st.columns(3)
        with summary_col1:
            with st.container(border=True):
                st.subheader("Current Executives")
                if st.session_state.executives:
                    exec_data = []
                    for exec_name in sorted(st.session_state.executives):
                        exec_code = st.session_state.executive_codes.get(exec_name, "No code")
                        exec_data.append({"Executive Name": exec_name, "Executive Code": exec_code})
                    st.dataframe(pd.DataFrame(exec_data), hide_index=True)
                else:
                    st.info("No executives extracted yet")
        with summary_col2:
            with st.container(border=True):
                st.subheader("Current Product Groups")
                if st.session_state.product_groups:
                    product_df = pd.DataFrame(sorted(st.session_state.product_groups), columns=["Product Group"])
                    st.dataframe(product_df, hide_index=True)
                else:
                    st.info("No product groups extracted yet")
        with summary_col3:
            with st.container(border=True):
                st.subheader("Customer-Executive Mappings")
                if st.session_state.customer_codes:
                    st.write(f"Total: {len(st.session_state.customer_codes)}")
                    mapping_sample = list(st.session_state.customer_codes.items())[:10]
                    if mapping_sample:
                        mapping_data = []
                        for code, exec_name in mapping_sample:
                            name = st.session_state.customer_names.get(code, "")
                            mapping_data.append({
                                "Customer Code": code,
                                "Customer Name": name,
                                "Executive": exec_name
                            })
                        mapping_df = pd.DataFrame(mapping_data)
                        st.dataframe(mapping_df)
                        if len(st.session_state.customer_codes) > 10:
                            st.caption(f"Showing 10 of {len(st.session_state.customer_codes)} mappings")
                else:
                    st.info("No customer-executive mappings extracted yet")

    with tab2:
        st.header("Executive Management")
        exec_tab1, exec_tab2 = st.tabs(["Executive Creation", "Customer Code Management"])
        with exec_tab1:
            with st.container(border=True):
                st.subheader("Add New Executive")
                col1, col2 = st.columns(2)
                with col1:
                    new_exec_name = st.text_input("Enter New Executive Name:")
                    new_exec_code = st.text_input("Enter New Executive Code:")
                with col2:
                    st.write("")
                    st.write("")
                    if st.button("Add New Executive", key="add_exec_button"):
                        if new_exec_name:
                            if new_exec_name in st.session_state.executives:
                                st.warning(f"Executive {new_exec_name} already exists")
                            else:
                                st.session_state.executives.append(new_exec_name)
                                if new_exec_code:
                                    st.session_state.executive_codes[new_exec_name] = new_exec_code
                                save_metadata()
                                log_update('executive_management', 'Add Executive', f"Added executive: {new_exec_name}")
                                st.success(f"Added new executive: {new_exec_name}")
            
            with st.container(border=True):
                st.subheader("Current Executives")
                if st.session_state.executives:
                    exec_code_data = []
                    for exec_name in sorted(st.session_state.executives):
                        exec_code = st.session_state.executive_codes.get(exec_name, "No code")
                        customer_count = len(get_customer_codes_for_executive(exec_name))
                        exec_code_data.append({
                            "Executive Name": exec_name,
                            "Executive Code": exec_code,
                            "Assigned Customers": customer_count
                        })
                    exec_code_df = pd.DataFrame(exec_code_data)
                    st.dataframe(exec_code_df, hide_index=True)
                    
                    st.subheader("Remove Executive")
                    exec_to_remove = st.selectbox("Select Executive to Remove:", 
                                                [""] + sorted(st.session_state.executives), 
                                                key="exec_to_remove")
                    if exec_to_remove and st.button("Remove Selected Executive", key="remove_exec_button"):
                        success, count = remove_executive(exec_to_remove)
                        if success:
                            st.success(f"Removed executive '{exec_to_remove}' and unmapped {count} customers")
                            st.rerun()
                        else:
                            st.error(f"Failed to remove executive '{exec_to_remove}'")
                else:
                    st.info("No executives added yet")
            
            with st.container(border=True):
                st.subheader("Executive Creation Update History")
                if st.session_state.update_logs['executive_management']:
                    update_df = pd.DataFrame(st.session_state.update_logs['executive_management'])
                    update_df = update_df[['timestamp', 'action', 'details']].sort_values(by='timestamp', ascending=False)
                    st.dataframe(update_df, hide_index=True, use_container_width=True)
                    if st.button("Clear Executive Update Logs", key="clear_exec_logs"):
                        st.session_state.update_logs['executive_management'] = []
                        save_metadata()
                        st.success("Executive update logs cleared")
                        st.rerun()
                else:
                    st.info("No updates recorded yet")

        with exec_tab2:
            with st.container(border=True):
                st.subheader("Bulk Customer Code Assignment")
                st.info("""
                Upload an Excel file containing Executive-Customer relationships for bulk assignment.
                The file should have columns for Executive Name, Executive Code, Customer Code, and Customer Name.
                This is useful for large-scale reassignments. 
                New executives and codes from the file will be automatically added to your mappings.
                """)
                reassignment_file = st.file_uploader(
                    "Upload Executive-Customer Reassignment File (Excel)", 
                    type=['xlsx', 'xls'], 
                    key="reassignment_file"
                )
                if reassignment_file is not None:
                    reassign_file_copy = io.BytesIO(reassignment_file.getvalue())
                    sheet_names = get_sheet_names(reassign_file_copy)
                    if sheet_names:
                        selected_sheet = st.selectbox(
                            "Select Sheet:", 
                            sheet_names, 
                            key="reassign_sheet"
                        )
                        header_row = st.number_input(
                            "Select Header Row (0-based index):", 
                            min_value=0, 
                            value=0, 
                            key="reassign_header"
                        )
                        reassign_df = get_sheet_preview(reassign_file_copy, selected_sheet, header_row)
                        if reassign_df is not None:
                            st.write("Preview of Reassignment Data:")
                            st.dataframe(reassign_df.head())
                            col1, col2 = st.columns(2)
                            with col1:
                                exec_name_col = st.selectbox(
                                    "Executive Name Column:", 
                                    list(reassign_df.columns),
                                    key="exec_name_col"
                                )
                                exec_code_col = st.selectbox(
                                    "Executive Code Column:", 
                                    ["None"] + list(reassign_df.columns),
                                    key="exec_code_col"
                                )
                            with col2:
                                cust_code_col = st.selectbox(
                                    "Customer Code Column:", 
                                    list(reassign_df.columns),
                                    key="cust_code_col"
                                )
                                cust_name_col = st.selectbox(
                                    "Customer Name Column:", 
                                    ["None"] + list(reassign_df.columns),
                                    key="cust_name_col"
                                )
                            add_new_execs = st.checkbox(
                                "Add all new executives from file (even if they have no customer assignments)",
                                value=True,
                                key="add_all_new_execs"
                            )
                            if st.button("Process Reassignments", key="process_reassign_button"):
                                if exec_name_col and cust_code_col:
                                    relationships, exec_codes, cust_names = extract_executive_customer_from_file(
                                        reassign_df, 
                                        exec_name_col, 
                                        cust_code_col, 
                                        exec_code_col, 
                                        cust_name_col, 
                                        add_new_execs
                                    )
                                    
                                    new_execs_added = 0
                                    new_exec_codes = 0
                                    for exec_name, exec_code in exec_codes.items():
                                        if exec_name not in st.session_state.executives:
                                            st.session_state.executives.append(exec_name)
                                            new_execs_added += 1
                                        if exec_code:
                                            st.session_state.executive_codes[exec_name] = exec_code
                                            new_exec_codes += 1
                                    
                                    new_assignments = 0
                                    reassignments = 0
                                    for cust_code, exec_name in relationships.items():
                                        if cust_code in st.session_state.customer_codes:
                                            if st.session_state.customer_codes[cust_code] != exec_name:
                                                reassignments += 1
                                        else:
                                            new_assignments += 1
                                        st.session_state.customer_codes[cust_code] = exec_name
                                        if cust_code in st.session_state.unmapped_customers:
                                            st.session_state.unmapped_customers.remove(cust_code)
                                    
                                    new_customer_names = 0
                                    for cust_code, cust_name in cust_names.items():
                                        if cust_code not in st.session_state.customer_names:
                                            new_customer_names += 1
                                        st.session_state.customer_names[cust_code] = cust_name
                                    
                                    save_metadata()
                                    save_all_mappings()
                                    log_update('executive_management', 'Bulk Reassignment', 
                                              f"Added {new_execs_added} new executives, {new_exec_codes} executive codes, " +
                                              f"{new_assignments} new assignments, {reassignments} reassignments, " +
                                              f"{new_customer_names} customer names")
                                    
                                    st.success(f"Successfully processed reassignment file: " +
                                              f"Added {new_execs_added} new executives, {new_exec_codes} executive codes, " +
                                              f"{new_assignments} new assignments, {reassignments} reassignments, " +
                                              f"{new_customer_names} customer names")
                                    
                                    st.info("Use the 'Re-process Budget Data' button in the sidebar to apply these changes to your reports.")
                                    
                                    with st.expander("Reassignment Details", expanded=True):
                                        st.subheader("Customer Counts by Executive")
                                        exec_counts = {}
                                        for customer, exec_name in st.session_state.customer_codes.items():
                                            exec_counts[exec_name] = exec_counts.get(exec_name, 0) + 1
                                        exec_count_data = [{"Executive": exec_name, "Executive Code": st.session_state.executive_codes.get(exec_name, ""), "Customer Count": count} 
                                                          for exec_name, count in exec_counts.items()]
                                        st.dataframe(pd.DataFrame(exec_count_data).sort_values(by="Customer Count", ascending=False))
                                else:
                                    st.error("Please select both Executive Name and Customer Code columns.")
                        else:
                            st.error("Failed to read the file. Please check the format and try again.")
            
            st.markdown("---")
            st.subheader("Manual Customer Code Management")
            if st.session_state.executives:
                with st.container(border=True):
                    st.subheader("Select Executive")
                    selected_exec = st.selectbox(
                        "Choose an executive to manage their customer codes:",
                        sorted(st.session_state.executives),
                        key="selected_exec_filter"
                    )
                    exec_code = st.session_state.executive_codes.get(selected_exec, "No code assigned")
                    st.write(f"Executive Code: **{exec_code}**")
                
                customer_codes = get_customer_codes_for_executive(selected_exec)
                with st.container(border=True):
                    st.subheader(f"Customer Codes for {selected_exec}")
                    if customer_codes:
                        st.write(f"**{len(customer_codes)} Customers assigned**")
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            show_all = st.checkbox("Show all customer codes", value=len(customer_codes) <= 20)
                            display_codes = sorted(customer_codes)
                            if not show_all and len(display_codes) > 20:
                                display_codes = display_codes[:20]
                                st.caption(f"Showing 20 of {len(customer_codes)} customer codes")
                            code_data = []
                            for code in display_codes:
                                name = st.session_state.customer_names.get(code, "")
                                code_data.append({
                                    "Customer Code": code,
                                    "Customer Name": name
                                })
                            code_df = pd.DataFrame(code_data)
                            st.dataframe(code_df, hide_index=True)
                        with col2:
                            display_options = {}
                            for code in sorted(customer_codes):
                                display_text = get_customer_info_string(code)
                                display_options[display_text] = code
                            codes_to_remove = st.multiselect(
                                "Select Customers to Remove:",
                                options=list(display_options.keys()),
                                key="remove_customers"
                            )
                            if st.button("Remove Selected", key="remove_customers_button"):
                                if codes_to_remove:
                                    actual_codes = [display_options[text] for text in codes_to_remove]
                                    count = remove_customer_codes(selected_exec, actual_codes)
                                    log_update('executive_management', 'Remove Customers', f"Removed {count} customers from {selected_exec}")
                                    st.success(f"Removed {count} customers from {selected_exec}. These customers are now unmapped.")
                                    st.rerun()
                    else:
                        st.info(f"No customers currently assigned to {selected_exec}")
                
                if st.session_state.unmapped_customers:
                    with st.container(border=True):
                        st.subheader(f"Assign Unmapped Customers to {selected_exec}")
                        st.write(f"There are {len(st.session_state.unmapped_customers)} unmapped customers available.")
                        display_options = {}
                        for code in sorted(st.session_state.unmapped_customers):
                            display_text = get_customer_info_string(code)
                            display_options[display_text] = code
                        customers_to_assign = st.multiselect(
                            "Select Customers to Assign:",
                            options=list(display_options.keys()),
                            key="assign_customers"
                        )
                        if st.button("Assign Selected", key="assign_customers_button"):
                            if customers_to_assign:
                                actual_codes = [display_options[text] for text in customers_to_assign]
                                count = assign_customer_codes(selected_exec, actual_codes)
                                log_update('executive_management', 'Assign Customers', f"Assigned {count} customers to {selected_exec}")
                                st.success(f"Assigned {count} customers to {selected_exec}")
                                st.rerun()
                
                with st.container(border=True):
                    st.subheader(f"Add New Customer Codes to {selected_exec}")
                    new_customer_codes = st.text_area("Enter Customer Codes (one per line):", key="new_customer_codes")
                    if st.button("Add Customer Codes", key="add_customer_codes_button"):
                        if new_customer_codes:
                            codes_list = [code.strip() for code in new_customer_codes.split('\n') if code.strip()]
                            if codes_list:
                                count = assign_customer_codes(selected_exec, codes_list)
                                log_update('executive_management', 'Add Customer Codes', f"Added {count} customer codes to {selected_exec}")
                                st.success(f"Added {count} customers to {selected_exec}")
                                st.rerun()
            else:
                st.warning("No executives available. Please add executives first.")
            
            with st.container(border=True):
                st.subheader("Unmapped Customers")
                if st.session_state.unmapped_customers:
                    st.write(f"Total unmapped customers: {len(st.session_state.unmapped_customers)}")
                    show_all_unmapped = st.checkbox("Show all unmapped customers", value=len(st.session_state.unmapped_customers) <= 50)
                    display_list = sorted(st.session_state.unmapped_customers)
                    if not show_all_unmapped and len(display_list) > 50:
                        display_list = display_list[:50]
                        st.caption(f"Showing 50 of {len(st.session_state.unmapped_customers)} unmapped customers")
                    unmapped_data = []
                    for code in display_list:
                        name = st.session_state.customer_names.get(code, "")
                        unmapped_data.append({
                            "Customer Code": code,
                            "Customer Name": name
                        })
                    unmapped_df = pd.DataFrame(unmapped_data)
                    st.dataframe(unmapped_df, hide_index=True)
                    if st.button("Clear Unmapped Customers", key="clear_unmapped_button"):
                        st.session_state.unmapped_customers = []
                        save_metadata()
                        log_update('executive_management', 'Clear Unmapped Customers', "Cleared all unmapped customers")
                        st.success("Unmapped customers list cleared")
                        st.rerun()
                else:
                    st.info("No unmapped customers")

    # Continue with remaining tabs (tab3-tab6)...
    with tab3:
        st.header("Branch & Region Mapping")
        branch_tab1, branch_tab2 = st.tabs(["Branch Management", "Region Management"])
        with branch_tab1:
            branch_col1, branch_col2 = st.columns(2)
            with branch_col1:
                with st.container(border=True):
                    st.subheader("Create New Branch")
                    new_branch = st.text_input("Enter Branch Name:")
                    if st.button("Create Branch", key="create_branch_button") and new_branch:
                        if new_branch not in st.session_state.branch_exec_mapping:
                            st.session_state.branch_exec_mapping[new_branch] = []
                            save_branch_mappings()
                            log_update('branch_region_mapping', 'Create Branch', f"Created branch: {new_branch}")
                            st.success(f"Created branch: {new_branch}")
                        else:
                            st.warning(f"Branch {new_branch} already exists")
            
            with branch_col2:
                with st.container(border=True):
                    st.subheader("Current Branches")
                    branches = list(st.session_state.branch_exec_mapping.keys())
                    if branches:
                        branch_df = pd.DataFrame(sorted(branches), columns=["Branch Name"])
                        st.dataframe(branch_df, hide_index=True)
                        st.subheader("Remove Branch")
                        branch_to_remove = st.selectbox("Select Branch to Remove:", 
                                                    [""] + sorted(branches), 
                                                    key="branch_to_remove")
                        if branch_to_remove and st.button("Remove Selected Branch", key="remove_branch_button"):
                            success = remove_branch(branch_to_remove)
                            if success:
                                st.success(f"Removed branch '{branch_to_remove}'")
                                st.rerun()
                            else:
                                st.error(f"Failed to remove branch '{branch_to_remove}'")
                    else:
                        st.info("No branches created yet")
                        
            with st.container(border=True):
                st.subheader("Map Executives to Branches")
                st.info("These mappings represent which executives are responsible for each branch.")
                branches = list(st.session_state.branch_exec_mapping.keys())
                if branches:
                    selected_branch = st.selectbox("Select Branch:", branches, key="select_branch_mapping")
                    if st.session_state.executives:
                        current_execs = st.session_state.branch_exec_mapping.get(selected_branch, [])
                        valid_current_execs = [exec_name for exec_name in current_execs if exec_name in st.session_state.executives]
                        selected_execs = st.multiselect(
                            "Select Executives for this Branch:",
                            sorted(st.session_state.executives),
                            default=valid_current_execs,
                            key="branch_exec_multiselect"
                        )
                        if st.button("Update Branch-Executive Mapping", key="update_branch_exec_button"):
                            st.session_state.branch_exec_mapping[selected_branch] = selected_execs
                            save_branch_mappings()
                            log_update('branch_region_mapping', 'Update Branch-Executive Mapping', f"Updated executives for branch: {selected_branch}")
                            st.success(f"Updated executives for branch: {selected_branch}")
                    else:
                        st.info("No executives available. Please extract or add executives first.")
                else:
                    st.info("Please create branches first")
            
            with st.container(border=True):
                st.subheader("Current Branch-Executive Mappings")
                if st.session_state.branch_exec_mapping:
                    branch_mapping_data = []
                    for branch, execs in st.session_state.branch_exec_mapping.items():
                        branch_mapping_data.append({
                            "Branch": branch,
                            "Executives": ", ".join(sorted(execs)) if execs else "None",
                            "Count": len(execs)
                        })
                    branch_mapping_df = pd.DataFrame(branch_mapping_data)
                    st.dataframe(branch_mapping_df, hide_index=True)
                    if st.button("Clear All Branch Mappings", key="clear_branch_mappings"):
                        st.session_state.branch_exec_mapping = {}
                        save_branch_mappings()
                        log_update('branch_region_mapping', 'Clear Branch Mappings', "Cleared all branch mappings")
                        st.success("All branch mappings cleared")
                        st.rerun()
                else:
                    st.info("No branch mappings created yet")

        with branch_tab2:
            region_col1, region_col2 = st.columns(2)
            with region_col1:
                with st.container(border=True):
                    st.subheader("Create New Region")
                    new_region = st.text_input("Enter Region Name:")
                    if st.button("Create Region", key="create_region_button") and new_region:
                        if new_region not in st.session_state.region_branch_mapping:
                            st.session_state.region_branch_mapping[new_region] = []
                            save_region_mappings()
                            log_update('branch_region_mapping', 'Create Region', f"Created region: {new_region}")
                            st.success(f"Created region: {new_region}")
                        else:
                            st.warning(f"Region {new_region} already exists")
                            
            with region_col2:
                with st.container(border=True):
                    st.subheader("Current Regions")
                    regions = list(st.session_state.region_branch_mapping.keys())
                    if regions:
                        region_df = pd.DataFrame(sorted(regions), columns=["Region Name"])
                        st.dataframe(region_df, hide_index=True)
                        st.subheader("Remove Region")
                        region_to_remove = st.selectbox("Select Region to Remove:", 
                                                    [""] + sorted(regions), 
                                                    key="region_to_remove")
                        if region_to_remove and st.button("Remove Selected Region", key="remove_region_button"):
                            success = remove_region(region_to_remove)
                            if success:
                                st.success(f"Removed region '{region_to_remove}'")
                                st.rerun()
                            else:
                                st.error(f"Failed to remove region '{region_to_remove}'")
                    else:
                        st.info("No regions created yet")
                        
            with st.container(border=True):
                st.subheader("Map Branches to Regions")
                st.info("These mappings represent which branches belong to each region.")
                regions = list(st.session_state.region_branch_mapping.keys())
                if regions:
                    selected_region = st.selectbox("Select Region:", regions, key="select_region_mapping")
                    branches = list(st.session_state.branch_exec_mapping.keys())
                    if branches:
                        current_branches = st.session_state.region_branch_mapping.get(selected_region, [])
                        valid_current_branches = [branch for branch in current_branches if branch in branches]
                        selected_branches = st.multiselect(
                            "Select Branches for this Region:",
                            sorted(branches),
                            default=valid_current_branches,
                            key="region_branch_multiselect"
                        )
                        if st.button("Update Region-Branch Mapping", key="update_region_branch_button"):
                            st.session_state.region_branch_mapping[selected_region] = selected_branches
                            save_region_mappings()
                            log_update('branch_region_mapping', 'Update Region-Branch Mapping', f"Updated branches for region: {selected_region}")
                            st.success(f"Updated branches for region: {selected_region}")
                    else:
                        st.info("No branches available. Please create branches first.")
                else:
                    st.info("Please create regions first")
                    
            with st.container(border=True):
                st.subheader("Current Region-Branch Mappings")
                if st.session_state.region_branch_mapping:
                    region_mapping_data = []
                    for region, branches in st.session_state.region_branch_mapping.items():
                        region_mapping_data.append({
                            "Region": region,
                            "Branches": ", ".join(sorted(branches)) if branches else "None",
                            "Count": len(branches)
                        })
                    region_mapping_df = pd.DataFrame(region_mapping_data)
                    st.dataframe(region_mapping_df, hide_index=True)
                    if st.button("Clear All Region Mappings", key="clear_region_mappings"):
                        st.session_state.region_branch_mapping = {}
                        save_region_mappings()
                        log_update('branch_region_mapping', 'Clear Region Mappings', "Cleared all region mappings")
                        st.success("All region mappings cleared")
                        st.rerun()
                else:
                    st.info("No region mappings created yet")

    with tab4:
        st.header("Company Group-Product Group Mapping")
        product_col1, product_col2 = st.columns(2)
        with product_col1:
            with st.container(border=True):
                st.subheader("Create New Product Group")
                new_product = st.text_input("Enter Product Group Name:")
                if st.button("Create Product Group", key="create_product_button") and new_product:
                    if new_product not in st.session_state.product_groups:
                        st.session_state.product_groups.append(new_product)
                        save_metadata()
                        log_update('company_product_mapping', 'Create Product Group', f"Created product group: {new_product}")
                        st.success(f"Created product group: {new_product}")
                    else:
                        st.warning(f"Product group {new_product} already exists")
                        
        with product_col2:
            with st.container(border=True):
                st.subheader("Current Product Groups")
                if st.session_state.product_groups:
                    product_df = pd.DataFrame(sorted(st.session_state.product_groups), columns=["Product Group"])
                    st.dataframe(product_df, hide_index=True)
                    st.subheader("Remove Product Group")
                    product_to_remove = st.selectbox("Select Product Group to Remove:", 
                                                [""] + sorted(st.session_state.product_groups), 
                                                key="product_to_remove")
                    if product_to_remove and st.button("Remove Selected Product Group", key="remove_product_button"):
                        success = remove_product_group(product_to_remove)
                        if success:
                            st.success(f"Removed product group '{product_to_remove}'")
                            st.rerun()
                        else:
                            st.error(f"Failed to remove product group '{product_to_remove}'")
                else:
                    st.info("No product groups created yet")
        
        company_col1, company_col2 = st.columns(2)
        with company_col1:
            with st.container(border=True):
                st.subheader("Create New Company Group")
                new_company = st.text_input("Enter Company Group Name:")
                if st.button("Create Company Group", key="create_company_button") and new_company:
                    if new_company not in st.session_state.company_product_mapping:
                        st.session_state.company_product_mapping[new_company] = []
                        save_company_mappings()
                        log_update('company_product_mapping', 'Create Company Group', f"Created company group: {new_company}")
                        st.success(f"Created company group: {new_company}")
                    else:
                        st.warning(f"Company group {new_company} already exists")
                        
        with company_col2:
            with st.container(border=True):
                st.subheader("Current Company Groups")
                company_groups = list(st.session_state.company_product_mapping.keys())
                if company_groups:
                    company_df = pd.DataFrame(sorted(company_groups), columns=["Company Group"])
                    st.dataframe(company_df, hide_index=True)
                    st.subheader("Remove Company Group")
                    company_to_remove = st.selectbox("Select Company Group to Remove:", 
                                                [""] + sorted(company_groups), 
                                                key="company_to_remove")
                    if company_to_remove and st.button("Remove Selected Company Group", key="remove_company_button"):
                        success = remove_company_group(company_to_remove)
                        if success:
                            st.success(f"Removed company group '{company_to_remove}'")
                            st.rerun()
                        else:
                            st.error(f"Failed to remove company group '{company_to_remove}'")
                else:
                    st.info("No company groups created yet")
        
        with st.container(border=True):
            st.subheader("Map Product Groups to Company Groups")
            st.info("These mappings will be used to add company group information to the budget data.")
            company_groups = list(st.session_state.company_product_mapping.keys())
            if company_groups:
                selected_company = st.selectbox("Select Company Group:", company_groups, key="select_company_mapping")
                if st.session_state.product_groups:
                    current_products = st.session_state.company_product_mapping.get(selected_company, [])
                    valid_current_products = [product for product in current_products if product in st.session_state.product_groups]
                    selected_products = st.multiselect(
                        "Select Product Groups for this Company Group:",
                        sorted(st.session_state.product_groups),
                        default=valid_current_products,
                        key="company_product_multiselect"
                    )
                    if st.button("Update Company-Product Mapping", key="update_company_product_button"):
                        st.session_state.company_product_mapping[selected_company] = selected_products
                        save_company_mappings()
                        log_update('company_product_mapping', 'Update Company-Product Mapping', f"Updated product groups for company group: {selected_company}")
                        st.success(f"Updated product groups for company group: {selected_company}")
                else:
                    st.info("No product groups available. Please extract or add product groups first.")
            else:
                st.info("Please create company groups first")
        
        with st.container(border=True):
            st.subheader("Current Company Group-Product Group Mappings")
            if st.session_state.company_product_mapping:
                company_mapping_data = []
                for company, products in st.session_state.company_product_mapping.items():
                    company_mapping_data.append({
                        "Company Group": company,
                        "Product Groups": ", ".join(sorted(products)) if products else "None",
                        "Count": len(products)
                    })
                company_mapping_df = pd.DataFrame(company_mapping_data)
                st.dataframe(company_mapping_df, hide_index=True)
                if st.button("Clear All Company Group Mappings", key="clear_company_mappings"):
                    st.session_state.company_product_mapping = {}
                    save_company_mappings()
                    log_update('company_product_mapping', 'Clear Company Mappings', "Cleared all company group mappings")
                    st.success("All company group mappings cleared")
                    st.rerun()
            else:
                st.info("No company group mappings created yet")

    with tab5:
        st.header("Backup & Restore")
        with st.container(border=True):
            st.markdown("""
            This section allows you to back up all your mappings to a single file that you can download and restore later.
            This ensures your mappings are safely stored even if the application data is reset.
            """)
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Backup Mappings")
                st.write("Export all your mappings to a backup file:")
                st.markdown("**Current Data Summary:**")
                st.write(f"- Branch Mappings: {len(st.session_state.branch_exec_mapping)} branches")
                st.write(f"- Region Mappings: {len(st.session_state.region_branch_mapping)} regions")
                st.write(f"- Company Group Mappings: {len(st.session_state.company_product_mapping)} company groups")
                st.write(f"- Executives: {len(st.session_state.executives)}")
                st.write(f"- Product Groups: {len(st.session_state.product_groups)}")
                st.write(f"- Customer Codes: {len(st.session_state.customer_codes)}")
                st.write(f"- Customer Names: {len(st.session_state.customer_names)}")
                st.write(f"- Unmapped Customers: {len(st.session_state.unmapped_customers)}")
                if st.button("Create Backup", key="create_backup_button"):
                    export_all_mappings()
                    
            with col2:
                st.subheader("Restore from Backup")
                st.write("Import mappings from a backup file:")
                backup_file = st.file_uploader("Upload Backup File", type=['json'], key="backup_file_uploader")
                if backup_file is not None:
                    if st.button("Restore Mappings from Backup", key="restore_backup_button"):
                        success = import_mappings_from_file(backup_file)
                        if success:
                            st.rerun()

    with tab6:
        st.header("Consolidated Data View")
        with st.container(border=True):
            st.subheader("Customer Code - Executive - Branch Relationships")
            st.info("This view shows the relationships between customer codes, executives, and branches")
            if st.session_state.customer_codes:
                consolidated_data = []
                for customer_code, executive_name in st.session_state.customer_codes.items():
                    executive_code = st.session_state.executive_codes.get(executive_name, "No code")
                    branch = get_branches_for_executive(executive_name)
                    customer_name = st.session_state.customer_names.get(customer_code, "")
                    consolidated_data.append({
                        "Customer Code": customer_code,
                        "Customer Name": customer_name,
                        "Executive": executive_name,
                        "Executive Code": executive_code,
                        "Branch": branch
                    })
                
                st.write("### Filter Options")
                filter_col1, filter_col2, filter_col3 = st.columns(3)
                with filter_col1:
                    if st.session_state.executives:
                        filter_exec = st.multiselect(
                            "Filter by Executive:",
                            ["All"] + sorted(st.session_state.executives),
                            default=["All"],
                            key="filter_exec_consolidated"
                        )
                    else:
                        filter_exec = ["All"]
                        st.info("No executives available for filtering")
                with filter_col2:
                    branches = list(st.session_state.branch_exec_mapping.keys())
                    if branches:
                        filter_branch = st.multiselect(
                            "Filter by Branch:",
                            ["All"] + sorted(branches),
                            default=["All"],
                            key="filter_branch_consolidated"
                        )
                    else:
                        filter_branch = ["All"]
                        st.info("No branches available for filtering")
                with filter_col3:
                    search_customer = st.text_input(
                        "Search by Customer Code or Name:",
                        key="search_customer_consolidated"
                    )
                
                filtered_data = consolidated_data
                if "All" not in filter_exec and filter_exec:
                    filtered_data = [data for data in filtered_data if data["Executive"] in filter_exec]
                if "All" not in filter_branch and filter_branch:
                    filtered_data = [data for data in filtered_data if data["Branch"] in filter_branch]
                if search_customer:
                    search_term = search_customer.lower()
                    filtered_data = [data for data in filtered_data if (
                        search_term in data["Customer Code"].lower() or 
                        search_term in data["Customer Name"].lower()
                    )]
                
                sort_options = st.radio(
                    "Sort by:",
                    ["Customer Code", "Customer Name", "Executive", "Executive Code"],
                    horizontal=True,
                    key="sort_consolidated"
                )
                if sort_options == "Customer Code":
                    filtered_data.sort(key=lambda x: x["Customer Code"])
                elif sort_options == "Customer Name":
                    filtered_data.sort(key=lambda x: x["Customer Name"])
                elif sort_options == "Executive":
                    filtered_data.sort(key=lambda x: x["Executive"])
                elif sort_options == "Executive Code":
                    filtered_data.sort(key=lambda x: x["Executive Code"])
                
                if filtered_data:
                    st.write(f"### Results ({len(filtered_data)} records)")
                    consolidated_df = pd.DataFrame(filtered_data)
                    csv = consolidated_df.to_csv(index=False)
                    st.download_button(
                        "Download as CSV",
                        csv,
                        "consolidated_data.csv",
                        "text/csv",
                        key="download_consolidated_csv"
                    )
                    st.dataframe(consolidated_df, hide_index=True, use_container_width=True)
                else:
                    st.warning("No records match the selected filters")
            else:
                st.warning("No customer-executive mappings available")
        
        st.markdown("---")
        st.subheader("Executive-Branch Assignments")
        if st.session_state.executives and st.session_state.branch_exec_mapping:
            exec_branch_data = []
            for exec_name in sorted(st.session_state.executives):
                exec_code = st.session_state.executive_codes.get(exec_name, "No code")
                branch = get_branches_for_executive(exec_name)
                customer_count = len(get_customer_codes_for_executive(exec_name))
                exec_branch_data.append({
                    "Executive": exec_name,
                    "Executive Code": exec_code,
                    "Branch": branch,
                    "Customer Count": customer_count
                })
            exec_branch_df = pd.DataFrame(exec_branch_data)
            st.dataframe(exec_branch_df, hide_index=True, use_container_width=True)
            exec_csv = exec_branch_df.to_csv(index=False)
            st.download_button(
                "Download Executive Summary",
                exec_csv,
                "executive_branch_summary.csv",
                "text/csv",
                key="download_exec_summary"
            )
        else:
            st.info("No executive-branch mappings available")

    with tab7:
        st.header("Data Processing & Download")
        process_tab1, process_tab2, process_tab3 = st.tabs(["Budget Processing", "Sales Processing", "OS Processing"])
        
        with process_tab1:
            st.subheader("Process Budget File")
            if st.session_state.budget_df is not None:
                st.info("""
                This will update the existing executive name and executive code columns in the budget file 
                based on the customer code (SL code) mappings from the Customer Code Management section. 
                It will also add or update Branch, Region, and Company Group columns based on your mappings.
                """)
                with st.expander("Preview Original Budget Data", expanded=False):
                    st.dataframe(st.session_state.budget_df.head(10))
                if st.button("Process Budget File", key="process_budget_file_button"):
                    if st.session_state.customer_col and st.session_state.exec_col:
                        processed_budget = process_budget_file(
                            st.session_state.budget_df, 
                            st.session_state.customer_col
                        )
                        st.session_state.processed_budget = processed_budget
                        st.success("Budget file processed successfully!")
                        st.subheader("Preview of Processed Budget Data")
                        st.dataframe(processed_budget.head(10))
                        budget_excel = to_excel_buffer(processed_budget)
                        st.download_button(
                            "Download Processed Budget File",
                            budget_excel,
                            "processed_budget.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_budget_excel"
                        )
                    else:
                        st.error("Required columns (Customer Code and Executive) not selected. Please import the budget file in the Data Import tab.")
            else:
                st.warning("No budget data available. Please upload and extract data from a Budget file in the Data Import tab.")

        with process_tab2:
            st.subheader("Process Sales File")
            sales_file = st.file_uploader("Upload Sales File (Excel)", type=['xlsx', 'xls'], key="sales_file")
            if sales_file is not None:
                sales_file_copy = io.BytesIO(sales_file.getvalue())
                sheet_names = get_sheet_names(sales_file_copy)
                if sheet_names:
                    selected_sheet = st.selectbox("Select Sheet from Sales File:", sheet_names, key="sales_sheet")
                    header_row = st.number_input("Select Header Row (0-based index):", min_value=0, value=1, key="sales_header")
                    sales_df = get_sheet_preview(sales_file_copy, selected_sheet, header_row)
                    if sales_df is not None:
                        st.write("Preview of Sales Data:")
                        st.dataframe(sales_df.head())
                        st.subheader("Select Columns for Mapping")
                        col1, col2 = st.columns(2)
                        with col1:
                            sales_exec_code_col_options = [""] + list(sales_df.columns)
                            default_sales_exec_code_col = "Executive Code" if "Executive Code" in sales_exec_code_col_options else ""
                            sales_exec_code_col = st.selectbox(
                                "Select Executive Code Column:", 
                                sales_exec_code_col_options, 
                                index=sales_exec_code_col_options.index(default_sales_exec_code_col) if default_sales_exec_code_col in sales_exec_code_col_options else 0, 
                                key="sales_exec_code_col"
                            )
                            sales_exec_name_col_options = ["None"] + list(sales_df.columns)
                            default_sales_exec_name_col = "Executive Name" if "Executive Name" in sales_exec_name_col_options else "None"
                            sales_exec_name_col = st.selectbox(
                                "Select Executive Name Column:", 
                                sales_exec_name_col_options, 
                                index=sales_exec_name_col_options.index(default_sales_exec_name_col) if default_sales_exec_name_col in sales_exec_name_col_options else 0, 
                                key="sales_exec_name_col"
                            )
                        with col2:
                            sales_product_col_options = ["None"] + list(sales_df.columns)
                            default_sales_product_col = "Type (Make)" if "Type (Make)" in sales_product_col_options else "None"
                            sales_product_col = st.selectbox(
                                "Select Product Group Column:", 
                                sales_product_col_options, 
                                index=sales_product_col_options.index(default_sales_product_col) if default_sales_product_col in sales_product_col_options else 0, 
                                key="sales_product_col"
                            )
                        
                        # New unit conversion section
                        st.subheader("Unit Conversion Settings")
                        st.info("Configure columns for unit conversion. Units will be converted to MT and values to lakhs.")
                        conv_col1, conv_col2, conv_col3 = st.columns(3)
                        with conv_col1:
                            unit_col_options = ["None"] + list(sales_df.columns)
                            default_unit_col = "UOM" if "UOM" in unit_col_options else "None"
                            unit_col = st.selectbox(
                                "Select Unit Column:",
                                unit_col_options,
                                index=unit_col_options.index(default_unit_col) if default_unit_col in unit_col_options else 0,
                                key="sales_unit_col"
                            )
                        with conv_col2:
                            quantity_col_options = ["None"] + list(sales_df.columns)
                            default_quantity_col = "Quantity" if "Quantity" in quantity_col_options else "None"
                            quantity_col = st.selectbox(
                                "Select Quantity Column:",
                                quantity_col_options,
                                index=quantity_col_options.index(default_quantity_col) if default_quantity_col in quantity_col_options else 0,
                                key="sales_quantity_col"
                            )
                        with conv_col3:
                            value_col_options = ["None"] + list(sales_df.columns)
                            default_value_col = "Product Value" if "Product Value" in value_col_options else "None"
                            value_col = st.selectbox(
                                "Select Value Column:",
                                value_col_options,
                                index=value_col_options.index(default_value_col) if default_value_col in value_col_options else 0,
                                key="sales_value_col"
                            )
                        
                        # Show conversion info
                        if unit_col != "None" and quantity_col != "None":
                            st.info("**Quantity Conversion Rules:**\n- MT → Same quantity\n- KGS/NOS → Divide by 1000 (convert to MT)")
                        if value_col != "None":
                            st.info("**Value Conversion:** All values will be divided by 100,000 (convert to lakhs)")
                        
                        if st.button("Process Sales File", key="process_sales_file_button"):
                            if sales_exec_code_col:
                                product_col = None if sales_product_col == "None" else sales_product_col
                                exec_name_col = None if sales_exec_name_col == "None" else sales_exec_name_col
                                unit_conversion_col = None if unit_col == "None" else unit_col
                                quantity_conversion_col = None if quantity_col == "None" else quantity_col
                                value_conversion_col = None if value_col == "None" else value_col
                                
                                processed_sales = process_sales_file(
                                    sales_df,
                                    sales_exec_code_col,
                                    product_col,
                                    exec_name_col,
                                    unit_conversion_col,
                                    quantity_conversion_col,
                                    value_conversion_col
                                )
                                st.session_state.processed_sales = processed_sales
                                st.success("Sales file processed successfully!")
                                st.subheader("Preview of Processed Sales Data")
                                st.dataframe(processed_sales.head(10))
                                
                                with st.expander("Summary of Mappings Applied", expanded=True):
                                    st.subheader("Mapping Statistics")
                                    exec_name_updated = processed_sales[exec_name_col].notna().sum() if exec_name_col else 0
                                    branch_mapped = processed_sales[processed_sales["Branch"] != ""]["Branch"].count()
                                    region_mapped = processed_sales[processed_sales["Region"] != ""]["Region"].count()
                                    company_mapped = processed_sales[processed_sales["Company Group"] != ""]["Company Group"].count()
                                    
                                    st.write(f"- Records with updated Executive Names: {exec_name_updated}")
                                    st.write(f"- Records with Branch Mappings: {branch_mapped}")
                                    st.write(f"- Records with Region Mappings: {region_mapped}")
                                    st.write(f"- Records with Company Group Mappings: {company_mapped}")
                                    
                                    # Unit conversion statistics
                                    if unit_conversion_col and quantity_conversion_col:
                                        actual_qty_mapped = processed_sales[processed_sales["Actual Quantity"] != ""]["Actual Quantity"].count()
                                        st.write(f"- Records with Quantity Conversions: {actual_qty_mapped}")
                                    
                                    if value_conversion_col:
                                        value_mapped = processed_sales[processed_sales["Value"] != ""]["Value"].count()
                                        st.write(f"- Records with Value Conversions: {value_mapped}")
                                    
                                    st.subheader("Sample of Mapped Data")
                                    sample_cols = [sales_exec_code_col]
                                    if exec_name_col:
                                        sample_cols.append(exec_name_col)
                                    sample_cols.extend(["Branch", "Region", "Company Group"])
                                    if product_col:
                                        sample_cols.append(product_col)
                                    if unit_conversion_col and quantity_conversion_col:
                                        sample_cols.extend([unit_conversion_col, quantity_conversion_col, "Actual Quantity"])
                                    if value_conversion_col:
                                        sample_cols.extend([value_conversion_col, "Value"])
                                    st.dataframe(processed_sales[sample_cols].head(10))
                                
                                sales_excel = to_excel_buffer(processed_sales)
                                st.download_button(
                                    "Download Processed Sales File",
                                    sales_excel,
                                    "processed_sales.xlsx",
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_sales_excel"
                                )
                            else:
                                st.error("Please select the Executive Code column.")
                    else:
                        st.error("Failed to read the sales file. Please check the file format and try again.")
                else:
                    st.warning("No sheets found in the uploaded sales file.")
            else:
                st.info("Please upload a sales file to process.")

        with process_tab3:
            st.subheader("Process OS File")
            st.info("Upload your OS file to add Branch and Region columns based on Executive Code mappings.")
            os_file = st.file_uploader("Upload OS File (Excel)", type=['xlsx', 'xls'], key="os_file")
            if os_file is not None:
                os_file_copy = io.BytesIO(os_file.getvalue())
                sheet_names = get_sheet_names(os_file_copy)
                if sheet_names:
                    selected_sheet = st.selectbox("Select Sheet from OS File:", sheet_names, key="os_sheet")
                    header_row = st.number_input("Select Header Row (0-based index):", min_value=0, value=1, key="os_header")
                    os_df = get_sheet_preview(os_file_copy, selected_sheet, header_row)
                    if os_df is not None:
                        st.write("Preview of OS Data:")
                        st.dataframe(os_df.head())
                        st.subheader("Select Executive Code Column")
                        os_exec_code_col_options = [""] + list(os_df.columns)
                        default_os_exec_code_col = "Executive Code" if "Executive Code" in os_exec_code_col_options else ""
                        os_exec_code_col = st.selectbox(
                            "Select Executive Code Column:", 
                            os_exec_code_col_options, 
                            index=os_exec_code_col_options.index(default_os_exec_code_col) if default_os_exec_code_col in os_exec_code_col_options else 0, 
                            key="os_exec_code_col"
                        )
                        if st.button("Process OS File", key="process_os_file_button"):
                            if os_exec_code_col:
                                processed_os = process_os_file(os_df, os_exec_code_col)
                                st.session_state.processed_os = processed_os
                                st.success("OS file processed successfully!")
                                st.subheader("Preview of Processed OS Data")
                                st.dataframe(processed_os.head(10))
                                
                                with st.expander("Summary of Mappings Applied", expanded=True):
                                    st.subheader("Mapping Statistics")
                                    branch_mapped = processed_os[processed_os["Branch"] != ""]["Branch"].count()
                                    region_mapped = processed_os[processed_os["Region"] != ""]["Region"].count()
                                    
                                    st.write(f"- Records with Branch Mappings: {branch_mapped}")
                                    st.write(f"- Records with Region Mappings: {region_mapped}")
                                    
                                    st.subheader("Sample of Mapped Data")
                                    sample_cols = [os_exec_code_col, "Branch", "Region"]
                                    st.dataframe(processed_os[sample_cols].head(10))
                                
                                os_excel = to_excel_buffer(processed_os)
                                st.download_button(
                                    "Download Processed OS File",
                                    os_excel,
                                    "processed_os.xlsx",
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_os_excel"
                                )
                            else:
                                st.error("Please select the Executive Code column.")
                    else:
                        st.error("Failed to read the OS file. Please check the file format and try again.")
                else:
                    st.warning("No sheets found in the uploaded OS file.")
            else:
                st.info("Please upload an OS file to process.")

    with st.sidebar:
        st.header("Global Operations")
        with st.container(border=True):
            st.subheader("Save All Mappings")
            if st.button("Save All Mappings", key="save_all_mappings_button"):
                save_all_mappings()
                st.success("All mappings saved successfully!")
        
        st.markdown("---")
        st.header("Additional Controls")
        with st.container(border=True):
            if st.session_state.budget_df is not None:
                if st.button("Re-process Budget Data", key="reprocess_budget_button"):
                    reprocess_budget_data()
                    st.rerun()
        
        st.markdown("---")
        st.subheader("Reset All Data")
        st.warning("This will clear all mappings and reset the application to its initial state.")
        if st.button("Reset All Mappings", key="reset_all_button"):
            reset_all_mappings()
            st.session_state.budget_df = None
            st.session_state.processed_budget = None
            st.session_state.processed_sales = None
            st.session_state.processed_os = None  # Reset OS processing state
            st.session_state.customer_col = None
            st.session_state.exec_col = None
            st.session_state.product_col = None
            st.session_state.customer_name_col = None
            st.session_state.exec_code_col = None
            st.success("All mappings and data have been reset")
            st.rerun()

if __name__ == "__main__":
    main()
