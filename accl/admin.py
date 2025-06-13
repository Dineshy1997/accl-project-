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
        st.session_state.executives = []
        st.session_state.executive_codes = {}
        st.session_state.product_groups = []
        st.session_state.customer_codes = {}
        st.session_state.customer_names = {}
        st.session_state.unmapped_customers = []
        st.session_state.branch_exec_mapping = {}
        st.session_state.region_branch_mapping = {}
        st.session_state.company_product_mapping = {}
        load_all_mappings()

def save_metadata():
    ensure_data_dir()
    metadata = {
        "executives": st.session_state.executives,
        "executive_codes": st.session_state.executive_codes,
        "product_groups": st.session_state.product_groups,
        "customer_codes": st.session_state.customer_codes,
        "customer_names": st.session_state.customer_names,
        "unmapped_customers": st.session_state.unmapped_customers
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
    save_all_mappings()
    return True

# NEW REMOVE FUNCTIONS
def remove_branch(branch_name):
    """Remove a branch and all its mappings"""
    if branch_name in st.session_state.branch_exec_mapping:
        # Remove from branch-executive mapping
        del st.session_state.branch_exec_mapping[branch_name]
        
        # Remove from region-branch mappings
        for region, branches in st.session_state.region_branch_mapping.items():
            if branch_name in branches:
                st.session_state.region_branch_mapping[region].remove(branch_name)
        
        save_branch_mappings()
        save_region_mappings()
        return True
    return False

def remove_region(region_name):
    """Remove a region and all its mappings"""
    if region_name in st.session_state.region_branch_mapping:
        del st.session_state.region_branch_mapping[region_name]
        save_region_mappings()
        return True
    return False

def remove_company_group(company_name):
    """Remove a company group and all its mappings"""
    if company_name in st.session_state.company_product_mapping:
        # Get products that were mapped to this company
        products = st.session_state.company_product_mapping[company_name]
        
        # Remove the company
        del st.session_state.company_product_mapping[company_name]
        
        save_company_mappings()
        return True, len(products)
    return False, 0

def remove_product_group(product_name):
    """Remove a product group from all company mappings and product list"""
    removed_count = 0
    
    # Remove from product groups list
    if product_name in st.session_state.product_groups:
        st.session_state.product_groups.remove(product_name)
        removed_count += 1
    
    # Remove from company-product mappings
    for company, products in st.session_state.company_product_mapping.items():
        if product_name in products:
            st.session_state.company_product_mapping[company].remove(product_name)
            removed_count += 1
    
    save_metadata()
    save_company_mappings()
    return removed_count > 0, removed_count

def get_branches_using_executive(exec_name):
    """Get all branches that use a specific executive"""
    branches = []
    for branch, execs in st.session_state.branch_exec_mapping.items():
        if exec_name in execs:
            branches.append(branch)
    return branches

def get_regions_using_branch(branch_name):
    """Get all regions that contain a specific branch"""
    regions = []
    for region, branches in st.session_state.region_branch_mapping.items():
        if branch_name in branches:
            regions.append(region)
    return regions

def get_companies_using_product(product_name):
    """Get all companies that use a specific product"""
    companies = []
    for company, products in st.session_state.company_product_mapping.items():
        if product_name in products:
            companies.append(company)
    return companies

# Continue with existing functions...
def export_mappings():
    ensure_data_dir()
    # Include product_groups in the backup data
    all_data = {
        "branch_exec_mapping": st.session_state.branch_exec_mapping,
        "region_branch_mapping": st.session_state.region_branch_mapping,
        "company_product_mapping": st.session_state.company_product_mapping,
        "product_groups": st.session_state.product_groups,  # Add this line
        "executives": st.session_state.executives,  # Add this line
        "executive_codes": st.session_state.executive_codes  # Add this line
    }
    json_data = json.dumps(all_data, indent=4)
    st.download_button(
        "Download Backup File",
        json_data,
        "mappings_backup.json",
        "application/json",
        key="download_backup"
    )

def import_mappings_from_file(file):
    try:
        file_content = file.read()
        data = json.loads(file_content)
        
        # Restore all mappings
        st.session_state.branch_exec_mapping = data.get("branch_exec_mapping", {})
        st.session_state.region_branch_mapping = data.get("region_branch_mapping", {})
        st.session_state.company_product_mapping = data.get("company_product_mapping", {})
        
        # Restore product groups (this was missing!)
        st.session_state.product_groups = data.get("product_groups", [])
        
        # Also restore executives and executive codes if they exist in backup
        if "executives" in data:
            st.session_state.executives = data.get("executives", [])
        if "executive_codes" in data:
            st.session_state.executive_codes = data.get("executive_codes", {})
        
        # Save all restored data
        save_all_mappings()
        st.success("Successfully restored mappings from backup file")
        return True
    except Exception as e:
        st.error(f"Error importing mappings: {e}")
        return False

def normalize_customer_code(code):
    if pd.isna(code):
        return ""
    code_str = str(code).strip()
    try:
        if '.' in code_str and code_str.replace('.', '').replace('-', '').isdigit():
            float_val = float(code_str)
            if float_val == int(float_val):
                return str(int(float_val))
    except (ValueError, OverflowError):
        pass
    return code_str

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

def get_assigned_executives():
    """Get all executives that are already assigned to branches"""
    assigned_executives = set()
    for branch, execs in st.session_state.branch_exec_mapping.items():
        assigned_executives.update(execs)
    return assigned_executives

def get_assigned_branches():
    """Get all branches that are already assigned to regions"""
    assigned_branches = set()
    for region, branches in st.session_state.region_branch_mapping.items():
        assigned_branches.update(branches)
    return assigned_branches

def get_assigned_products():
    """Get all products that are already assigned to companies"""
    assigned_products = set()
    for company, products in st.session_state.company_product_mapping.items():
        assigned_products.update(products)
    return assigned_products

def get_available_executives_for_branch(current_branch=None):
    """Get executives available for assignment to a branch"""
    assigned_executives = get_assigned_executives()
    available_executives = []    
    for exec_name in st.session_state.executives:
        if exec_name not in assigned_executives:
            available_executives.append(exec_name)
        elif current_branch and exec_name in st.session_state.branch_exec_mapping.get(current_branch, []):
            # Include executives already assigned to current branch
            available_executives.append(exec_name)
    
    return sorted(available_executives)

def get_available_branches_for_region(current_region=None):
    """Get branches available for assignment to a region"""
    assigned_branches = get_assigned_branches()
    available_branches = []    
    for branch in st.session_state.branch_exec_mapping.keys():
        if branch not in assigned_branches:
            available_branches.append(branch)
        elif current_region and branch in st.session_state.region_branch_mapping.get(current_region, []):
            # Include branches already assigned to current region
            available_branches.append(branch)    
    return sorted(available_branches)

def get_available_products_for_company(current_company=None):
    """Get products available for assignment to a company"""
    assigned_products = get_assigned_products()
    available_products = []    
    for product in st.session_state.product_groups:
        if product not in assigned_products:
            available_products.append(product)
        elif current_company and product in st.session_state.company_product_mapping.get(current_company, []):
            # Include products already assigned to current company
            available_products.append(product)    
    return sorted(available_products)

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
        return True, len(customer_codes)
    return False, 0

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

def apply_reassignment_changes(relationships, exec_codes, cust_names):
    new_execs_added = 0
    new_exec_codes = 0
    new_assignments = 0
    reassignments = 0    
    for exec_name, exec_code in exec_codes.items():
        if exec_name not in st.session_state.executives:
            st.session_state.executives.append(exec_name)
            new_execs_added += 1
        if exec_code:
            st.session_state.executive_codes[exec_name] = exec_code
            new_exec_codes += 1    
    for cust_code, exec_name in relationships.items():
        normalized_code = normalize_customer_code(cust_code)
        if normalized_code in st.session_state.customer_codes:
            if st.session_state.customer_codes[normalized_code] != exec_name:
                reassignments += 1
        else:
            new_assignments += 1
        st.session_state.customer_codes[normalized_code] = exec_name
        if normalized_code in st.session_state.unmapped_customers:
            st.session_state.unmapped_customers.remove(normalized_code)    
    new_customer_names = 0
    for cust_code, cust_name in cust_names.items():
        normalized_code = normalize_customer_code(cust_code)
        if normalized_code not in st.session_state.customer_names:
            new_customer_names += 1
        st.session_state.customer_names[normalized_code] = cust_name    
    save_metadata()
    save_all_mappings()    
    st.success(f"Successfully processed: Added {new_execs_added} new executives, {new_exec_codes} executive codes, {new_assignments} new assignments, {reassignments} reassignments, {new_customer_names} customer names")

def normalize_product_name(product_name):
    """
    Normalize product name for better matching
    """
    if pd.isna(product_name):
        return ""
    
    # Convert to string and normalize
    normalized = str(product_name).strip()
    # Remove extra spaces, convert to lowercase
    normalized = ' '.join(normalized.split()).lower()
    # Remove common punctuation that might interfere
    normalized = normalized.replace('-', ' ').replace('_', ' ').replace('.', ' ')
    # Remove extra spaces again after replacements
    normalized = ' '.join(normalized.split())
    
    return normalized

def get_company_for_product_improved(product_name):
    """
    Improved product to company mapping with fuzzy matching
    """
    if not product_name:
        return ""
    
    normalized_input = normalize_product_name(product_name)
    
    # First try exact match
    for company, products in st.session_state.company_product_mapping.items():
        for mapped_product in products:
            if normalize_product_name(mapped_product) == normalized_input:
                return company
    
    # If no exact match, try partial matching
    for company, products in st.session_state.company_product_mapping.items():
        for mapped_product in products:
            normalized_mapped = normalize_product_name(mapped_product)
            
            # Check if either contains the other (for partial matches)
            if (normalized_mapped in normalized_input or 
                normalized_input in normalized_mapped):
                # Only accept if the match is significant (>60% of either string)
                match_score = min(len(normalized_mapped), len(normalized_input)) / max(len(normalized_mapped), len(normalized_input))
                if match_score > 0.6:
                    return company
    
    return ""

def process_branch_region_file(df, exec_code_col, exec_name_col, branch_col, region_col):
    """Process branch and region mapping file"""
    branch_updates = 0
    region_updates = 0
    new_branches = 0
    new_regions = 0
    temp_exec_to_branch = {}
    temp_branch_to_region = {}    
    for _, row in df.iterrows():
        if pd.notna(row[exec_name_col]) and pd.notna(row[branch_col]):
            exec_name = str(row[exec_name_col]).strip()
            branch_name = str(row[branch_col]).strip()
            if exec_name not in st.session_state.executives:
                st.session_state.executives.append(exec_name)
            if exec_code_col != "None" and pd.notna(row[exec_code_col]):
                exec_code = str(row[exec_code_col]).strip()
                st.session_state.executive_codes[exec_name] = exec_code
            if branch_name not in st.session_state.branch_exec_mapping:
                st.session_state.branch_exec_mapping[branch_name] = []
                new_branches += 1
            temp_exec_to_branch[exec_name] = branch_name
            if region_col != "None" and pd.notna(row[region_col]):
                region_name = str(row[region_col]).strip()
                if region_name not in st.session_state.region_branch_mapping:
                    st.session_state.region_branch_mapping[region_name] = []
                    new_regions += 1                
                temp_branch_to_region[branch_name] = region_name
    for exec_name, branch_name in temp_exec_to_branch.items():
        if exec_name not in st.session_state.branch_exec_mapping[branch_name]:
            st.session_state.branch_exec_mapping[branch_name].append(exec_name)
            branch_updates += 1
    for branch_name, region_name in temp_branch_to_region.items():
        if branch_name not in st.session_state.region_branch_mapping[region_name]:
            st.session_state.region_branch_mapping[region_name].append(branch_name)
            region_updates += 1    
    save_all_mappings()
    st.success(f"Successfully processed: {new_branches} new branches, {new_regions} new regions, {branch_updates} executive-branch mappings, {region_updates} branch-region mappings")

def process_company_product_file(df, company_col, product_col):
    """Process company and product mapping file"""
    company_updates = 0
    new_companies = 0
    new_products = 0    
    for _, row in df.iterrows():
        if pd.notna(row[company_col]) and pd.notna(row[product_col]):
            company_name = str(row[company_col]).strip()
            product_name = str(row[product_col]).strip()
            if product_name not in st.session_state.product_groups:
                st.session_state.product_groups.append(product_name)
                new_products += 1
            if company_name not in st.session_state.company_product_mapping:
                st.session_state.company_product_mapping[company_name] = []
                new_companies += 1
            if product_name not in st.session_state.company_product_mapping[company_name]:
                st.session_state.company_product_mapping[company_name].append(product_name)
                company_updates += 1    
    save_all_mappings()
    st.success(f"Successfully processed: {new_companies} new companies, {new_products} new products, {company_updates} company-product mappings")

def process_budget_file(budget_df, customer_col, exec_code_col, exec_name_col, branch_col, region_col, cust_name_col):
    processed_df = budget_df.copy()
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    if "Company Group" not in processed_df.columns:
        processed_df["Company Group"] = ""    
    for idx, row in processed_df.iterrows():
        if pd.notna(row[customer_col]):
            customer_code = normalize_customer_code(str(row[customer_col]).strip())            
            if customer_code in st.session_state.customer_codes:
                exec_name = st.session_state.customer_codes[customer_code]
                if exec_code_col in processed_df.columns and exec_name in st.session_state.executive_codes:
                    processed_df.at[idx, exec_code_col] = st.session_state.executive_codes[exec_name]
                if exec_name_col in processed_df.columns:
                    processed_df.at[idx, exec_name_col] = exec_name
                branch = get_branches_for_executive(exec_name)
                if branch_col in processed_df.columns:
                    processed_df.at[idx, branch_col] = branch
                processed_df.at[idx, "Branch"] = branch
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    if region_col in processed_df.columns:
                        processed_df.at[idx, region_col] = region
                    processed_df.at[idx, "Region"] = region    
    return processed_df

def process_sales_file(sales_df, exec_code_col, product_col=None, exec_name_col=None, unit_col=None, quantity_col=None, value_col=None):
    processed_df = sales_df.copy()
    
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    if "Company Group" not in processed_df.columns:
        processed_df["Company Group"] = ""
    
    if unit_col and quantity_col and unit_col != "None" and quantity_col != "None":
        processed_df["Actual Quantity"] = ""
    if value_col and value_col != "None":
        processed_df["Value"] = ""
    
    # Debug counters
    total_rows = len(processed_df)
    exec_found_count = 0
    branch_mapped_count = 0
    region_mapped_count = 0
    product_mapped_count = 0
    
    # Debug lists
    debug_products = []
    
    for idx, row in processed_df.iterrows():
        exec_name = None
        
        # Method 1: Try to find executive by code
        if pd.notna(row[exec_code_col]):
            exec_code = str(row[exec_code_col]).strip()
            
            # Look for executive by code
            for name, code in st.session_state.executive_codes.items():
                if str(code).strip() == exec_code:
                    exec_name = name
                    exec_found_count += 1
                    break
            
            # Method 2: If not found by code, try by name (if exec_name_col is provided and has data)
            if not exec_name and exec_name_col and exec_name_col != "None" and pd.notna(row[exec_name_col]):
                potential_name = str(row[exec_name_col]).strip()
                if potential_name in st.session_state.executives:
                    exec_name = potential_name
                    exec_found_count += 1
        
        # Update executive name if found
        if exec_name:
            if exec_name_col and exec_name_col in processed_df.columns:
                processed_df.at[idx, exec_name_col] = exec_name
            
            # Get and update branch
            branch = get_branches_for_executive(exec_name)
            if branch:
                processed_df.at[idx, "Branch"] = branch
                branch_mapped_count += 1
                
                # Get and update region (only if single branch)
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    if region:
                        processed_df.at[idx, "Region"] = region
                        region_mapped_count += 1
        
        # Product to Company mapping - FIXED VERSION
        if product_col and product_col != "None" and pd.notna(row[product_col]):
            product_name = str(row[product_col]).strip()
            
            # Normalize product name for better matching
            product_normalized = product_name.lower().strip()
            product_normalized = ' '.join(product_normalized.split())  # Remove extra spaces
            
            company_group = ""
            
            # Try exact match first
            for company, products in st.session_state.company_product_mapping.items():
                for mapped_product in products:
                    mapped_normalized = mapped_product.lower().strip()
                    mapped_normalized = ' '.join(mapped_normalized.split())
                    
                    if product_normalized == mapped_normalized:
                        company_group = company
                        break
                if company_group:
                    break
            
            # If no exact match, try partial match
            if not company_group:
                for company, products in st.session_state.company_product_mapping.items():
                    for mapped_product in products:
                        mapped_normalized = mapped_product.lower().strip()
                        mapped_normalized = ' '.join(mapped_normalized.split())
                        
                        # Check if either contains the other
                        if (mapped_normalized in product_normalized or 
                            product_normalized in mapped_normalized):
                            company_group = company
                            break
                    if company_group:
                        break
            
            # Update the dataframe
            if company_group:
                processed_df.at[idx, "Company Group"] = company_group
                product_mapped_count += 1
                debug_products.append(f"✅ '{product_name}' → '{company_group}'")
            else:
                debug_products.append(f"❌ '{product_name}' → No match found")
        
        # Handle quantity conversion
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
                actual_quantity = quantity
            
            processed_df.at[idx, "Actual Quantity"] = actual_quantity
        
        # Handle value conversion
        if value_col and value_col != "None" and pd.notna(row[value_col]):
            try:
                value = float(row[value_col]) if isinstance(row[value_col], (int, float, str)) else 0
            except (ValueError, TypeError):
                value = 0
            converted_value = value / 100000
            processed_df.at[idx, "Value"] = converted_value
    
    # Display processing statistics
    st.info(f"""
    Processing Summary:
    - Total rows: {total_rows}
    - Executives found: {exec_found_count}
    - Branches mapped: {branch_mapped_count}
    - Regions mapped: {region_mapped_count}
    - Products mapped: {product_mapped_count}
    """)
    
    return processed_df

def process_os_file(os_df, exec_code_col):
    processed_df = os_df.copy()
    
    if "Branch" not in processed_df.columns:
        processed_df["Branch"] = ""
    if "Region" not in processed_df.columns:
        processed_df["Region"] = ""
    total_rows = len(processed_df)
    exec_found_count = 0
    branch_mapped_count = 0
    region_mapped_count = 0
    exec_not_found = []
    branch_not_found = []    
    for idx, row in processed_df.iterrows():
        exec_name = None
        exec_code_from_file = None        
        if pd.notna(row[exec_code_col]):
            exec_code_from_file = str(row[exec_code_col]).strip()
            for name, code in st.session_state.executive_codes.items():
                if str(code).strip() == exec_code_from_file:
                    exec_name = name
                    exec_found_count += 1
            if not exec_name:
                for name, code in st.session_state.executive_codes.items():
                    if str(code).strip().lower() == exec_code_from_file.lower():
                        exec_name = name
                        exec_found_count += 1
                        break
            if not exec_name:
                for name, code in st.session_state.executive_codes.items():
                    if exec_code_from_file in str(code).strip() or str(code).strip() in exec_code_from_file:
                        exec_name = name
                        exec_found_count += 1
                        break
            if not exec_name:
                exec_not_found.append(exec_code_from_file)
        if exec_name:
            branch = get_branches_for_executive(exec_name)
            if branch:
                processed_df.at[idx, "Branch"] = branch
                branch_mapped_count += 1
                if branch and "," not in branch:
                    region = get_region_for_branch(branch)
                    if region:
                        processed_df.at[idx, "Region"] = region
                        region_mapped_count += 1
                    else:
                        branch_not_found.append(branch)
                else:
                    if branch:
                        processed_df.at[idx, "Region"] = "Multiple Branches"
            else:
                branch_not_found.append(f"Executive: {exec_name}")
    st.info(f"""
    Processing Summary:
    - Total rows: {total_rows}
    - Executives found: {exec_found_count}
    - Branches mapped: {branch_mapped_count}
    - Regions mapped: {region_mapped_count}
    """)
    if exec_not_found:
        unique_not_found = list(set(exec_not_found))
        st.warning(f"Executive codes not found in system: {unique_not_found[:10]}")        
    if branch_not_found:
        unique_branch_issues = list(set(branch_not_found))
        st.warning(f"Branch/Region mapping issues: {unique_branch_issues[:10]}")    
    return processed_df

def debug_executive_mappings():
    """Helper function to debug executive mappings"""
    st.write("**Current Executive Mappings:**")
    if st.session_state.executives:
        debug_data = []
        for exec_name in st.session_state.executives:
            exec_code = st.session_state.executive_codes.get(exec_name, "No Code")
            branch = get_branches_for_executive(exec_name)
            region = get_region_for_branch(branch) if branch and "," not in branch else ""
            debug_data.append({
                "Executive": exec_name,
                "Code": exec_code,
                "Branch": branch,
                "Region": region
            })
        st.dataframe(pd.DataFrame(debug_data), hide_index=True)
    else:
        st.warning("No executives found in the system")    
    st.write("**Current Branch Mappings:**")
    if st.session_state.branch_exec_mapping:
        branch_data = []
        for branch, execs in st.session_state.branch_exec_mapping.items():
            region = get_region_for_branch(branch)
            branch_data.append({
                "Branch": branch,
                "Executives": ", ".join(execs) if execs else "None",
                "Region": region
            })
        st.dataframe(pd.DataFrame(branch_data), hide_index=True)
    else:
        st.warning("No branch mappings found")    
    st.write("**Current Company Mappings:**")
    if st.session_state.company_product_mapping:
        company_data = []
        for company, products in st.session_state.company_product_mapping.items():
            company_data.append({
                "Company": company,
                "Products": ", ".join(products) if products else "None"
            })
        st.dataframe(pd.DataFrame(company_data), hide_index=True)
    else:
        st.warning("No company mappings found")

def smart_column_selector(label, columns, field_type, key=None, include_none=False):
    """
    Smart column selector with auto-detection (case-insensitive)
    """
    # Auto-mapping patterns (condensed) - all lowercase for matching
    patterns = {
        'executive_name': ['empname', 'executive name', 'exec name', 'sales rep', 'rep name', 'salesperson'],
        'executive_code': ['executive code', 'empcode', 'exec code', 'sales code', 'rep code', 'emp code'],
        'customer_code': ['customer code', 'slcode', 'sl code', 'cust code', 'customer id', 'client code'],
        'customer_name': ['customer name', 'slname', 'party name', 'cust name', 'customer', 'client name'],
        'branch': ['branch', 'office', 'location'],
        'region': ['region', 'zone', 'territory', 'area'],
        'company': ['company group', 'organization', 'firm'],
        'product': ['product group', 'type (make)', 'item'],
        'unit': ['uom', 'measure'],
        'quantity': ['quantity', 'qty', 'amount', 'volume'],
        'value': ['product value', 'price', 'total', 'cost']
    }
    
    # Find best match (case-insensitive)
    best_match = 0
    auto_detected = ""
    
    if field_type in patterns:
        # Sort patterns by length (longest first) to prioritize more specific matches
        sorted_patterns = sorted(patterns[field_type], key=len, reverse=True)
        
        for col in columns:
            col_lower = col.lower().strip().replace('_', ' ').replace('-', ' ')  # Normalize column name
            
            for pattern in sorted_patterns:
                pattern_lower = pattern.lower().strip()
                
                # Exact match gets highest priority
                if col_lower == pattern_lower:
                    auto_detected = col
                    best_match = 1.0
                    break
                
                # Pattern contained in column (column is longer/more specific)
                elif pattern_lower in col_lower:
                    # Only match if pattern takes up significant portion of column name
                    score = len(pattern_lower) / len(col_lower)
                    if score >= 0.6 and score > best_match:  # At least 60% match
                        best_match = score
                        auto_detected = col
                
                # Column contained in pattern (avoid partial matches like "product" matching "product value")
                # Only allow this for very short column names that are clearly abbreviations
                elif col_lower in pattern_lower and len(col_lower) <= 4:
                    score = len(col_lower) / len(pattern_lower)
                    if score > best_match:
                        best_match = score
                        auto_detected = col
            
            # Break if exact match found
            if best_match == 1.0:
                break
    
    # Create options list
    options = ["None"] + list(columns) if include_none else list(columns)
    
    # Set default index
    default_index = 0
    if auto_detected and auto_detected in options:
        default_index = options.index(auto_detected)
        label = f"{label} {'🎯' if best_match > 0.5 else '❓'}"
    
    return st.selectbox(label, options, index=default_index, key=key)

def main():
    st.set_page_config(
        page_title="Executive Mapping Admin Portal",
        page_icon="🔧",
        layout="wide"
    )
    init_session_state()
    st.title("Executive Mapping Administration Portal")
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Executive Management", 
        "Branch & Region Mapping", 
        "Company & Product Mapping",
        "Backup & Restore",
        "Consolidated Data View",
        "File Processing"
    ])
    
    with tab1:
        st.header("Executive Management")
        exec_tab1, exec_tab2 = st.tabs(["Executive Creation", "Customer Code Management"])
        
        with exec_tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Add New Executive")
                new_exec_name = st.text_input("Executive Name:")
                new_exec_code = st.text_input("Executive Code:")
                if st.button("Add Executive"):
                    if new_exec_name:
                        if new_exec_name in st.session_state.executives:
                            st.warning(f"Executive {new_exec_name} already exists")
                        else:
                            st.session_state.executives.append(new_exec_name)
                            if new_exec_code:
                                st.session_state.executive_codes[new_exec_name] = new_exec_code
                            save_metadata()
                            st.success(f"Added: {new_exec_name}")
            
            with col2:
                st.subheader("Current Executives")
                if st.session_state.executives:
                    exec_data = []
                    for exec_name in sorted(st.session_state.executives):
                        exec_code = st.session_state.executive_codes.get(exec_name, "")
                        customer_count = len(get_customer_codes_for_executive(exec_name))
                        branches = get_branches_using_executive(exec_name)
                        exec_data.append({
                            "Executive": exec_name,
                            "Code": exec_code,
                            "Customers": customer_count,
                            "Branches": len(branches)
                        })
                    st.dataframe(pd.DataFrame(exec_data), hide_index=True)
                    
                    # ENHANCED REMOVE EXECUTIVE with warning
                    exec_to_remove = st.selectbox("Remove Executive:", [""] + sorted(st.session_state.executives))
                    if exec_to_remove:
                        branches_using = get_branches_using_executive(exec_to_remove)
                        customer_count = len(get_customer_codes_for_executive(exec_to_remove))
                        
                        if branches_using or customer_count > 0:
                            st.warning(f"⚠️ Removing '{exec_to_remove}' will affect:")
                            if branches_using:
                                st.write(f"- **Branches:** {', '.join(branches_using)}")
                            if customer_count > 0:
                                st.write(f"- **Customer codes:** {customer_count} codes will become unmapped")
                        
                        if st.button("🗑️ Remove Executive", type="secondary"):
                            success, count = remove_executive(exec_to_remove)
                            if success:
                                st.success(f"Removed {exec_to_remove}. {count} customer codes moved to unmapped.")
                                st.rerun()
                else:
                    st.info("No executives added yet")
        
        with exec_tab2:
            st.subheader("Bulk Customer Assignment")
            reassignment_file = st.file_uploader("Upload Executive-Customer File", type=['xlsx', 'xls'])
            
            if reassignment_file is not None:
                file_copy = io.BytesIO(reassignment_file.getvalue())
                sheet_names = get_sheet_names(file_copy)
                if sheet_names:
                    selected_sheet = st.selectbox("Select Sheet:", sheet_names)
                    header_row = st.number_input("Header Row:", min_value=0, value=0)
                    df = get_sheet_preview(file_copy, selected_sheet, header_row)
                    
                    if df is not None:
                        st.dataframe(df.head())
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            exec_name_col = smart_column_selector("Executive Name Column", df.columns, 'executive_name')
                            exec_code_col = smart_column_selector("Executive Code Column", df.columns, 'executive_code', include_none=True)
                        with col2:
                            cust_code_col = smart_column_selector("Customer Code Column", df.columns, 'customer_code')
                            cust_name_col = smart_column_selector("Customer Name Column", df.columns, 'customer_name', include_none=True)
                        
                        if st.button("Process File"):
                            relationships, exec_codes, cust_names = extract_executive_customer_from_file(
                               df, exec_name_col, cust_code_col, exec_code_col, cust_name_col, True
                           )
                            apply_reassignment_changes(relationships, exec_codes, cust_names)
                            st.rerun()
           
            st.markdown("---")
            st.subheader("Manual Customer Management")
            if st.session_state.executives:
                selected_exec = st.selectbox("Select Executive:", sorted(st.session_state.executives))
                exec_code = st.session_state.executive_codes.get(selected_exec, "No code")
                st.write(f"Executive Code: **{exec_code}**")
                
                customer_codes = get_customer_codes_for_executive(selected_exec)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader(f"Assigned Customers ({len(customer_codes)})")
                    if customer_codes:
                        code_data = []
                        for code in sorted(customer_codes)[:20]:
                            name = st.session_state.customer_names.get(code, "")
                            code_data.append({"Code": code, "Name": name})
                        st.dataframe(pd.DataFrame(code_data), hide_index=True)
                        if len(customer_codes) > 20:
                            st.caption(f"Showing 20 of {len(customer_codes)} customers")
                    else:
                        st.info("No customers assigned")
                
                with col2:
                    st.subheader("Actions")
                    if customer_codes:
                        display_options = {get_customer_info_string(code): code for code in sorted(customer_codes)}
                        codes_to_remove = st.multiselect("Remove Customers:", list(display_options.keys()))
                        if st.button("Remove Selected") and codes_to_remove:
                            actual_codes = [display_options[text] for text in codes_to_remove]
                            count = remove_customer_codes(selected_exec, actual_codes)
                            st.success(f"Removed {count} customers")
                            st.rerun()
                    
                    if st.session_state.unmapped_customers:
                        st.write(f"Unmapped Customers: {len(st.session_state.unmapped_customers)}")
                        display_options = {get_customer_info_string(code): code for code in sorted(st.session_state.unmapped_customers)}
                        customers_to_assign = st.multiselect("Assign Customers:", list(display_options.keys()))
                        if st.button("Assign Selected") and customers_to_assign:
                            actual_codes = [display_options[text] for text in customers_to_assign]
                            count = assign_customer_codes(selected_exec, actual_codes)
                            st.success(f"Assigned {count} customers")
                            st.rerun()
                    
                    new_codes = st.text_area("Add New Customer Codes (one per line):")
                    if st.button("Add Codes") and new_codes:
                        codes_list = [code.strip() for code in new_codes.split('\n') if code.strip()]
                        if codes_list:
                            count = assign_customer_codes(selected_exec, codes_list)
                            st.success(f"Added {count} customers")
                            st.rerun()
            else:
                st.warning("No executives available")
            
            if st.session_state.unmapped_customers:
                st.subheader(f"Unmapped Customers ({len(st.session_state.unmapped_customers)})")
                unmapped_data = []
                for code in sorted(st.session_state.unmapped_customers)[:50]:
                    name = st.session_state.customer_names.get(code, "")
                    unmapped_data.append({"Code": code, "Name": name})
                st.dataframe(pd.DataFrame(unmapped_data), hide_index=True)
                if len(st.session_state.unmapped_customers) > 50:
                    st.caption(f"Showing 50 of {len(st.session_state.unmapped_customers)} unmapped customers")

    with tab2:
        st.header("Branch & Region Mapping")
        branch_tab1, branch_tab2 = st.tabs(["Manual Entry", "File Upload"])
        
        with branch_tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Create Branch")
                new_branch = st.text_input("Branch Name:")
                if st.button("Create Branch") and new_branch:
                    if new_branch not in st.session_state.branch_exec_mapping:
                        st.session_state.branch_exec_mapping[new_branch] = []
                        save_branch_mappings()
                        st.success(f"Created: {new_branch}")
                    else:
                        st.warning("Branch already exists")
                
                st.subheader("Create Region")
                new_region = st.text_input("Region Name:")
                if st.button("Create Region") and new_region:
                    if new_region not in st.session_state.region_branch_mapping:
                        st.session_state.region_branch_mapping[new_region] = []
                        save_region_mappings()
                        st.success(f"Created: {new_region}")
                    else:
                        st.warning("Region already exists")
            
            with col2:
                st.subheader("Current Branches")
                branches = list(st.session_state.branch_exec_mapping.keys())
                if branches:
                    branch_data = []
                    for branch in sorted(branches):
                        exec_count = len(st.session_state.branch_exec_mapping[branch])
                        regions_using = get_regions_using_branch(branch)
                        branch_data.append({
                            "Branch": branch,
                            "Executives": exec_count,
                            "In Regions": len(regions_using)
                        })
                    st.dataframe(pd.DataFrame(branch_data), hide_index=True)
                    
                    # ENHANCED REMOVE BRANCH with warning
                    branch_to_remove = st.selectbox("Remove Branch:", [""] + sorted(branches))
                    if branch_to_remove:
                        execs_in_branch = st.session_state.branch_exec_mapping[branch_to_remove]
                        regions_using = get_regions_using_branch(branch_to_remove)
                        
                        if execs_in_branch or regions_using:
                            st.warning(f"⚠️ Removing '{branch_to_remove}' will affect:")
                            if execs_in_branch:
                                st.write(f"- **Executives:** {', '.join(execs_in_branch)}")
                            if regions_using:
                                st.write(f"- **Regions:** {', '.join(regions_using)}")
                        
                        if st.button("🗑️ Remove Branch", type="secondary"):
                            success = remove_branch(branch_to_remove)
                            if success:
                                st.success(f"Removed branch: {branch_to_remove}")
                                st.rerun()
                else:
                    st.info("No branches created")
                
                st.subheader("Current Regions")
                regions = list(st.session_state.region_branch_mapping.keys())
                if regions:
                    region_data = []
                    for region in sorted(regions):
                        branch_count = len(st.session_state.region_branch_mapping[region])
                        region_data.append({
                            "Region": region,
                            "Branches": branch_count
                        })
                    st.dataframe(pd.DataFrame(region_data), hide_index=True)
                    
                    # ENHANCED REMOVE REGION with warning
                    region_to_remove = st.selectbox("Remove Region:", [""] + sorted(regions))
                    if region_to_remove:
                        branches_in_region = st.session_state.region_branch_mapping[region_to_remove]
                        
                        if branches_in_region:
                            st.warning(f"⚠️ Removing '{region_to_remove}' will affect:")
                            st.write(f"- **Branches:** {', '.join(branches_in_region)}")
                        
                        if st.button("🗑️ Remove Region", type="secondary"):
                            success = remove_region(region_to_remove)
                            if success:
                                st.success(f"Removed region: {region_to_remove}")
                                st.rerun()
                else:
                    st.info("No regions created")
            
            st.markdown("---")
            
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Map Executives to Branches")
                if branches:
                    selected_branch = st.selectbox("Select Branch:", branches)
                    current_execs = st.session_state.branch_exec_mapping.get(selected_branch, [])
                    available_execs = get_available_executives_for_branch(selected_branch)
                    
                    if available_execs:
                        selected_execs = st.multiselect(
                            "Select Executives:",
                            available_execs,
                            default=[exec for exec in current_execs if exec in available_execs]
                        )
                        if st.button("Update Branch Mapping"):
                            st.session_state.branch_exec_mapping[selected_branch] = selected_execs
                            save_branch_mappings()
                            st.success("Updated branch mapping")
                            st.rerun()
                    else:
                        st.info("No available executives")
                else:
                    st.info("Create branches first")
            
            with col2:
                st.subheader("Map Branches to Regions")
                if regions:
                    selected_region = st.selectbox("Select Region:", regions)
                    current_branches = st.session_state.region_branch_mapping.get(selected_region, [])
                    available_branches = get_available_branches_for_region(selected_region)
                    
                    if available_branches:
                        selected_branches = st.multiselect(
                            "Select Branches:",
                            available_branches,
                            default=[branch for branch in current_branches if branch in available_branches]
                        )
                        if st.button("Update Region Mapping"):
                            st.session_state.region_branch_mapping[selected_region] = selected_branches
                            save_region_mappings()
                            st.success("Updated region mapping")
                            st.rerun()
                    else:
                        st.info("No available branches")
                else:
                    st.info("Create regions first")
            
            st.subheader("Current Mappings")
            if st.session_state.branch_exec_mapping:
                mapping_data = []
                for branch, execs in st.session_state.branch_exec_mapping.items():
                    region = get_region_for_branch(branch)
                    mapping_data.append({
                        "Branch": branch,
                        "Region": region,
                        "Executives": ", ".join(sorted(execs)) if execs else "None",
                        "Count": len(execs)
                    })
                st.dataframe(pd.DataFrame(mapping_data), hide_index=True)
            else:
                st.info("No mappings created")
        
        with branch_tab2:
            st.subheader("Upload Branch & Region Mapping File")
            branch_file = st.file_uploader("Upload Branch-Region File", type=['xlsx', 'xls'], key="branch_file")
            
            if branch_file is not None:
                file_copy = io.BytesIO(branch_file.getvalue())
                sheet_names = get_sheet_names(file_copy)
                if sheet_names:
                    selected_sheet = st.selectbox("Select Sheet:", sheet_names, key="branch_sheet")
                    header_row = st.number_input("Header Row:", min_value=0, value=0, key="branch_header")
                    df = get_sheet_preview(file_copy, selected_sheet, header_row)
                    
                    if df is not None:
                        st.dataframe(df.head())
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            exec_code_col = smart_column_selector("Executive Code Column", df.columns, 'executive_code', key="br_exec_code", include_none=True)
                            exec_name_col = smart_column_selector("Executive Name Column", df.columns, 'executive_name', key="br_exec_name")
                        with col2:
                            branch_col = smart_column_selector("Branch Column", df.columns, 'branch', key="br_branch")
                            region_col = smart_column_selector("Region Column", df.columns, 'region', key="br_region", include_none=True)
                        
                        if st.button("Process Branch-Region File"):
                            process_branch_region_file(df, exec_code_col, exec_name_col, branch_col, region_col)
                            st.rerun()

    with tab3:
        st.header("Company & Product Mapping")
        company_tab1, company_tab2 = st.tabs(["Manual Entry", "File Upload"])
        
        with company_tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Create Product Group")
                new_product = st.text_input("Product Group Name:")
                if st.button("Create Product") and new_product:
                    if new_product not in st.session_state.product_groups:
                        st.session_state.product_groups.append(new_product)
                        save_metadata()
                        st.success(f"Created: {new_product}")
                    else:
                        st.warning("Product already exists")
                
                st.subheader("Create Company Group")
                new_company = st.text_input("Company Group Name:")
                if st.button("Create Company") and new_company:
                    if new_company not in st.session_state.company_product_mapping:
                        st.session_state.company_product_mapping[new_company] = []
                        save_company_mappings()
                        st.success(f"Created: {new_company}")
                    else:
                        st.warning("Company already exists")
            
            with col2:
                st.subheader("Current Products")
                if st.session_state.product_groups:
                    product_data = []
                    for product in sorted(st.session_state.product_groups):
                        companies_using = get_companies_using_product(product)
                        product_data.append({
                            "Product": product,
                            "In Companies": len(companies_using)
                        })
                    st.dataframe(pd.DataFrame(product_data), hide_index=True)
                    
                    # ENHANCED REMOVE PRODUCT with warning
                    product_to_remove = st.selectbox("Remove Product:", [""] + sorted(st.session_state.product_groups))
                    if product_to_remove:
                        companies_using = get_companies_using_product(product_to_remove)
                        
                        if companies_using:
                            st.warning(f"⚠️ Removing '{product_to_remove}' will affect:")
                            st.write(f"- **Companies:** {', '.join(companies_using)}")
                        
                        if st.button("🗑️ Remove Product", type="secondary"):
                            success, count = remove_product_group(product_to_remove)
                            if success:
                                st.success(f"Removed product: {product_to_remove} from {count} mappings")
                                st.rerun()
                else:
                    st.info("No products created")
                
                st.subheader("Current Companies")
                companies = list(st.session_state.company_product_mapping.keys())
                if companies:
                    company_data = []
                    for company in sorted(companies):
                        product_count = len(st.session_state.company_product_mapping[company])
                        company_data.append({
                            "Company": company,
                            "Products": product_count
                        })
                    st.dataframe(pd.DataFrame(company_data), hide_index=True)
                    
                    # ENHANCED REMOVE COMPANY with warning
                    company_to_remove = st.selectbox("Remove Company:", [""] + sorted(companies))
                    if company_to_remove:
                        products_in_company = st.session_state.company_product_mapping[company_to_remove]
                        
                        if products_in_company:
                            st.warning(f"⚠️ Removing '{company_to_remove}' will affect:")
                            st.write(f"- **Products:** {', '.join(products_in_company)}")
                        
                        if st.button("🗑️ Remove Company", type="secondary"):
                            success, count = remove_company_group(company_to_remove)
                            if success:
                                st.success(f"Removed company: {company_to_remove} ({count} products unmapped)")
                                st.rerun()
                else:
                    st.info("No companies created")
            
            st.markdown("---")
            st.subheader("Map Products to Companies")
            if companies:
                selected_company = st.selectbox("Select Company:", companies)
                current_products = st.session_state.company_product_mapping.get(selected_company, [])
                available_products = get_available_products_for_company(selected_company)
                
                if available_products:
                    selected_products = st.multiselect(
                        "Select Products:",
                        available_products,
                        default=[product for product in current_products if product in available_products]
                    )
                    if st.button("Update Company Mapping"):
                        st.session_state.company_product_mapping[selected_company] = selected_products
                        save_company_mappings()
                        st.success("Updated company mapping")
                        st.rerun()
                else:
                    st.info("No available products")
            else:
                st.info("Create companies first")
            
            st.subheader("Current Mappings")
            if st.session_state.company_product_mapping:
                mapping_data = []
                for company, products in st.session_state.company_product_mapping.items():
                    mapping_data.append({
                        "Company": company,
                        "Products": ", ".join(sorted(products)) if products else "None",
                        "Count": len(products)
                    })
                st.dataframe(pd.DataFrame(mapping_data), hide_index=True)
            else:
                st.info("No mappings created")
        
        with company_tab2:
            st.subheader("Upload Company & Product Mapping File")
            company_file = st.file_uploader("Upload Company-Product File", type=['xlsx', 'xls'], key="company_file")
            
            if company_file is not None:
                file_copy = io.BytesIO(company_file.getvalue())
                sheet_names = get_sheet_names(file_copy)
                if sheet_names:
                    selected_sheet = st.selectbox("Select Sheet:", sheet_names, key="company_sheet")
                    header_row = st.number_input("Header Row:", min_value=0, value=0, key="company_header")
                    df = get_sheet_preview(file_copy, selected_sheet, header_row)
                    
                    if df is not None:
                        st.dataframe(df.head())
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            company_col = smart_column_selector("Company Group Column", df.columns, 'company', key="cp_company")
                        with col2:
                            product_col = smart_column_selector("Product Group Column", df.columns, 'product', key="cp_product")
                        
                        if st.button("Process Company-Product File"):
                            process_company_product_file(df, company_col, product_col)
                            st.success("Company-Product file processed successfully.")
                            st.rerun()

    with tab4:
        st.header("Backup & Restore")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Backup Mappings")
            st.write("Export branch, region, and company mappings:")
            st.write(f"- Branches: {len(st.session_state.branch_exec_mapping)}")
            st.write(f"- Regions: {len(st.session_state.region_branch_mapping)}")
            st.write(f"- Companies: {len(st.session_state.company_product_mapping)}")
            
            if st.button("Create Backup"):
                export_mappings()
        
        with col2:
            st.subheader("Restore Mappings")
            backup_file = st.file_uploader("Upload Backup File", type=['json'])
            if backup_file is not None:
                if st.button("Restore from Backup"):
                    success = import_mappings_from_file(backup_file)
                    if success:
                        st.rerun()

    with tab5:
        st.header("Consolidated Data View")
        if st.session_state.customer_codes:
            consolidated_data = []
            for customer_code, executive_name in st.session_state.customer_codes.items():
                executive_code = st.session_state.executive_codes.get(executive_name, "")
                branch = get_branches_for_executive(executive_name)
                customer_name = st.session_state.customer_names.get(customer_code, "")
                region = get_region_for_branch(branch) if branch and "," not in branch else ""
                
                consolidated_data.append({
                    "Customer Code": customer_code,
                    "Customer Name": customer_name,
                    "Executive": executive_name,
                    "Executive Code": executive_code,
                    "Branch": branch,
                    "Region": region
                })
            
            col1, col2, col3 = st.columns(3)
            with col1:
                filter_exec = st.multiselect("Filter by Executive:", ["All"] + sorted(st.session_state.executives), default=["All"])
            with col2:
                branches = list(st.session_state.branch_exec_mapping.keys())
                filter_branch = st.multiselect("Filter by Branch:", ["All"] + sorted(branches), default=["All"])
            with col3:
                search_customer = st.text_input("Search Customer:")
            
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
            
            if filtered_data:
                st.write(f"Results: {len(filtered_data)} records")
                consolidated_df = pd.DataFrame(filtered_data)
                csv = consolidated_df.to_csv(index=False)
                st.download_button("Download CSV", csv, "consolidated_data.csv", "text/csv")
                st.dataframe(consolidated_df, hide_index=True, use_container_width=True)
            else:
                st.warning("No records match filters")
        else:
            st.warning("No customer mappings available")

    with tab6:
        st.header("File Processing")
        process_tab1, process_tab2, process_tab3 = st.tabs(["Budget Processing", "Sales Processing", "OS Processing"])
        
        with process_tab1:
            st.subheader("Process Budget File")
            budget_file = st.file_uploader("Upload Budget File", type=['xlsx', 'xls'], key="budget_file")
            
            if budget_file is not None:
                file_copy = io.BytesIO(budget_file.getvalue())
                sheet_names = get_sheet_names(file_copy)
                if sheet_names:
                    # Set "consolidate" as default sheet
                    default_index = next((i for i, sheet in enumerate(sheet_names) if sheet.lower().strip() == "consolidate"), 0)
                    selected_sheet = st.selectbox("Select Sheet:", sheet_names, index=default_index, key="budget_sheet")
                    header_row = st.number_input("Header Row:", min_value=0, value=1, key="budget_header")
                    df = get_sheet_preview(file_copy, selected_sheet, header_row)
                    
                    if df is not None:
                        st.dataframe(df.head())
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            customer_col = smart_column_selector("Customer Code Column", df.columns, 'customer_code', key="budget_customer")
                            exec_code_col = smart_column_selector("Executive Code Column", df.columns, 'executive_code', key="budget_exec_code")
                            exec_name_col = smart_column_selector("Executive Name Column", df.columns, 'executive_name', key="budget_exec_name")
                        with col2:
                            branch_col = smart_column_selector("Branch Column", df.columns, 'branch', key="budget_branch")
                            region_col = smart_column_selector("Region Column", df.columns, 'region', key="budget_region")
                            cust_name_col = smart_column_selector("Customer Name Column", df.columns, 'customer_name', key="budget_cust_name", include_none=True)
                        
                        if st.button("Process Budget File"):
                            processed_budget = process_budget_file(df, customer_col, exec_code_col, exec_name_col, branch_col, region_col, cust_name_col)
                            
                            total_rows = len(processed_budget)
                            updated_exec_rows = processed_budget[processed_budget[exec_name_col].notna()].shape[0]
                            updated_branch_rows = processed_budget[processed_budget['Branch'] != ''].shape[0]
                            updated_region_rows = processed_budget[processed_budget['Region'] != ''].shape[0]
                            
                            st.success("Budget file processed successfully!")
                            
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Rows", total_rows)
                            with col2:
                                st.metric("Executive Updated", updated_exec_rows)
                            with col3:
                                st.metric("Branch Mapped", updated_branch_rows)
                            with col4:
                                st.metric("Region Mapped", updated_region_rows)
                            
                            st.dataframe(processed_budget.head(10))
                            
                            budget_excel = to_excel_buffer(processed_budget)
                            st.download_button(
                                "Download Processed Budget File",
                                budget_excel,
                                "processed_budget.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
        
        with process_tab2:
            st.subheader("Process Sales File")
            sales_file = st.file_uploader("Upload Sales File", type=['xlsx', 'xls'], key="sales_file")
            
            if sales_file is not None:
                file_copy = io.BytesIO(sales_file.getvalue())
                sheet_names = get_sheet_names(file_copy)
                if sheet_names:
                    selected_sheet = st.selectbox("Select Sheet:", sheet_names, key="sales_sheet")
                    header_row = st.number_input("Header Row:", min_value=0, value=1, key="sales_header")
                    df = get_sheet_preview(file_copy, selected_sheet, header_row)
                    
                    if df is not None:
                        st.dataframe(df.head())
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            sales_exec_code_col = smart_column_selector("Executive Code Column", df.columns, 'executive_code', key="sales_exec_code")
                            sales_exec_name_col = smart_column_selector("Executive Name Column", df.columns, 'executive_name', key="sales_exec_name", include_none=True)
                            sales_product_col = smart_column_selector("Product Column", df.columns, 'product', key="sales_product", include_none=True)
                        with col2:
                            unit_col = smart_column_selector("Unit Column", df.columns, 'unit', key="sales_unit", include_none=True)
                            quantity_col = smart_column_selector("Quantity Column", df.columns, 'quantity', key="sales_quantity", include_none=True)
                            value_col = smart_column_selector("Value Column", df.columns, 'value', key="sales_value", include_none=True)
                        
                        if st.button("Process Sales File"):
                            processed_sales = process_sales_file(
                                df, sales_exec_code_col, sales_product_col, sales_exec_name_col, 
                                unit_col, quantity_col, value_col
                            )
                            
                            st.success("Sales file processed successfully!")
                            st.dataframe(processed_sales.head(10))
                            
                            sales_excel = to_excel_buffer(processed_sales)
                            st.download_button(
                                "Download Processed Sales File",
                                sales_excel,
                                "processed_sales.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
        
        with process_tab3:
            st.subheader("Process OS File")
            os_file = st.file_uploader("Upload OS File", type=['xlsx', 'xls'], key="os_file")
            
            if os_file is not None:
                file_copy = io.BytesIO(os_file.getvalue())
                sheet_names = get_sheet_names(file_copy)
                if sheet_names:
                    selected_sheet = st.selectbox("Select Sheet:", sheet_names, key="os_sheet")
                    header_row = st.number_input("Header Row:", min_value=0, value=1, key="os_header")
                    df = get_sheet_preview(file_copy, selected_sheet, header_row)
                    
                    if df is not None:
                        st.dataframe(df.head())
                        
                        os_exec_code_col = smart_column_selector("Executive Code Column", df.columns, 'executive_code', key="os_exec_code")
                        
                        if st.button("Process OS File"):
                            processed_os = process_os_file(df, os_exec_code_col)
                            
                            st.success("OS file processed successfully!")
                            st.dataframe(processed_os.head(10))
                            
                            os_excel = to_excel_buffer(processed_os)
                            st.download_button(
                                "Download Processed OS File",
                                os_excel,
                                "processed_os.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
    
    with st.sidebar:
        st.header("Global Operations")
        if st.button("Save All Mappings"):
            save_all_mappings()
            st.success("All mappings saved!")
        
        st.markdown("---")
        st.subheader("System Statistics")
        st.metric("Executives", len(st.session_state.executives))
        st.metric("Branches", len(st.session_state.branch_exec_mapping))
        st.metric("Regions", len(st.session_state.region_branch_mapping))
        st.metric("Companies", len(st.session_state.company_product_mapping))
        st.metric("Products", len(st.session_state.product_groups))
        st.metric("Customer Mappings", len(st.session_state.customer_codes))
        st.metric("Unmapped Customers", len(st.session_state.unmapped_customers))
        
        st.markdown("---")
        st.subheader("⚠️ Danger Zone")
        if st.button("🗑️ Reset All Data", type="secondary"):
            if st.button("⚠️ Confirm Reset All", type="secondary"):
                reset_all_mappings()
                st.success("All data reset!")
                st.rerun()

if __name__ == "__main__":
    main()
