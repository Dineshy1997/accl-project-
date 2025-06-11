import streamlit as st
import pandas as pd
from io import BytesIO

# Set up Streamlit page
st.set_page_config(page_title="Monthly Sales Merger", layout="wide")
st.title("📊 Monthly Sales Merger Tool")

st.markdown("### Upload your monthly sales files (April to March)")
st.markdown("Each file should be in `.xlsx` format. All column names will be automatically aligned.")

# Define month names in financial year order (April to March)
months = [
    "April", "May", "June", "July",
    "August", "September", "October", "November",
    "December", "January", "February", "March"
]

# Store uploaded files
uploaded_files = {}

# Display uploaders in 4-column layout (3 rows)
rows = [months[i:i + 4] for i in range(0, len(months), 4)]
for row in rows:
    cols = st.columns(4)
    for idx, month in enumerate(row):
        with cols[idx]:
            uploaded_files[month] = st.file_uploader(f"{month}", type=["xlsx"], key=month)

# Function to read individual Excel file
def read_excel_file(file, month):
    try:
        df = pd.read_excel(file)
        df.columns = df.columns.str.strip()  # Clean column names
        df['Month'] = month  # Add month column
        return df
    except Exception as e:
        st.warning(f"❌ Failed to read {month} file: {e}")
        return None

# Convert button and processing
if st.button("🔄 Convert to Cumulative Excel"):
    all_dfs = []
    for month, file in uploaded_files.items():
        if file:
            df = read_excel_file(file, month)
            if df is not None:
                df['Source File'] = file.name  # Track file origin
                all_dfs.append(df)

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True, sort=False)
        st.success(f"✅ Successfully combined {len(all_dfs)} monthly files.")

        # Show preview
        st.markdown("### 👀 Preview of Combined Data")
        st.dataframe(combined_df.head())

        # Downloadable Excel file
        def to_excel(df):
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name="Cumulative Sales")
            return output.getvalue()

        st.download_button(
            label="📥 Download Cumulative Excel File",
            data=to_excel(combined_df),
            file_name="Cumulative_Sales_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("⚠️ Please upload at least one file before clicking convert.")
