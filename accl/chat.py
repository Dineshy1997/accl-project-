import streamlit as st
import pandas as pd
import google.generativeai as genai
import os
# from dotenv import load_dotenv
import numpy as np

def main():
    # Load API key from .env or directly use it here
    # load_dotenv()
    genai.configure(api_key="AIzaSyDrvURpHhrOkNKxlunjnN7pDs8tfjCLXdU")

    model = genai.GenerativeModel("gemini-1.5-flash")

    # DO NOT use st.set_page_config() here because it's already called in main_app.py
    st.title("\U0001F4CA Chat with Your Excel File")

    with st.sidebar:
        st.header("\U0001F6E0️ Options")
        show_code = st.checkbox("Always show generated code", value=False)
        max_rows_display = st.slider("Max rows to display", 5, 50, 10)

    uploaded_file = st.file_uploader("Upload your Excel file", type=["xlsx", "xls"])

    if uploaded_file:
        try:
            xls = pd.ExcelFile(uploaded_file)
            sheet_names = xls.sheet_names
            selected_sheet = st.selectbox("Select a sheet", sheet_names)

            header_option = st.radio("Header row:", ["Row 1 (0)", "Row 2 (1)", "No header"], index=0)
            header_val = None if header_option == "No header" else int(header_option.split("(")[1].split(")")[0])

            df = pd.read_excel(xls, sheet_name=selected_sheet, header=header_val)

            if st.checkbox("Remove unnamed columns", value=True):
                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

            if st.checkbox("Remove last row (if summary)", value=True):
                df = df.iloc[:-1]

            st.success(f"Uploaded: {uploaded_file.name} | Sheet: {selected_sheet} | Shape: {df.shape}")

            with st.expander("\U0001F4CB Data Preview", expanded=True):
                st.dataframe(df.head(max_rows_display), use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                st.write("**\U0001F4CA Columns:**", df.columns.tolist())
            with col2:
                st.write("**\U0001F4C8 Data Types:**")
                for col, dtype in df.dtypes.items():
                    st.write(f"• {col}: {dtype}")

            if "chat_history" not in st.session_state:
                st.session_state.chat_history = []

            if st.session_state.chat_history:
                st.subheader("\U0001F4AC Chat History")
                for i, (q, a) in enumerate(st.session_state.chat_history):
                    with st.expander(f"Question {i+1}: {q[:50]}..."):
                        st.markdown(f"**You:** {q}")
                        st.markdown(f"**Bot:** {a}")

            if st.session_state.chat_history and st.button("\U0001F5D1️ Clear Chat History"):
                st.session_state.chat_history = []
                st.rerun()

            st.subheader("❓ Ask Your Question")
            user_question = st.text_input(
                "Ask a question about your Excel data:",
                placeholder="e.g., What's the average sales? Show top 5 customers by revenue"
            )

            with st.expander("\U0001F4A1 Sample Questions", expanded=False):
                sample_questions = [
                    "What's the total sum of [column_name]?",
                    "Show me the top 5 rows by [column_name]",
                    "What's the average value in [column_name]?",
                    "How many unique values are in [column_name]?",
                    "Filter rows where [column_name] > 100",
                    "What's the correlation between [col1] and [col2]?"
                ]
                for sq in sample_questions:
                    if st.button(sq, key=f"sample_{sq}"):
                        st.session_state.temp_question = sq

            if hasattr(st.session_state, 'temp_question'):
                user_question = st.session_state.temp_question
                del st.session_state.temp_question

            if user_question:
                with st.spinner("\U0001F9E0 Analyzing your data..."):
                    system_prompt = f"""
You are a Python data analyst working with a Pandas DataFrame called `df`.

DataFrame Info:
- Columns: {df.columns.tolist()}
- Shape: {df.shape}
- Data types: {dict(df.dtypes)}

IMPORTANT RULES:
1. Write ONLY ONE line of Python code that assigns the result to `result`
2. Always check if filtered DataFrames are empty before using .iloc[]
3. Handle potential errors (use try/except logic in a single line if needed)
4. If no data found or error occurs, set result = 'Not found' or appropriate message
5. Use proper pandas methods (.sum(), .mean(), .max(), .min(), .count(), etc.)
6. For text matching, use .str.contains() with case=False for flexibility
7. Output format: result = [your_code_here]

Examples:
- result = df['column'].sum() if 'column' in df.columns else 'Column not found'
- result = df.nlargest(5, 'column') if 'column' in df.columns and not df.empty else 'No data'
- result = df[df['column'] > 100].head() if 'column' in df.columns else 'Column not found'

Question: {user_question}

Output only the Python code line starting with 'result =':
"""

                    try:
                        gemini_response = model.generate_content(system_prompt)
                        python_code = gemini_response.text.strip()

                        if "```python" in python_code:
                            python_code = python_code.split("```python")[1].split("```")[0].strip()
                        elif "```" in python_code:
                            python_code = python_code.strip("```").strip()

                        if not python_code.startswith("result ="):
                            python_code = "result = " + python_code

                        local_vars = {"df": df, "pd": pd, "np": np}
                        exec(python_code, {}, local_vars)
                        result = local_vars.get("result", "No result found.")

                        st.session_state.chat_history.append((user_question, str(result)))

                        st.success("✅ Analysis Complete!")
                        col1, col2 = st.columns([2, 1])
                        with col1:
                            st.markdown("### \U0001F916 Answer:")
                            if isinstance(result, pd.DataFrame):
                                st.dataframe(result, use_container_width=True)
                            elif isinstance(result, pd.Series):
                                st.dataframe(result.to_frame(), use_container_width=True)
                            else:
                                st.write(result)

                        with col2:
                            st.markdown("### \U0001F4CA Result Type:")
                            st.write(f"Type: `{type(result).__name__}`")
                            if hasattr(result, 'shape'):
                                st.write(f"Shape: `{result.shape}`")

                        if show_code:
                            with st.expander("\U0001F50D Generated Code", expanded=True):
                                st.code(python_code, language="python")

                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        st.markdown("### \U0001F50D Generated Code (Debug):")
                        st.code(python_code, language="python")
                        st.markdown("### \U0001F4A1 Possible Solutions:")
                        st.write("• Check if column names are spelled correctly")
                        st.write("• Ensure the column contains numeric data for mathematical operations")
                        st.write("• Try rephrasing your question more specifically")

        except Exception as e:
            st.error(f"❌ Error reading the Excel file: {e}")
            st.write("**Troubleshooting tips:**")
            st.write("• Make sure the file is a valid Excel file (.xlsx or .xls)")
            st.write("• Check if the file is not corrupted")
            st.write("• Try saving the file again in Excel")

    else:
        st.info("\U0001F446 Please upload an Excel file to get started!")
        with st.expander("ℹ️ How to use this app", expanded=True):
            st.markdown("""
            1. **Upload** your Excel file using the file uploader above
            2. **Select** the sheet you want to analyze
            3. **Adjust** data cleaning options if needed
            4. **Ask questions** in natural language about your data
            5. **View** the results and generated Python code

            **Example questions:**
            - "What's the total revenue?"
            - "Show me the top 10 customers by sales"
            - "What's the average order value?"
            - "How many unique products do we have?"
            """)

    st.markdown("---")
    st.markdown("Made with CUBE")


if __name__ == "__main__":
    main()
