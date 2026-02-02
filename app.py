# -*- coding: utf-8 -*-

# Version 2.2 - Final Polish with English UI and Memory Protection
import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
import gc  # Garbage Collector for RAM management
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Data Sift")

# --- CONSTANTS AND DATA ---
GDPR_TERMS = """
This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, national ID numbers, health information, etc.), the processing of which is regulated by data protection laws like the General Data Protection Regulation (GDPR or LGPD).

It is your sole responsibility to ensure that all data used in this tool complies with applicable data protection regulations. We strongly recommend that you only use previously anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been properly handled and anonymized.
"""
MANUAL_CONTENT = {
    "Introduction": """**Welcome to Data Sift!**

This program is a spreadsheet filter tool designed to optimize your work with large volumes of data by offering two main functionalities:

1.  **Filtering:** To clean your database by removing rows that are not of interest.
2.  **Stratification:** To divide your database into specific subgroups.

Navigate through the topics in the menu above to learn how to use each part of the tool.""",
    "1. Global Settings": """**1. Global Settings**

This section contains the essential settings that are shared between both tools.

- **Select Spreadsheet:** Opens a window to select the source data file (.xlsx, .xls, .csv).
- **Age Column / Sex/Gender:** Fields to select the column name in your spreadsheet.
- **Output Format:** Select the format for generated files (.csv or .xlsx).""",
    "2. Filter Tool": """**2. Filter Tool**

The purpose of this tool is to **"clean"** your spreadsheet by **removing** rows that match specific criteria.""",
    "3. Stratification Tool": """**3. Stratification Tool**

Splits your spreadsheet into **multiple smaller files** (strata) based on age ranges and gender."""
}

DEFAULT_FILTERS = [
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '<', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
]

# --- PROCESSING CLASSES ---

@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '==', 'Not equal to': '!=', '≥': '>=', '≤': '<='}

    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series): return series
        return pd.to_numeric(series.astype(str).str.replace(',', '.', regex=False), errors='coerce')

    def _build_single_mask(self, series: pd.Series, op: str, val: Any) -> pd.Series:
        if isinstance(val, str):
            val_lower_strip = val.lower().strip()
            series_lower_strip = series.astype(str).str.strip().str.lower()
            if op == '==': return series_lower_strip == val_lower_strip
            elif op == '!=': return series_lower_strip != val_lower_strip
        return eval(f"series {op} val")

    def _create_main_mask(self, df: pd.DataFrame, f: Dict, col: str) -> pd.Series:
        op1_ui, val1 = f.get('p_op1'), f.get('p_val1')
        op1 = self.OPERATOR_MAP.get(op1_ui, op1_ui)
        if val1 and val1.lower() == 'empty':
            if op1 == '==': return df[col].isna() | (df[col].astype(str).str.strip() == '')
            if op1 == '!=': return df[col].notna() & (df[col].astype(str).str.strip() != '')
            return pd.Series([False] * len(df), index=df.index)
        try:
            v1_num = float(str(val1).replace(',', '.'))
            return self._build_single_mask(df[col], op1, v1_num)
        except: return pd.Series([False] * len(df), index=df.index)

    def apply_filters(self, df: pd.DataFrame, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
        df_processed = df.copy()
        active_filters = [f for f in filters_config if f['p_check']]
        for i, f_config in enumerate(active_filters):
            progress_bar.progress((i + 1) / len(active_filters), text=f"Processing filters...")
            col = f_config.get('p_col', '')
            if col in df_processed.columns:
                mask = self._create_main_mask(df_processed, f_config, col)
                df_processed = df_processed[~mask]
        return df_processed

# --- HELPER FUNCTIONS ---

@st.cache_data(max_entries=1, show_spinner="Loading spreadsheet...")
def load_dataframe(uploaded_file):
    if uploaded_file is None: return None
    try:
        uploaded_file.seek(0)
        if uploaded_file.name.endswith('.csv'):
            try: return pd.read_csv(uploaded_file, sep=';', decimal=',', encoding='latin-1', low_memory=True)
            except: 
                uploaded_file.seek(0)
                return pd.read_csv(uploaded_file, sep=',', decimal='.', encoding='utf-8', low_memory=True)
        return pd.read_excel(uploaded_file, engine='openpyxl')
    except Exception as e:
        st.error(f"Error: {e}")
        return None

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

# --- INTERFACE FUNCTIONS ---

def handle_select_all():
    new_state = st.session_state.get('select_all_master_checkbox', False)
    for rule in st.session_state.filter_rules: rule['p_check'] = new_state

def draw_filter_rules(sex_column_values, column_options):
    st.markdown("""<style>.stButton>button { padding: 0.25rem 0.3rem; font-size: 0.8rem; }</style>""", unsafe_allow_html=True)
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium")
    all_checked = all(rule.get('p_check', True) for rule in st.session_state.filter_rules) if st.session_state.filter_rules else False
    header_cols[0].checkbox("All", value=all_checked, key='select_all_master_checkbox', on_change=handle_select_all, label_visibility="collapsed")
    header_cols[1].markdown("**Column**")
    header_cols[2].markdown("**Operator**")
    header_cols[3].markdown("**Value**")
    header_cols[7].markdown("**Actions**")
    
    ops_main = ["", ">", "<", "=", "Not equal to", "≥", "≤"]
    for i, rule in enumerate(st.session_state.filter_rules):
        cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium")
        rule['p_check'] = cols[0].checkbox(" ", value=rule.get('p_check', True), key=f"p_check_{rule['id']}", label_visibility="collapsed")
        rule['p_col'] = cols[1].selectbox("Col", options=column_options, index=column_options.index(rule['p_col']) if rule['p_col'] in column_options else None, key=f"p_col_{rule['id']}", label_visibility="collapsed", placeholder="Select column")
        rule['p_op1'] = cols[2].selectbox("Op", ops_main, index=ops_main.index(rule.get('p_op1', '=')) if rule.get('p_op1') in ops_main else 0, key=f"p_op1_{rule['id']}", label_visibility="collapsed")
        rule['p_val1'] = cols[3].text_input("Val", value=rule.get('p_val1', ''), key=f"p_val1_{rule['id']}", label_visibility="collapsed")
        if cols[7].button("X", key=f"del_{rule['id']}"):
            st.session_state.filter_rules.pop(i)
            st.rerun()

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    if not st.session_state.lgpd_accepted:
        st.title("Data Sift")
        st.markdown(GDPR_TERMS)
        if st.button("I accept the terms"):
            st.session_state.lgpd_accepted = True
            st.rerun()
        return

    if 'filter_rules' not in st.session_state: st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)

    with st.sidebar:
        st.title("User Manual")
        topic = st.selectbox("Topic", list(MANUAL_CONTENT.keys()))
        st.markdown(MANUAL_CONTENT[topic])

    st.title("Data Sift")

    # --- BLOCK REQUESTED BY USER (FIXED INDENTATION) ---
    with st.expander("1. Global Settings", expanded=True):
        uploaded_file = st.file_uploader("Select spreadsheet", type=['csv', 'xlsx', 'xls'])
        df = None
        if uploaded_file:
            df = load_dataframe(uploaded_file)
        
        if df is not None:
            column_options = df.columns.tolist()
        else:
            column_options = []

        c1, c2, c3 = st.columns(3)
        with c1: st.selectbox("Age Column", options=column_options, key="col_idade", index=None, placeholder="Select Age column")
        with c2: st.selectbox("Sex/Gender Column", options=column_options, key="col_sexo", index=None, placeholder="Select Sex column")
        with c3: st.selectbox("Output Format", ["CSV (.csv)", "Excel (.xlsx)"], key="output_format")
    # --------------------------------------------------

    tab_filter, tab_stratify = st.tabs(["2. Filter Tool", "3. Stratification Tool"])

    with tab_filter:
        st.header("Exclusion Rules")
        draw_filter_rules([], column_options)
        if st.button("Add New Filter Rule"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '=', 'p_val1': ''})
            st.rerun()
        
        if st.button("Generate Filtered Sheet", type="primary", use_container_width=True):
            if df is None: st.error("Please upload a file first.")
            else:
                with st.spinner("Processing..."):
                    progress_bar = st.progress(0)
                    processor = get_data_processor()
                    filtered_df = processor.apply_filters(df, st.session_state.filter_rules, {}, progress_bar)
                    
                    if not filtered_df.empty:
                        del df
                        gc.collect()
                        is_excel = "Excel" in st.session_state.output_format
                        file_bytes = to_excel(filtered_df) if is_excel else to_csv(filtered_df)
                        st.session_state.filtered_result = (file_bytes, f"Filtered_Data_{datetime.now().strftime('%Y%m%d')}.{'xlsx' if is_excel else 'csv'}")
                        st.success("File ready for download!")

        if 'filtered_result' in st.session_state:
            st.download_button("Download Result", data=st.session_state.filtered_result[0], file_name=st.session_state.filtered_result[1], use_container_width=True)

if __name__ == "__main__":
    main()
