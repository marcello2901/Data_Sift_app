# -*- coding: utf-8 -*-

# Versão 1.9.8 - Correção de estabilidade na seleção de colunas e memória
import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
import polars as pl
from datetime import datetime
from typing import List, Dict, Any, Optional

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Data Sift")

# --- CONSTANTES E DADOS ---
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

- **Select Spreadsheet:**
  Opens a window to select the source data file. It supports `.xlsx`, `.xls`, and `.csv` formats. Once selected, the file becomes available for both tools.

- **Age Column / Sex/Gender:**
  Fields to **select** the column name in your spreadsheet. The options in the list appear after the file is uploaded.

- **Output Format:**
  A selection menu to choose the format of the generated files. The default is `.csv`. Choose `Excel (.xlsx)` for better compatibility with Microsoft Excel or `CSV (.csv)` for a lighter, universal format.
  """,
    "2. Filter Tool": """**2. Filter Tool**

The purpose of this tool is to **"clean"** your spreadsheet by **removing** rows that match specific criteria. The result is a **single file** containing only the data that "survived" the filters.

**How Exclusion Rules Work:**
Each row you add is a condition to **remove** data. If a row in your spreadsheet matches an active rule, it **will be excluded** from the final file.

- **[✓] (Activation Checkbox):** Toggles a rule on or off without deleting it.

- **Column:** The name of the column where the filter will be applied. **Tip:** You can apply the rule to multiple columns at once by separating their names with a semicolon (;). When doing so, a row will be excluded only if **all** specified columns meet the condition.

- **Operator and Value:** Operators ">", "<", "≥", "≤", "=", "Not equal to" define the rule's logic. They are used to define the ranges that will be considered for data **exclusion**.
**Tip:** The keyword `empty` is a powerful feature:
    - **Scenario 1: Exclude rows with MISSING data.**
        - **Configuration:** Column: `"Exam_X"`, Operator: `"is equal to"`, Value: `"empty"`.
    - **Scenario 2: Keep only rows with EXISTING data.**
        - **Configuration:** Column: `"Observations"`, Operator: `"Not equal to"`, Value: `"empty"`.

- **Compound Logic:** Expands the rule to create `AND` / `OR` conditions for when the user wants to set exclusion ranges.

- **Condition:** Allows applying a secondary filter. The main rule will only be applied to rows that also meet the specified sex and/or age conditions.

- **Actions:** The `X` button deletes the rule. The 'Clone' button duplicates it.

- **Generate Filtered Sheet:** Starts the process. A download button will appear at the end with the `Filtered_Sheet_` file with a timestamp.""",
    "3. Stratification Tool": """**3. Stratification Tool**

Unlike the filter, the purpose of this tool is to **split** your spreadsheet into **multiple smaller files**, where each file represents a subgroup of interest (a "stratum").

**How Stratification Works:**


- **Stratification Options by Sex/Gender:**
  - After loading a spreadsheet and selecting the "Sex/Gender" column in the Global Settings, this area will display a checkbox for each unique value found (e.g., Male, Female, etc.). Check the ones you want to include in the stratification.

- **Age Range Definitions:**
  - This area is used **exclusively** to create age-based strata.

- **Generate Stratified Sheets:**
  - Starts the splitting process. The number of generated files will be (`number of age ranges` x `number of selected genders`).

  - **Confirmation:** Before starting, the program will ask if you are using an already filtered spreadsheet."""
}

DEFAULT_FILTERS = [
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '50', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '600', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ultra-PCR.ULTRAPCR', 'p_op1': '>', 'p_val1': '5', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
]

CHUNK_SIZE = 50000

# --- CLASSES DE PROCESSAMENTO ---

@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '==', 'Não é igual a': '!=', '≥': '>=', '≤': '<=', 'is equal to': '==', 'Not equal to': '!='}

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
            if f.get('p_expand'):
                v1_num = float(str(val1).replace(',', '.'))
                op_central_ui, op2_ui, val2 = f.get('p_op_central'), f.get('p_op2'), f.get('p_val2')
                op2 = self.OPERATOR_MAP.get(op2_ui, op2_ui)
                v2_num = float(str(val2).replace(',', '.'))

                if op_central_ui.upper() == 'BETWEEN':
                    min_val, max_val = sorted((v1_num, v2_num))
                    return df[col].between(min_val, max_val, inclusive='both')
                m1 = self._build_single_mask(df[col], op1, v1_num)
                m2 = self._build_single_mask(df[col], op2, v2_num)
                if op_central_ui.upper() == 'AND': return m1 & m2
                if op_central_ui.upper() == 'OR': return m1 | m2
            else:
                v1_num = float(str(val1).replace(',', '.'))
                return self._build_single_mask(df[col], op1, v1_num)
        except (ValueError, TypeError, Exception):
            return pd.Series([False] * len(df), index=df.index)

    def _create_conditional_mask(self, df: pd.DataFrame, f: Dict, global_config: Dict) -> pd.Series:
        mascara_condicional = pd.Series(True, index=df.index)
        if not f.get('c_check'): return mascara_condicional

        col_idade = global_config.get('coluna_idade')
        if f.get('c_idade_check') and col_idade and col_idade in df.columns:
            df[col_idade] = self._safe_to_numeric(df[col_idade])
            try:
                op_idade1_ui, val_idade1 = f.get('c_idade_op1'), f.get('c_idade_val1')
                if op_idade1_ui and val_idade1:
                    op1 = self.OPERATOR_MAP.get(op_idade1_ui, op_idade1_ui)
                    v1 = float(str(val_idade1).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op1, v1)
                
                op_idade2_ui, val_idade2 = f.get('c_idade_op2'), f.get('c_idade_val2')
                if op_idade2_ui and val_idade2:
                    op2 = self.OPERATOR_MAP.get(op_idade2_ui, op_idade2_ui)
                    v2 = float(str(val_idade2).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op2, v2)
            except (ValueError, TypeError):
                pass

        col_sexo = global_config.get('coluna_sexo')
        if f.get('c_sexo_check') and col_sexo and col_sexo in df.columns:
            val_sexo_gui = f.get('c_sexo_val', '').lower().strip()
            if val_sexo_gui:
                mascara_condicional &= self._build_single_mask(df[col_sexo], '==', val_sexo_gui)
        return mascara_condicional

    def apply_filters(self, uploaded_file, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
        active_filters = [f for f in filters_config if f['p_check']]
        
        uploaded_file.seek(0)
        enc = st.session_state.get('detected_encoding', 'latin-1')
        sep = st.session_state.get('detected_separator', ';')

        if not active_filters:
            progress_bar.progress(1.0, text="No active filters.")
            uploaded_file.seek(0)
            if uploaded_file.name.endswith('.csv'):
                return pd.read_csv(uploaded_file, sep=sep, engine='python', encoding=enc, encoding_errors='replace', on_bad_lines='skip')
            else:
                return pd.read_excel(uploaded_file)

        processed_chunks = []
        uploaded_file.seek(0)
        
        if uploaded_file.name.endswith('.csv'):
            reader = pd.read_csv(uploaded_file, chunksize=CHUNK_SIZE, sep=sep, engine='python', decimal=',', encoding=enc, encoding_errors='replace', on_bad_lines='skip')
        else:
            full_df = pd.read_excel(uploaded_file)
            reader = [full_df[i:i + CHUNK_SIZE] for i in range(0, len(full_df), CHUNK_SIZE)]

        for idx, chunk in enumerate(reader):
            temp_chunk = chunk.copy()
            progress_bar.progress(0.1, text=f"Processing block {idx+1}...")

            for f_config in active_filters:
                col_config_str = f_config.get('p_col', '')
                cols_to_check = [c.strip() for c in col_config_str.split(';') if c.strip()]
                is_numeric_filter = f_config.get('p_val1', '').lower() != 'empty'
                
                combined_mask = pd.Series(True, index=temp_chunk.index)
                if not cols_to_check:
                    combined_mask = pd.Series(False, index=temp_chunk.index)
                else:
                    for sub_col in cols_to_check:
                        if sub_col in temp_chunk.columns:
                            if is_numeric_filter:
                                temp_chunk[sub_col] = self._safe_to_numeric(temp_chunk[sub_col])
                            combined_mask &= self._create_main_mask(temp_chunk, f_config, sub_col)
                        else:
                            combined_mask = pd.Series(False, index=temp_chunk.index)
                            break
                
                conditional_mask = self._create_conditional_mask(temp_chunk, f_config, global_config)
                final_mask_to_exclude = combined_mask & conditional_mask
                temp_chunk = temp_chunk[~final_mask_to_exclude]

            processed_chunks.append(temp_chunk)

        progress_bar.progress(1.0, text="Filtering complete!")
        return pd.concat(processed_chunks, ignore_index=True) if processed_chunks else pd.DataFrame()

    def apply_stratification(self, uploaded_file, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        uploaded_file.seek(0)
        enc = st.session_state.get('detected_encoding', 'latin-1')
        sep = st.session_state.get('detected_separator', ';')
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file, sep=sep, engine='python', encoding=enc, encoding_errors='replace', on_bad_lines='skip')
            else:
                df = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"Error loading file for stratification: {e}")
            return {}
        
        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')

        if not (col_idade and col_idade in df.columns): return {}
        if not (col_sexo and col_sexo in df.columns): return {}

        df[col_idade] = self._safe_to_numeric(df[col_idade])
        age_strata = strata_config.get('ages', [])
        sex_strata = strata_config.get('sexes', [])

        final_strata_to_process = []
        if not age_strata and sex_strata:
            for sex_rule in sex_strata: final_strata_to_process.append({'age': None, 'sex': sex_rule})
        elif age_strata and not sex_strata:
            for age_rule in age_strata: final_strata_to_process.append({'age': age_rule, 'sex': None})
        else:
            for sex_rule in sex_strata:
                for age_rule in age_strata:
                    final_strata_to_process.append({'age': age_rule, 'sex': sex_rule})

        total_files = len(final_strata_to_process)
        generated_dfs = {}

        for i, stratum in enumerate(final_strata_to_process):
            progress = (i + 1) / total_files
            combined_mask = pd.Series(True, index=df.index)
            age_rule = stratum.get('age')
            sex_rule = stratum.get('sex')

            if age_rule:
                age_mask = pd.Series(True, index=df.index)
                try:
                    if age_rule.get('op1') and age_rule.get('val1'):
                        op1 = self.OPERATOR_MAP.get(age_rule['op1'], age_rule['op1'])
                        val1 = float(str(age_rule['val1']).replace(',', '.'))
                        age_mask &= eval(f"df['{col_idade}'] {op1} {val1}")
                    if age_rule.get('op2') and age_rule.get('val2'):
                        op2 = self.OPERATOR_MAP.get(age_rule['op2'], age_rule['op2'])
                        val2 = float(str(age_rule['val2']).replace(',', '.'))
                        age_mask &= eval(f"df['{col_idade}'] {op2} {val2}")
                    combined_mask &= age_mask
                except Exception: continue

            if sex_rule:
                sex_val = sex_rule.get('value')
                if sex_val:
                    combined_mask &= self._build_single_mask(df[col_sexo], '==', sex_val)
            
            stratum_df = df[combined_mask]
            filename = self._generate_stratum_name(age_rule, sex_rule)
            progress_bar.progress(progress, text=f"Generating: {filename}...")
            if not stratum_df.empty:
                generated_dfs[filename] = stratum_df
        
        progress_bar.progress(1.0, text="Stratification complete!")
        return generated_dfs

    def _generate_stratum_name(self, age_rule: Optional[Dict], sex_rule: Optional[Dict]) -> str:
        name_parts = []
        if age_rule:
            op1, val1 = age_rule.get('op1'), age_rule.get('val1')
            op2, val2 = age_rule.get('op2'), age_rule.get('val2')
            def get_int(val): 
                try: return int(float(str(val).replace(',', '.')))
                except (ValueError, TypeError): return None
            v1_int = get_int(val1)
            v2_int = get_int(val2)
            if op1 and val1 and not (op2 and val2):
                if v1_int is not None:
                    if op1 == '>': name_parts.append(f"Over_{v1_int}_years")
                    elif op1 == '≥': name_parts.append(f"{v1_int}_and_over_years")
                    elif op1 == '<': name_parts.append(f"Under_{v1_int}_years")
                    elif op1 == '≤': name_parts.append(f"Up_to_{v1_int}_years")
            elif op1 and val1 and op2 and val2:
                if v1_int is not None and v2_int is not None:
                    v1_f, v2_f = float(str(val1).replace(',', '.')), float(str(val2).replace(',', '.'))
                    bounds = sorted([(v1_f, op1), (v2_f, op2)], key=lambda x: x[0])
                    low_val_f, low_op = bounds[0]
                    high_val_f, high_op = bounds[1]
                    low_bound = int(low_val_f) if low_op == '≥' else int(low_val_f + 1) if low_op == '>' else int(low_val_f)
                    high_bound = int(high_val_f) if high_op == '≤' else int(high_val_f - 1) if high_op == '<' else int(high_val_f)
                    name_parts.append(f"{low_bound}_to_{high_bound}_years")
        if sex_rule:
            sex_name = str(sex_rule.get('value', '')).replace(' ', '_')
            if sex_name: name_parts.append(sex_name)
        return "_".join(part for part in name_parts if part)

# --- FUNÇÕES AUXILIARES ---

@st.cache_data(show_spinner=False)
def detect_and_get_columns(file_content, file_name):
    """Lê apenas o cabeçalho para detectar colunas sem sobrecarregar memória."""
    if file_content is None: return []
    try:
        # Criar um buffer a partir do conteúdo para não resetar o file_uploader original
        buffer = io.BytesIO(file_content)
        if file_name.endswith('.csv'):
            encodings_to_try = ['utf-8-sig', 'latin-1', 'utf-8', 'cp1252']
            separators_to_try = [';', ',', '\t']
            for enc in encodings_to_try:
                for sep in separators_to_try:
                    try:
                        buffer.seek(0)
                        df_test = pd.read_csv(buffer, nrows=2, sep=sep, encoding=enc, encoding_errors='replace')
                        if len(df_test.columns) > 1:
                            st.session_state.detected_encoding = enc
                            st.session_state.detected_separator = sep
                            return df_test.columns.tolist()
                    except Exception: continue
            buffer.seek(0)
            return pl.read_csv(buffer, n_rows=1).columns
        else:
            df_header = pd.read_excel(buffer, nrows=0)
            return df_header.columns.tolist()
    except Exception as e:
        st.error(f"Critical error during column detection: {e}")
        return []

@st.cache_data(show_spinner="Fetching unique values...")
def get_unique_values_cached(file_content, file_name, column_name, enc, sep):
    if not file_content or not column_name: return []
    try:
        buffer = io.BytesIO(file_content)
        if file_name.endswith('.csv'):
            # Lê apenas a coluna necessária
            df_col = pd.read_csv(buffer, usecols=[column_name], sep=sep, engine='python', encoding=enc, encoding_errors='replace')
        else:
            df_col = pd.read_excel(buffer, usecols=[column_name])
        return [""] + [str(x) for x in df_col[column_name].dropna().unique()]
    except Exception:
        return []

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

# --- FUNÇÕES DE INTERFACE ---

def handle_select_all():
    new_state = st.session_state.get('select_all_master_checkbox', False)
    for rule in st.session_state.filter_rules:
        rule['p_check'] = new_state

def draw_filter_rules(sex_column_values, column_options): 
    st.markdown("""<style>
        .stButton>button { padding: 0.25rem 0.3rem; font-size: 0.8rem; }
    </style>""", unsafe_allow_html=True)
    
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="small")
    all_checked = all(rule.get('p_check', True) for rule in st.session_state.filter_rules) if st.session_state.filter_rules else False

    header_cols[0].checkbox("All", value=all_checked, key='select_all_master_checkbox', on_change=handle_select_all, label_visibility="collapsed")
    header_cols[1].markdown("**Column**")
    header_cols[2].markdown("**Operator**")
    header_cols[3].markdown("**Value**")
    header_cols[5].markdown("**Compound Logic**")
    header_cols[6].markdown("**Condition**")
    header_cols[7].markdown("**Actions**")
    st.markdown("---")

    ops_main = ["", ">", "<", "=", "Not equal to", "≥", "≤"]
    ops_age = ["", ">", "<", "≥", "≤", "="]
    ops_central_logic = ["AND", "OR", "BETWEEN"]

    for i, rule in enumerate(st.session_state.filter_rules):
        rule_id = rule['id']
        with st.container():
            cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="small") 
            rule['p_check'] = cols[0].checkbox(" ", value=rule.get('p_check', True), key=f"p_check_{rule_id}", label_visibility="collapsed")
            rule['p_col'] = cols[1].selectbox("Col", options=column_options, index=column_options.index(rule['p_col']) if rule['p_col'] in column_options else None, key=f"p_col_{rule_id}", label_visibility="collapsed")
            rule['p_op1'] = cols[2].selectbox("Op1", ops_main, index=ops_main.index(rule.get('p_op1', '=')) if rule.get('p_op1') in ops_main else 0, key=f"p_op1_{rule_id}", label_visibility="collapsed")
            rule['p_val1'] = cols[3].text_input("V1", value=rule.get('p_val1', ''), key=f"p_val1_{rule_id}", label_visibility="collapsed")
            rule['p_expand'] = cols[4].checkbox("+", value=rule.get('p_expand', False), key=f"p_expand_{rule_id}", label_visibility="collapsed")
            
            with cols[5]:
                if rule['p_expand']:
                    exp_cols = st.columns([3, 2, 2])
                    rule['p_op_central'] = exp_cols[0].selectbox("Log", ops_central_logic, index=ops_central_logic.index(rule.get('p_op_central', 'OR')) if rule.get('p_op_central') in ops_central_logic else 0, key=f"p_op_central_{rule_id}", label_visibility="collapsed")
                    rule['p_op2'] = exp_cols[1].selectbox("Op2", ops_main, index=ops_main.index(rule.get('p_op2', '>')) if rule.get('p_op2') in ops_main else 0, key=f"p_op2_{rule_id}", label_visibility="collapsed")
                    rule['p_val2'] = exp_cols[2].text_input("V2", value=rule.get('p_val2', ''), key=f"p_val2_{rule_id}", label_visibility="collapsed")

            rule['c_check'] = cols[6].checkbox("Cond", value=rule.get('c_check', False), key=f"c_check_{rule_id}")
            
            act_cols = cols[7].columns(2)
            if act_cols[0].button("Clone", key=f"clone_{rule_id}"):
                new_r = copy.deepcopy(rule); new_r['id'] = str(uuid.uuid4())
                st.session_state.filter_rules.insert(i + 1, new_r); st.rerun()
            if act_cols[1].button("X", key=f"del_{rule_id}"):
                st.session_state.filter_rules.pop(i); st.rerun()

            if rule['c_check']:
                cond_cols = st.columns([0.55, 0.5, 1, 3, 1, 3])
                rule['c_idade_check'] = cond_cols[2].checkbox("Age", value=rule.get('c_idade_check', False), key=f"c_idade_check_{rule_id}")
                if rule['c_idade_check']:
                    age_c = cond_cols[3].columns([2, 2, 1, 2, 2])
                    rule['c_idade_op1'] = age_c[0].selectbox("AOp1", ops_age, index=ops_age.index(rule.get('c_idade_op1','>')) if rule.get('c_idade_op1') in ops_age else 0, key=f"c_idade_op1_{rule_id}", label_visibility="collapsed")
                    rule['c_idade_val1'] = age_c[1].text_input("AV1", value=rule.get('c_idade_val1',''), key=f"c_idade_val1_{rule_id}", label_visibility="collapsed")
                    age_c[2].write("AND")
                    rule['c_idade_op2'] = age_c[3].selectbox("AOp2", ops_age, index=ops_age.index(rule.get('c_idade_op2','<')) if rule.get('c_idade_op2') in ops_age else 0, key=f"c_idade_op2_{rule_id}", label_visibility="collapsed")
                    rule['c_idade_val2'] = age_c[4].text_input("AV2", value=rule.get('c_idade_val2',''), key=f"c_idade_val2_{rule_id}", label_visibility="collapsed")
                
                rule['c_sexo_check'] = cond_cols[4].checkbox("Sex", value=rule.get('c_sexo_check', False), key=f"c_sexo_check_{rule_id}")
                if rule['c_sexo_check']:
                    rule['c_sexo_val'] = cond_cols[5].selectbox("SVal", options=sex_column_values, index=sex_column_values.index(rule['c_sexo_val']) if rule['c_sexo_val'] in sex_column_values else 0, key=f"c_sexo_val_{rule_id}", label_visibility="collapsed")
        st.markdown("---")

def draw_stratum_rules():
    ops_stratum = ["", ">", "<", "≥", "≤"]
    for i, stratum_rule in enumerate(st.session_state.stratum_rules):
        rule_id = stratum_rule['id']
        cols = st.columns([2, 1, 1, 0.5, 1, 1, 1])
        cols[0].write(f"**Age Range {i+1}:**")
        stratum_rule['op1'] = cols[1].selectbox("SOp1", ops_stratum, index=ops_stratum.index(stratum_rule.get('op1', '')) if stratum_rule.get('op1') in ops_stratum else 0, key=f"s_op1_{rule_id}", label_visibility="collapsed")
        stratum_rule['val1'] = cols[2].text_input("SV1", value=stratum_rule.get('val1', ''), key=f"s_val1_{rule_id}", label_visibility="collapsed")
        cols[3].write("AND")
        stratum_rule['op2'] = cols[4].selectbox("SOp2", ops_stratum, index=ops_stratum.index(stratum_rule.get('op2', '')) if stratum_rule.get('op2') in ops_stratum else 0, key=f"s_op2_{rule_id}", label_visibility="collapsed")
        stratum_rule['val2'] = cols[5].text_input("SV2", value=stratum_rule.get('val2', ''), key=f"s_val2_{rule_id}", label_visibility="collapsed")
        if cols[6].button("X", key=f"del_s_{rule_id}"):
            if len(st.session_state.stratum_rules) > 1: st.session_state.stratum_rules.pop(i); st.rerun()
        st.markdown("---")

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    if not st.session_state.lgpd_accepted:
        st.title("Welcome to Data Sift!")
        st.markdown(GDPR_TERMS)
        if st.checkbox("Confirm anonymized data"):
            if st.button("Continue"): st.session_state.lgpd_accepted = True; st.rerun()
        return

    if 'filter_rules' not in st.session_state: st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)
    if 'stratum_rules' not in st.session_state: st.session_state.stratum_rules = [{'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''}]
    if 'column_options' not in st.session_state: st.session_state.column_options = []
    
    with st.sidebar:
        topic = st.selectbox("Manual", list(MANUAL_CONTENT.keys()))
        st.markdown(MANUAL_CONTENT[topic], unsafe_allow_html=True)

    st.title("Data Sift")

    with st.expander("1. Global Settings", expanded=True):
        uploaded_file = st.file_uploader("Select spreadsheet", type=['csv', 'xlsx', 'xls'])
        
        # Só processamos as colunas se um arquivo novo foi carregado
        if uploaded_file:
            # Pegamos o conteúdo do arquivo uma vez para cache
            file_bytes = uploaded_file.getvalue()
            if not st.session_state.column_options:
                st.session_state.column_options = detect_and_get_columns(file_bytes, uploaded_file.name)
            
            if st.button("Manual Refresh Columns"):
                st.session_state.column_options = detect_and_get_columns(file_bytes, uploaded_file.name)
                st.rerun()
        else:
            st.session_state.column_options = []

        column_options = st.session_state.column_options
        
        c1, c2, c3 = st.columns(3)
        col_idade = c1.selectbox("Age Column", options=column_options, key="col_idade", index=None)
        col_sexo = c2.selectbox("Sex/Gender Column", options=column_options, key="col_sexo", index=None)
        c3.selectbox("Output Format", ["CSV (.csv)", "Excel (.xlsx)"], key="output_format")

        sex_column_values = []
        if uploaded_file and col_sexo and column_options:
            enc = st.session_state.get('detected_encoding', 'latin-1')
            sep = st.session_state.get('detected_separator', ';')
            # Usando a versão com cache para evitar ler o arquivo a cada interação
            sex_column_values = get_unique_values_cached(uploaded_file.getvalue(), uploaded_file.name, col_sexo, enc, sep)

    tab_filter, tab_stratify = st.tabs(["2. Filter Tool", "3. Stratification Tool"])

    with tab_filter:
        draw_filter_rules(sex_column_values, column_options)
        if st.button("Add New Filter Rule"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '=', 'p_val1': '', 'p_expand': False})
            st.rerun()
        
        filter_btn_placeholder = st.empty()
        if filter_btn_placeholder.button("Generate Filtered Sheet", type="primary", use_container_width=True, disabled=not uploaded_file):
            with st.spinner("Processing Large Dataset..."):
                prog = st.progress(0)
                processor = get_data_processor()
                filtered_df = processor.apply_filters(uploaded_file, st.session_state.filter_rules, {"coluna_idade": col_idade, "coluna_sexo": col_sexo}, prog)
                
                if not filtered_df.empty:
                    is_ex = "Excel" in st.session_state.output_format
                    st.session_state.filtered_result = (to_excel(filtered_df) if is_ex else to_csv(filtered_df), f"Filtered_{datetime.now().strftime('%H%M%S')}.{'xlsx' if is_ex else 'csv'}")
                    st.success(f"Generated {len(filtered_df)} rows.")

        if 'filtered_result' in st.session_state:
            st.download_button("Download Result", data=st.session_state.filtered_result[0], file_name=st.session_state.filtered_result[1], use_container_width=True, key="main_download")

    with tab_stratify:
        if sex_column_values:
            if 'strat_gender_selection' not in st.session_state:
                st.session_state.strat_gender_selection = {v: True for v in sex_column_values if v}
            
            valid_genders = [v for v in sex_column_values if v]
            if valid_genders:
                cols = st.columns(5)
                for i, g in enumerate(valid_genders):
                    st.session_state.strat_gender_selection[g] = cols[i % 5].checkbox(str(g), value=st.session_state.strat_gender_selection.get(g, True), key=f"strat_sex_{g}")

        draw_stratum_rules()
        if st.button("Add Age Range"):
            st.session_state.stratum_rules.append({'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''}); st.rerun()
        
        if st.button("Generate Stratified Sheets", type="primary", use_container_width=True, disabled=not uploaded_file):
            st.session_state.confirm_stratify = True
            st.rerun()

        if st.session_state.get('confirm_stratify'):
            st.warning("Confirm stratification?")
            c1, c2 = st.columns(2)
            if c1.button("Confirm"):
                with st.spinner("Processing..."):
                    prog = st.progress(0)
                    processor = get_data_processor()
                    age_rules = [r for r in st.session_state.stratum_rules if r.get('val1')]
                    sex_rules = [{'value': g} for g, sel in st.session_state.strat_gender_selection.items() if sel]
                    res = processor.apply_stratification(uploaded_file, {'ages': age_rules, 'sexes': sex_rules}, {"coluna_idade": col_idade, "coluna_sexo": col_sexo}, prog)
                    st.session_state.stratified_results = res
                st.session_state.confirm_stratify = False
                st.rerun()
            if c2.button("Cancel"):
                st.session_state.confirm_stratify = False
                st.rerun()

        if st.session_state.get('stratified_results'):
            for fn, ddf in st.session_state.stratified_results.items():
                is_ex = "Excel" in st.session_state.output_format
                st.download_button(f"Download {fn}", data=to_excel(ddf) if is_ex else to_csv(ddf), file_name=f"{fn}.{'xlsx' if is_ex else 'csv'}", key=f"dl_{fn}")

if __name__ == "__main__":
    main()
