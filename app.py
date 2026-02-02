# -*- coding: utf-8 -*-

# Data Sift - Versão 2.0 (Otimizada para Performance e Memória)
import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
from datetime import datetime
from zoneinfo import ZoneInfo
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
1. **Filtering:** To clean your database by removing rows that are not of interest.
2. **Stratification:** To divide your database into specific subgroups.""",
    "1. Global Settings": """**1. Global Settings**
Essencial para carregar o arquivo e definir colunas de referência (Idade/Sexo).""",
    "2. Filter Tool": """**2. Filter Tool**
O objetivo é remover linhas. Regras ativas indicam o que deve ser **excluído**.""",
    "3. Stratification Tool": """**3. Stratification Tool**
Divide a planilha em múltiplos arquivos baseados em estratos de idade e sexo."""
}

DEFAULT_FILTERS = [
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '50', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '600', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ultra-PCR.ULTRAPCR', 'p_op1': '>', 'p_val1': '5', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
]

# --- MOTOR DE PROCESSAMENTO (BACKEND OTIMIZADO) ---

@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '==', 'Não é igual a': '!=', '≥': '>=', '≤': '<=', 'is equal to': '==', 'Not equal to': '!='}

    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series): return series
        return pd.to_numeric(series.astype(str).str.replace(',', '.', regex=False), errors='coerce')

    def _apply_comparison(self, s: pd.Series, op: str, val: float) -> pd.Series:
        if op in ['==', 'is equal to']: return s == val
        if op in ['!=', 'Not equal to']: return s != val
        if op == '>': return s > val
        if op == '<': return s < val
        if op == '>=' or op == '≥': return s >= val
        if op == '<=' or op == '≤': return s <= val
        return pd.Series([False] * len(s), index=s.index)

    def _create_main_mask(self, df: pd.DataFrame, f: Dict, col: str) -> pd.Series:
        op1_ui, val1 = f.get('p_op1'), f.get('p_val1')
        
        # Caso especial: Vazio
        if val1 and val1.lower() == 'empty':
            is_empty = df[col].isna() | (df[col].astype(str).str.strip() == '')
            return is_empty if op1_ui in ['=', 'is equal to'] else ~is_empty

        try:
            s_num = self._safe_to_numeric(df[col])
            v1 = float(str(val1).replace(',', '.'))

            if f.get('p_expand'):
                logic = f.get('p_op_central', 'OR').upper()
                v2 = float(str(f.get('p_val2')).replace(',', '.'))
                op2 = f.get('p_op2')

                if logic == 'BETWEEN':
                    low, high = sorted((v1, v2))
                    return s_num.between(low, high)
                
                m1 = self._apply_comparison(s_num, op1_ui, v1)
                m2 = self._apply_comparison(s_num, op2, v2)
                return (m1 & m2) if logic == 'AND' else (m1 | m2)
            
            return self._apply_comparison(s_num, op1_ui, v1)
        except:
            return pd.Series([False] * len(df), index=df.index)

    def _create_conditional_mask(self, df: pd.DataFrame, f: Dict, global_config: Dict) -> pd.Series:
        if not f.get('c_check'): return pd.Series([True] * len(df), index=df.index)
        
        cond_mask = pd.Series(True, index=df.index)
        
        # Idade
        c_age = global_config.get('coluna_idade')
        if f.get('c_idade_check') and c_age in df.columns:
            s_age = self._safe_to_numeric(df[c_age])
            if f.get('c_idade_val1'):
                cond_mask &= self._apply_comparison(s_age, f['c_idade_op1'], float(str(f['c_idade_val1']).replace(',','.')))
            if f.get('c_idade_val2'):
                cond_mask &= self._apply_comparison(s_age, f['c_idade_op2'], float(str(f['c_idade_val2']).replace(',','.')))

        # Sexo
        c_sex = global_config.get('coluna_sexo')
        if f.get('c_sexo_check') and c_sex in df.columns:
            val_sex = str(f.get('c_sexo_val', '')).strip().lower()
            if val_sex:
                cond_mask &= (df[c_sex].astype(str).str.strip().str.lower() == val_sex)
        
        return cond_mask

    def apply_filters(self, df: pd.DataFrame, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
        active = [f for f in filters_config if f['p_check']]
        if not active: return df
        
        # Máscara global de EXCLUSÃO
        to_exclude = pd.Series(False, index=df.index)
        total = len(active)

        for i, f in enumerate(active):
            progress_bar.progress((i/total), text=f"Processando regra: {f.get('p_col')}...")
            col = f.get('p_col')
            if col and col in df.columns:
                m_main = self._create_main_mask(df, f, col)
                m_cond = self._create_conditional_mask(df, f, global_config)
                to_exclude |= (m_main & m_cond)

        progress_bar.progress(1.0, text="Finalizado!")
        return df[~to_exclude]

    def apply_stratification(self, df: pd.DataFrame, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        # Implementação simplificada mantendo a lógica de nomeação original
        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')
        if not col_idade or not col_sexo: return {}

        df[col_idade] = self._safe_to_numeric(df[col_idade])
        generated = {}
        
        age_rules = strata_config.get('ages', [])
        sex_rules = strata_config.get('sexes', [])
        
        # Combinações de Sexo e Idade
        for i, s_rule in enumerate(sex_rules):
            for j, a_rule in enumerate(age_rules):
                mask = pd.Series(True, index=df.index)
                
                # Filtro Sexo
                mask &= (df[col_sexo].astype(str).str.strip() == str(s_rule['value']))
                
                # Filtro Idade
                v1 = float(str(a_rule['val1']).replace(',','.'))
                mask &= self._apply_comparison(df[col_idade], a_rule['op1'], v1)
                if a_rule.get('val2'):
                    v2 = float(str(a_rule['val2']).replace(',','.'))
                    mask &= self._apply_comparison(df[col_idade], a_rule['op2'], v2)
                
                res = df[mask]
                if not res.empty:
                    name = f"Stratum_{s_rule['value']}_Age_{v1}"
                    generated[name] = res
        
        progress_bar.progress(1.0)
        return generated

# --- AUXILIARES ---

@st.cache_data
def load_dataframe(uploaded_file):
    if uploaded_file is None: return None
    try:
        uploaded_file.seek(0)
        if uploaded_file.name.endswith('.csv'):
            return pd.read_csv(uploaded_file, sep=None, engine='python', decimal=',', encoding='latin-1')
        return pd.read_excel(uploaded_file, engine='openpyxl')
    except Exception as e:
        st.error(f"Erro ao carregar: {e}"); return None

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

# --- INTERFACE ---

def handle_select_all():
    new_state = st.session_state.get('select_all_master_checkbox', False)
    for rule in st.session_state.filter_rules:
        rule['p_check'] = new_state

def draw_filter_rules(sex_values, col_options):
    st.markdown("<style>.stCheckbox {margin-bottom: -15px;} .stButton>button {width:100%}</style>", unsafe_allow_html=True)
    
    cols_h = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5])
    cols_h[0].checkbox("All", key='select_all_master_checkbox', on_change=handle_select_all, label_visibility="collapsed")
    cols_h[1].write("**Column**")
    cols_h[2].write("**Operator**")
    cols_h[3].write("**Value**")
    cols_h[5].write("**Compound**")
    cols_h[6].write("**Cond.**")
    
    ops_main = ["", ">", "<", "=", "Not equal to", "≥", "≤"]

    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            c = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5])
            rule['p_check'] = c[0].checkbox(" ", value=rule.get('p_check', True), key=f"chk_{rule['id']}", label_visibility="collapsed")
            
            rule['p_col'] = c[1].selectbox("Col", options=col_options, index=col_options.index(rule['p_col']) if rule['p_col'] in col_options else None, key=f"col_{rule['id']}", label_visibility="collapsed", placeholder="Select Col")
            
            rule['p_op1'] = c[2].selectbox("Op1", ops_main, index=ops_main.index(rule['p_op1']) if rule['p_op1'] in ops_main else 0, key=f"op1_{rule['id']}", label_visibility="collapsed")
            rule['p_val1'] = c[3].text_input("V1", value=rule['p_val1'], key=f"val1_{rule['id']}", label_visibility="collapsed")
            
            rule['p_expand'] = c[4].checkbox("+", value=rule['p_expand'], key=f"exp_{rule['id']}")
            
            if rule['p_expand']:
                exp = c[5].columns([1.5, 1, 1.5])
                rule['p_op_central'] = exp[0].selectbox("L", ["OR", "AND", "BETWEEN"], key=f"log_{rule['id']}", label_visibility="collapsed")
                rule['p_op2'] = exp[1].selectbox("Op2", ops_main, key=f"op2_{rule['id']}", label_visibility="collapsed")
                rule['p_val2'] = exp[2].text_input("V2", value=rule['p_val2'], key=f"val2_{rule['id']}", label_visibility="collapsed")
            
            rule['c_check'] = c[6].checkbox("C", value=rule['c_check'], key=f"cchk_{rule['id']}")
            
            btn = c[7].columns(2)
            if btn[0].button("Clone", key=f"cln_{rule['id']}"):
                nr = copy.deepcopy(rule); nr['id'] = str(uuid.uuid4())
                st.session_state.filter_rules.insert(i+1, nr); st.rerun()
            if btn[1].button("X", key=f"del_{rule['id']}"):
                st.session_state.filter_rules.pop(i); st.rerun()

            if rule['c_check']:
                cond = st.columns([1, 1, 4, 1, 4])
                cond[1].write("↳")
                rule['c_idade_check'] = cond[2].checkbox("Age Condition", value=rule['c_idade_check'], key=f"ca_{rule['id']}")
                rule['c_sexo_check'] = cond[4].checkbox("Sex Condition", value=rule['c_sexo_check'], key=f"cs_{rule['id']}")

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    if not st.session_state.lgpd_accepted:
        st.title("Data Sift")
        st.markdown(GDPR_TERMS)
        if st.button("Accept and Continue"):
            st.session_state.lgpd_accepted = True; st.rerun()
        return

    if 'filter_rules' not in st.session_state: st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)
    if 'stratum_rules' not in st.session_state: st.session_state.stratum_rules = []

    st.title("Data Sift")
    
    with st.expander("1. Global Settings", expanded=True):
        up = st.file_uploader("Upload", type=['csv', 'xlsx'])
        df = load_dataframe(up)
        cols = df.columns.tolist() if df is not None else []
        
        c1, c2, c3 = st.columns(3)
        sel_age = c1.selectbox("Age Col", cols, key="col_idade", index=None)
        sel_sex = c2.selectbox("Sex Col", cols, key="col_sexo", index=None)
        fmt = c3.selectbox("Format", ["CSV", "Excel"], key="output_format")

    t1, t2 = st.tabs(["Filters", "Stratification"])
    
    with t1:
        draw_filter_rules([], cols)
        if st.button("Add Rule"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '=', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''})
            st.rerun()
        
        if st.button("RUN FILTER", type="primary"):
            if df is not None:
                proc = get_data_processor()
                prog = st.progress(0)
                res = proc.apply_filters(df, st.session_state.filter_rules, {"coluna_idade": sel_age, "coluna_sexo": sel_sex}, prog)
                st.session_state.res_df = res
                st.success(f"Restantes: {len(res)} linhas.")
            else: st.error("No file!")

        if 'res_df' in st.session_state:
            data = to_excel(st.session_state.res_df) if fmt == "Excel" else to_csv(st.session_state.res_df)
            st.download_button("Download Result", data, f"result.{'xlsx' if fmt=='Excel' else 'csv'}")

if __name__ == "__main__":
    main()
