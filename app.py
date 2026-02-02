# -*- coding: utf-8 -*-

# Data Sift - Versão 3.0 (Enterprise/High-Performance Edition)
import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
import gc
from datetime import datetime
from typing import List, Dict, Any

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Data Sift | Laboratory Data Filter")

# --- CONTEÚDO ESTÁTICO (DISCLAIMER E MANUAL) ---
GDPR_TERMS = """
### Data Privacy & Compliance (LGPD / GDPR)

This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, health information, etc.), the processing of which is regulated by data protection laws like the **General Data Protection Regulation (GDPR)** and the **Lei Geral de Proteção de Dados (LGPD)**.

**Important Notices:**
1. **Local Processing:** Data is processed in the application's temporary memory and is not permanently stored on our servers.
2. **Your Responsibility:** It is your sole responsibility to ensure that all data used in this tool complies with applicable regulations. We strongly recommend using **anonymized data**.
3. **Liability:** The responsibility for the nature and legality of the processed data is exclusively yours.

*By proceeding, you confirm that the data has been properly handled and you accept responsibility for its processing.*
"""

MANUAL_CONTENT = {
    "Introduction": """**Welcome to Data Sift!**
This program is a professional spreadsheet filter tool designed to handle large volumes of laboratory data (up to 500MB) by offering two main functionalities:
1. **Filtering (Exclusion):** Clean your database by removing rows based on clinical or technical criteria.
2. **Stratification:** Divide your database into specific subgroups (Age/Sex).""",
    "1. Global Settings": """**1. Global Settings**
Upload your file (CSV recommended for files >100MB) and define which columns represent **Age** and **Sex** for conditional rules.""",
    "2. Filter Tool": """**2. Filter Tool**
The goal is to remove rows. Rules active here define what will be **EXCLUDED** from the final file. 
*Example: If you set 'Ferritin < 15', all rows with Ferritin lower than 15 will be deleted.*""",
}

DEFAULT_FILTERS = [
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '50', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '600', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
]

# --- MOTOR DE PROCESSAMENTO (CHUNKING LOGIC) ---

class DataProcessor:
    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series): return series
        return pd.to_numeric(series.astype(str).str.replace(',', '.', regex=False), errors='coerce')

    def _apply_comparison(self, s: pd.Series, op: str, val: float) -> pd.Series:
        if op in ['==', '=', 'is equal to']: return s == val
        if op in ['!=', 'Not equal to']: return s != val
        if op == '>': return s > val
        if op == '<': return s < val
        if op in ['>=', '≥']: return s >= val
        if op in ['<=', '≤']: return s <= val
        return pd.Series([False] * len(s), index=s.index)

    def _create_mask(self, df: pd.DataFrame, f: Dict, global_config: Dict) -> pd.Series:
        col = f.get('p_col')
        if not col or col not in df.columns: return pd.Series([False] * len(df), index=df.index)

        try:
            s_num = self._safe_to_numeric(df[col])
            v1 = float(str(f.get('p_val1')).replace(',', '.'))
            
            if f.get('p_expand'):
                v2 = float(str(f.get('p_val2')).replace(',', '.'))
                logic = f.get('p_op_central', 'OR').upper()
                if logic == 'BETWEEN':
                    m_main = s_num.between(min(v1, v2), max(v1, v2))
                else:
                    m1 = self._apply_comparison(s_num, f['p_op1'], v1)
                    m2 = self._apply_comparison(s_num, f['p_op2'], v2)
                    m_main = (m1 & m2) if logic == 'AND' else (m1 | m2)
            else:
                m_main = self._apply_comparison(s_num, f['p_op1'], v1)
        except:
            m_main = pd.Series([False] * len(df), index=df.index)

        m_cond = pd.Series(True, index=df.index)
        if f.get('c_check'):
            c_age = global_config.get('coluna_idade')
            if f.get('c_idade_check') and c_age in df.columns:
                s_age = self._safe_to_numeric(df[c_age])
                if f.get('c_idade_val1'):
                    m_cond &= self._apply_comparison(s_age, f['c_idade_op1'], float(str(f['c_idade_val1']).replace(',','.')))
                if f.get('c_idade_val2'):
                    m_cond &= self._apply_comparison(s_age, f['c_idade_op2'], float(str(f['c_idade_val2']).replace(',','.')))
            
            c_sex = global_config.get('coluna_sexo')
            if f.get('c_sexo_check') and c_sex in df.columns:
                val_sex = str(f.get('c_sexo_val', '')).strip().lower()
                if val_sex:
                    m_cond &= (df[c_sex].astype(str).str.strip().str.lower() == val_sex)

        return m_main & m_cond

    def run_chunked_filter(self, file_obj, filters, config):
        active = [f for f in filters if f['p_check']]
        processed_chunks = []
        chunk_size = 45000 
        file_obj.seek(0)
        
        try:
            is_csv = hasattr(file_obj, 'name') and file_obj.name.endswith('.csv')
            if is_csv:
                reader = pd.read_csv(file_obj, sep=None, engine='python', encoding='latin-1', chunksize=chunk_size)
            else:
                # Nota: Excel não permite chunking nativo, mas tentamos ler de forma otimizada
                df_full = pd.read_excel(file_obj)
                reader = [df_full]

            prog_bar = st.progress(0, text="Lendo e filtrando blocos...")
            
            for i, chunk in enumerate(reader):
                # Otimização de Memória (Downcasting)
                for c in chunk.select_dtypes('float').columns:
                    chunk[c] = pd.to_numeric(chunk[c], downcast='float')
                
                to_exclude = pd.Series(False, index=chunk.index)
                for f in active:
                    to_exclude |= self._create_mask(chunk, f, config)
                
                processed_chunks.append(chunk[~to_exclude].copy())
                del chunk
                gc.collect()
                prog_bar.progress(0.5, text=f"Processando bloco {i+1}...")

            final_df = pd.concat(processed_chunks, ignore_index=True)
            prog_bar.progress(1.0, text="Processamento Finalizado!")
            return final_df
        except Exception as e:
            st.error(f"Erro Crítico: {e}")
            return None

# --- COMPONENTES DE INTERFACE ---

def draw_filters_ui(cols):
    ops = ["", ">", "<", "=", "Not equal to", "≥", "≤"]
    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            c = st.columns([0.5, 3, 2, 2, 0.5, 3, 1])
            rule['p_check'] = c[0].checkbox("On", value=rule['p_check'], key=f"chk_{rule['id']}", label_visibility="collapsed")
            rule['p_col'] = c[1].selectbox("Col", cols, index=cols.index(rule['p_col']) if rule['p_col'] in cols else 0, key=f"col_{rule['id']}", label_visibility="collapsed")
            rule['p_op1'] = c[2].selectbox("Op", ops, index=ops.index(rule['p_op1']) if rule['p_op1'] in ops else 0, key=f"op_{rule['id']}", label_visibility="collapsed")
            rule['p_val1'] = c[3].text_input("Val", value=rule['p_val1'], key=f"val_{rule['id']}", label_visibility="collapsed")
            rule['p_expand'] = c[4].checkbox("+", value=rule['p_expand'], key=f"exp_{rule['id']}")
            
            if rule['p_expand']:
                exp = c[5].columns([1, 1, 1])
                rule['p_op_central'] = exp[0].selectbox("Log", ["OR", "AND", "BETWEEN"], key=f"log_{rule['id']}", label_visibility="collapsed")
                rule['p_op2'] = exp[1].selectbox("Op2", ops, key=f"op2_{rule['id']}", label_visibility="collapsed")
                rule['p_val2'] = exp[2].text_input("V2", value=rule['p_val2'], key=f"val2_{rule['id']}", label_visibility="collapsed")
            
            if c[6].button("X", key=f"del_{rule['id']}"):
                st.session_state.filter_rules.pop(i); st.rerun()

# --- APLICAÇÃO PRINCIPAL ---

def main():
    if 'accepted_terms' not in st.session_state: st.session_state.accepted_terms = False
    
    # TELA DE TERMOS (LGPD)
    if not st.session_state.accepted_terms:
        st.title("Data Sift | Secure Filter")
        st.markdown(GDPR_TERMS)
        if st.button("I accept the terms and wish to proceed"):
            st.session_state.accepted_terms = True
            st.rerun()
        return

    # INICIALIZAÇÃO DE ESTADO
    if 'filter_rules' not in st.session_state: st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)

    st.title("Data Sift 3.0")
    
    with st.sidebar:
        st.header("1. Data Input")
        up = st.file_uploader("Upload CSV/Excel (Max 500MB)", type=['csv', 'xlsx'])
        
        with st.expander("Help / Manual"):
            for title, content in MANUAL_CONTENT.items():
                st.markdown(f"**{title}**\n{content}")

    if up:
        # Extração leve do cabeçalho
        up.seek(0)
        if up.name.endswith('.csv'):
            header = pd.read_csv(up, nrows=0, sep=None, engine='python', encoding='latin-1')
        else:
            header = pd.read_excel(up, nrows=0)
        
        cols = header.columns.tolist()

        with st.expander("2. Global Settings", expanded=True):
            c1, c2, c3 = st.columns(3)
            sel_age = c1.selectbox("Age Column", cols, index=None, placeholder="Select age column")
            sel_sex = c2.selectbox("Sex Column", cols, index=None, placeholder="Select sex column")
            out_fmt = c3.selectbox("Output Format", ["CSV", "Excel"])

        st.subheader("3. Filter Rules (Exclusion Logic)")
        draw_filters_ui(cols)
        
        if st.button("Add New Rule"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': cols[0], 'p_op1': '=', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False})
            st.rerun()

        st.divider()
        if st.button("RUN HEAVY PROCESSING", type="primary", use_container_width=True):
            proc = DataProcessor()
            config = {"coluna_idade": sel_age, "coluna_sexo": sel_sex}
            
            # O processamento acontece aqui sem carregar o arquivo inteiro na RAM global
            res = proc.run_chunked_filter(up, st.session_state.filter_rules, config)
            
            if res is not None:
                st.session_state.final_df = res
                st.success(f"Processing Complete! {len(res)} rows retained.")

        # DOWNLOAD DO RESULTADO
        if 'final_df' in st.session_state:
            df_res = st.session_state.final_df
            if out_fmt == "CSV":
                csv_data = df_res.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📥 Download Filtered CSV", csv_data, "filtered_data.csv", "text/csv")
            else:
                out_io = io.BytesIO()
                with pd.ExcelWriter(out_io, engine='openpyxl') as writer:
                    df_res.to_excel(writer, index=False)
                st.download_button("📥 Download Filtered Excel", out_io.getvalue(), "filtered_data.xlsx")

if __name__ == "__main__":
    main()
