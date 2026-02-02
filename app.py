# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
from datetime import datetime
from typing import List, Dict, Any, Optional

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Data Sift - Validação Laboratorial")

# --- FILTROS PADRÃO (CONSOLIDADO E ATUALIZADO) ---
# Esta lista contém as regras de EXCLUSÃO. Se o dado cair aqui, ele é removido.
DEFAULT_FILTERS = [
    # Filtros das Imagens Originais (Mantidos)
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '50', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '600', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ultra-PCR.ULTRAPCR', 'p_op1': '>', 'p_val1': '5', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    
    # Novos Filtros Solicitados
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Creatinina.CRE', 'p_op1': '>', 'p_val1': '1,5', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Creatinina.eTGF2021', 'p_op1': '<', 'p_val1': '60', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'GLICOSE.GLI', 'p_op1': '<', 'p_val1': '65', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '200', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'HBGLI.HBGLI', 'p_op1': '>', 'p_val1': '6,5', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'TSH.TSH', 'p_op1': '<', 'p_val1': '0,2', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '10', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'TGP.TGP', 'p_op1': '>', 'p_val1': '123', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'BTF.BTBTF', 'p_op1': '>', 'p_val1': '2,4', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'FALC.FALC', 'p_op1': '>', 'p_val1': '193,5', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'GGT.GGT', 'p_op1': '>', 'p_val1': '180', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.COL2', 'p_op1': '>', 'p_val1': '190', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.COL2', 'p_op1': '>', 'p_val1': '190', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.TRI2', 'p_op1': '>', 'p_val1': '150', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.TRI2', 'p_op1': '>', 'p_val1': '150', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.LDL2', 'p_op1': '>', 'p_val1': '130', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.LDLD', 'p_op1': '>', 'p_val1': '130', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.HDL5', 'p_op1': '>', 'p_val1': '80', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.HDL5', 'p_op1': '>', 'p_val1': '80', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '', 'p_val2': '', 'c_check': False},
]

# --- LÓGICA DE PROCESSAMENTO ---

class DataProcessor:
    def __init__(self):
        self.operator_map = {'=': '==', '≥': '>=', '≤': '<='}

    def _convert_value(self, val: str) -> float:
        try:
            return float(str(val).replace(',', '.'))
        except:
            return 0.0

    def apply_filters(self, df: pd.DataFrame, filters: List[Dict], progress_bar) -> pd.DataFrame:
        total_steps = len([f for f in filters if f['p_check']])
        if total_steps == 0:
            return df

        current_step = 0
        df_filtered = df.copy()

        for f in filters:
            if not f['p_check']:
                continue
            
            col = f['p_col']
            if col not in df_filtered.columns:
                continue

            # Converte a coluna para numérico para garantir a comparação
            series = pd.to_numeric(df_filtered[col].astype(str).str.replace(',', '.'), errors='coerce')
            
            op1 = self.operator_map.get(f['p_op1'], f['p_op1'])
            v1 = self._convert_value(f['p_val1'])
            
            # Criação da máscara de exclusão
            mask_exclude = eval(f"series {op1} {v1}")
            
            if f.get('p_expand') and f.get('p_val2'):
                op2 = self.operator_map.get(f['p_op2'], f['p_op2'])
                v2 = self._convert_value(f['p_val2'])
                m2 = eval(f"series {op2} {v2}")
                
                if f.get('p_op_central') == 'OR':
                    mask_exclude = mask_exclude | m2
                else:
                    mask_exclude = mask_exclude & m2

            # Remove as linhas que batem com o critério de exclusão
            df_filtered = df_filtered[~mask_exclude.fillna(False)]
            
            current_step += 1
            progress_bar.progress(current_step / total_steps, text=f"Limpando: {col}")

        return df_filtered

# --- INTERFACE STREAMLIT ---

def main():
    st.title("Data Sift - Limpeza de Dados Clínicos")
    
    if 'filter_rules' not in st.session_state:
        st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)

    # 1. Upload e Configurações
    with st.sidebar:
        st.header("1. Entrada de Arquivo")
        uploaded_file = st.file_uploader("Arraste sua planilha (CSV ou XLSX)", type=['csv', 'xlsx'])
        
        separator = st.text_input("Separador (apenas CSV)", value=";")
        decimal = st.text_input("Decimal (apenas CSV)", value=",")
        encoding = st.selectbox("Codificação", ["utf-8-sig", "latin-1", "cp1252"])
        
        st.divider()
        st.header("2. Formato de Saída")
        out_fmt = st.radio("Salvar como:", ["CSV (.csv)", "Excel (.xlsx)"])

    # 2. Gestão de Filtros
    st.subheader("Configurações de Filtros de Exclusão")
    
    # Renderização da Tabela de Filtros
    cols_header = st.columns([1, 4, 2, 2, 1, 2, 2])
    cols_header[0].write("**Ativo**")
    cols_header[1].write("**Analito**")
    cols_header[2].write("**Op 1**")
    cols_header[3].write("**Valor 1**")
    cols_header[4].write("**Lógica**")
    cols_header[5].write("**Op 2**")
    cols_header[6].write("**Valor 2**")

    for i, rule in enumerate(st.session_state.filter_rules):
        r_cols = st.columns([1, 4, 2, 2, 1, 2, 2])
        rule['p_check'] = r_cols[0].checkbox(" ", value=rule['p_check'], key=f"c_{i}")
        rule['p_col'] = r_cols[1].text_input("Coluna", value=rule['p_col'], key=f"col_{i}")
        rule['p_op1'] = r_cols[2].selectbox("Op1", ["<", ">", "=", "≥", "≤"], 
                                          index=["<", ">", "=", "≥", "≤"].index(rule['p_op1']) if rule['p_op1'] in ["<", ">", "=", "≥", "≤"] else 0, 
                                          key=f"op1_{i}", label_visibility="collapsed")
        rule['p_val1'] = r_cols[3].text_input("V1", value=rule['p_val1'], key=f"v1_{i}", label_visibility="collapsed")
        
        if rule.get('p_expand'):
            r_cols[4].write(rule['p_op_central'])
            rule['p_op2'] = r_cols[5].selectbox("Op2", ["<", ">", "=", "≥", "≤"], 
                                              index=["<", ">", "=", "≥", "≤"].index(rule['p_op2']) if rule['p_op2'] in ["<", ">", "=", "≥", "≤"] else 0, 
                                              key=f"op2_{i}", label_visibility="collapsed")
            rule['p_val2'] = r_cols[6].text_input("V2", value=rule['p_val2'], key=f"v2_{i}", label_visibility="collapsed")
        else:
            r_cols[4].write("-")

    # 3. Processamento
    if st.button("PROCESSAR E GERAR ARQUIVO", type="primary", use_container_width=True):
        if uploaded_file:
            try:
                # Carregamento Otimizado
                with st.spinner("Lendo arquivo..."):
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file, sep=separator, decimal=decimal, encoding=encoding)
                    else:
                        df = pd.read_excel(uploaded_file)

                # Processamento
                p_bar = st.progress(0)
                processor = DataProcessor()
                df_clean = processor.apply_filters(df, st.session_state.filter_rules, p_bar)
                
                st.success(f"Sucesso! {len(df) - len(df_clean)} linhas removidas. {len(df_clean)} linhas restantes.")

                # Preparação do Download (Evitando estourar a memória)
                if out_fmt == "CSV (.csv)":
                    csv_buffer = df_clean.to_csv(index=False, sep=separator, decimal=decimal, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button("BAIXAR RESULTADO (CSV)", data=csv_buffer, file_name="resultado_limpo.csv", mime="text/csv", use_container_width=True)
                else:
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_clean.to_excel(writer, index=False)
                    st.download_button("BAIXAR RESULTADO (EXCEL)", data=output.getvalue(), file_name="resultado_limpo.xlsx", use_container_width=True)

            except Exception as e:
                st.error(f"Erro inesperado: {str(e)}")
        else:
            st.warning("Por favor, faça o upload de um arquivo primeiro.")

if __name__ == "__main__":
    main()
