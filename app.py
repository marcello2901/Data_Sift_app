# --- VERIFICADOR E INSTALADOR DE DEPENDÊNCIAS ---
import sys
import subprocess
import importlib.util

print("Verificando dependências necessárias...")
required_packages = {'pandas': 'pandas', 'openpyxl': 'openpyxl', 'xlrd': 'xlrd', 'Pillow': 'PIL'}
for package, module_name in required_packages.items():
    if importlib.util.find_spec(module_name) is None:
        print(f"O pacote '{package}' não foi encontrado. Instalando...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"'{package}' instalado com sucesso.")
        except subprocess.CalledProcessError as e:
            print(f"ERRO: Falha ao instalar o pacote '{package}'.\nPor favor, instale-o manualmente: pip install {package}\nErro original: {e}")
            sys.exit(1)
print("Todas as dependências estão satisfeitas.\n")
# --- FIM DO VERIFICADOR ---


# -*- coding: utf-8 -*-

# Versão 11.8 - Correção de scroll simultâneo e layout da janela de ajuda
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
import threading
import queue
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
from PIL import Image, ImageTk

# --- CONSTANTES ---
CONFIG_FILE_FILTERS = "config_filtros_v11.json"
MANUAL_CONTENT = {
    "Introdução": """**Bem-vindo à Ferramenta de Análise de Planilhas!**

Este programa foi projetado para otimizar seu trabalho com grandes volumes de dados, oferecendo duas funcionalidades principais:

1.  **Filtragem:** Para limpar sua base de dados, removendo linhas que não são de seu interesse.
2.  **Estratificação:** Para dividir sua base de dados em subgrupos específicos.

Navegue pelos tópicos no menu à esquerda para aprender a usar cada parte da ferramenta.""",
    "1. Configurações Globais": """**1. Configurações Globais**

Esta seção, localizada no topo da janela, contém as configurações essenciais que são compartilhadas entre as duas ferramentas. Configure-as uma vez para usar em ambas as abas.

- **Selecionar Planilha...**
  Abre uma janela para selecionar o arquivo de dados de origem. Suporta os formatos `.xlsx`, `.xls` e `.csv`. Uma vez selecionado, o arquivo fica disponível para ambas as ferramentas.

- **Selecionar Pasta de Saída...**
  Abre uma janela para selecionar a pasta onde os novos arquivos gerados (filtrados ou estratificados) serão salvos. **Dica:** Se nenhuma pasta for selecionada, os arquivos serão salvos no mesmo local da planilha original.

- **Coluna Idade / Coluna Sexo**
  Campos para especificar o nome **exato** do cabeçalho da coluna em sua planilha. **Atenção:** O nome deve ser idêntico, incluindo maiúsculas e minúsculas (ex: "Idade" é diferente de "idade").

- **Valor para masculino / feminino**
  Campos para definir o valor exato que representa cada sexo na sua planilha (ex: 'M', 'Masculino'). É crucial para o funcionamento correto da "Ferramenta de Estratificação".

- **Formato de Saída**
  Menu de seleção para escolher o formato dos arquivos gerados. O padrão é `.csv`. Escolha `Excel (.xlsx)` para maior compatibilidade com o Microsoft Excel ou `CSV (.csv)` para um formato mais leve e universal.""",
    "2. Ferramenta de Filtro": """**2. Ferramenta de Filtro**

O objetivo desta ferramenta é **"limpar"** sua planilha, **removendo** linhas que correspondam a critérios específicos. O resultado é um **único arquivo** contendo apenas os dados que "sobreviveram" aos filtros.

**Funcionamento das Regras de Exclusão**
Cada linha que você adiciona é uma condição para **remover** dados. Se uma linha da sua planilha corresponder a uma regra ativa, ela **será excluída** do arquivo final.

- **[✓] (Caixa de Ativação):** Liga ou desliga uma regra sem precisar apagá-la.

- **Coluna:** O nome da coluna onde o filtro será aplicado. **Dica:** você pode aplicar a mesma regra a várias colunas de uma vez, separando seus nomes por ponto e vírgula (`;`).

- **Operador e Valor:** Define a lógica da regra. A palavra-chave `vazio` é um recurso poderoso:
    - **Cenário 1: Excluir linhas com dados FALTANTES.**
        - **Objetivo:** Remover da análise todos os pacientes que não possuem um valor na coluna "Exame_X".
        - **Configuração:** `Coluna: "Exame_X"`, `Operador: "é igual a"`, `Valor: "vazio"`.
        - **Resultado:** Apenas as linhas que tinham um valor preenchido em "Exame_X" permanecerão.
    - **Cenário 2: Manter apenas linhas com dados EXISTENTES.**
        - **Objetivo:** Analisar apenas os laudos que possuem algum texto na coluna "Observações", excluindo todos os que não têm nada escrito.
        - **Configuração:** `Coluna: "Observações"`, `Operador: "Não é igual a"`, `Valor: "vazio"`.
        - **Resultado:** A planilha final conterá apenas as linhas que tinham algum tipo de texto preenchido na coluna "Observações".

- **Botão `+` / `-` (Regra Composta):** Expande a regra para criar condições `E` / `OU`.
    - **Exemplo `OU`:** Para excluir todos que têm glicose muito baixa **OU** muito alta, use: `GLICOSE.GLI < 70 OU > 150`.

- **Condição:** Permite aplicar um filtro secundário. A regra principal só será aplicada às linhas que também atenderem a esta condição.

- **Ações:** `+ clonar` para duplicar a regra; `X` para apagar.

- **Gerar Planilha Filtrada:** Inicia o processo. Um único arquivo, nomeado `Planilha_Filtrada_` com data e hora, será salvo na pasta de saída.""",
    "3. Ferramenta de Estratificação": """**3. Ferramenta de Estratificação**

Diferente do filtro, o objetivo desta ferramenta é **dividir** sua planilha em **vários arquivos menores**, onde cada arquivo representa um subgrupo de interesse (um "estrato").

**Funcionamento da Estratificação**

- **Opções de Estratificação por Sexo:**
  - **[✓] Masculino** e **[✓] Feminino:** Estas caixas controlam a divisão por sexo.
      - Se você marcar **ambas**, cada faixa etária que você definir abaixo será dividida em dois arquivos: um para o sexo masculino e outro para o feminino.
      - Se marcar apenas **uma**, todas as faixas etárias serão geradas apenas para aquele sexo.
      - Se não marcar **nenhuma**, a estratificação levará em conta apenas as faixas etárias.

- **Definição das Faixas Etárias:**
  - Esta área serve **exclusivamente** para criar os estratos baseados em idade.
  - Cada linha representa uma faixa etária que resultará em um (ou mais) arquivos.
  - **Exemplo:** Para criar um estrato de 18 a 30 anos (inclusive), use os operadores: `≥ 18` E `≤ 30`.

- **Gerar Planilhas Estratificadas:**
  - Inicia o processo de divisão. O número de arquivos gerados será o (`nº de faixas etárias` x `nº de sexos selecionados`).
  - **Confirmação:** Antes de iniciar, o programa perguntará se você está usando uma planilha já filtrada. É altamente recomendável que sim para garantir a qualidade da análise."""
}
DEFAULT_FILTERS = [
    {'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '>', 'p_val2': '50', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '>', 'p_val2': '600', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Ultra-PCR.ULTRAPCR', 'p_op1': '>', 'p_val1': '5', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Hemo.#HGB', 'p_op1': '<', 'p_val1': '7,0', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Hemo.LEUCO', 'p_op1': '>', 'p_val1': '11000', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Creatinina.CRE', 'p_op1': '>', 'p_val1': '1,5', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Creatinina.eTFG2021', 'p_op1': '<', 'p_val1': '60', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'HBGLI.HBGLI', 'p_op1': '>', 'p_val1': '6,5', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'GLICOSE.GLI', 'p_op1': '>', 'p_val1': '200', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '65', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'TSH.TSH', 'p_op1': '>', 'p_val1': '10', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '0,01', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Idade', 'p_op1': '>', 'p_val1': '75', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Hemo.OBSSV', 'p_op1': 'Não é igual a', 'p_val1': 'vazio', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Hemo.OBSSB', 'p_op1': 'Não é igual a', 'p_val1': 'vazio', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'p_check': True, 'p_col': 'Hemo.OBSSP', 'p_op1': 'Não é igual a', 'p_val1': 'vazio', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
]

class DataProcessor:
    OPERATOR_MAP = {'=': '==', 'Não é igual a': '!=', '≥': '>=', '≤': '<='}
    def __init__(self, progress_queue: queue.Queue): self.progress_queue = progress_queue
    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series): return series
        return pd.to_numeric(series.astype(str).str.replace(',', '.', regex=False), errors='coerce')
    def _build_single_mask(self, series: pd.Series, op: str, val: Any) -> pd.Series:
        if isinstance(val, str) and op == '==': return series.astype(str).str.strip().str.lower() == val.lower().strip()
        return eval(f"series {op} val")
    def _create_main_mask(self, df: pd.DataFrame, f: Dict, col: str) -> pd.Series:
        op1_ui, val1 = f.get('p_op1'), f.get('p_val1')
        op1 = self.OPERATOR_MAP.get(op1_ui, op1_ui)
        if val1 and val1.lower() == 'vazio':
            if op1 == '==': return df[col].isna() | (df[col].astype(str).str.strip() == '')
            if op1 == '!=': return df[col].notna() & (df[col].astype(str).str.strip() != '')
            return pd.Series([False] * len(df), index=df.index)
        try:
            if f.get('p_expand'):
                v1_num = float(str(val1).replace(',', '.'))
                op_central, op2_ui, val2 = f.get('p_op_central'), f.get('p_op2'), f.get('p_val2')
                op2 = self.OPERATOR_MAP.get(op2_ui, op2_ui); v2_num = float(str(val2).replace(',', '.'))
                if op_central == 'ENTRE':
                    min_val, max_val = sorted((v1_num, v2_num)); return df[col].between(min_val, max_val, inclusive='both')
                m1 = self._build_single_mask(df[col], op1, v1_num); m2 = self._build_single_mask(df[col], op2, v2_num)
                if op_central == 'E': return m1 & m2
                if op_central == 'OU': return m1 | m2
            else:
                v1_num = float(str(val1).replace(',', '.')); return self._build_single_mask(df[col], op1, v1_num)
        except (ValueError, TypeError): return pd.Series([False] * len(df), index=df.index)
    def _create_conditional_mask(self, df: pd.DataFrame, f: Dict, global_config: Dict) -> pd.Series:
        mascara_condicional = pd.Series([True] * len(df), index=df.index)
        if not f.get('c_check'): return mascara_condicional
        col_idade = global_config.get('coluna_idade')
        if f.get('c_idade_check') and col_idade and col_idade in df.columns:
            try:
                op_idade1_ui, val_idade1 = f.get('c_idade_op1'), f.get('c_idade_val1')
                if op_idade1_ui and val_idade1:
                    op1 = self.OPERATOR_MAP.get(op_idade1_ui, op_idade1_ui); v1 = float(str(val_idade1).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op1, v1)
                op_idade2_ui, val_idade2 = f.get('c_idade_op2'), f.get('c_idade_val2')
                if op_idade2_ui and val_idade2:
                    op2 = self.OPERATOR_MAP.get(op_idade2_ui, op_idade2_ui); v2 = float(str(val_idade2).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op2, v2)
            except (ValueError, TypeError): pass
        col_sexo = global_config.get('coluna_sexo')
        if f.get('c_sexo_check') and col_sexo and col_sexo in df.columns:
            val_sexo_gui = f.get('c_sexo_val', '').lower().strip()
            if val_sexo_gui: mascara_condicional &= self._build_single_mask(df[col_sexo], '==', val_sexo_gui)
        return mascara_condicional
    def apply_filters(self, input_path: str, output_path: str, filters_config: List[Dict], global_config: Dict, formato_saida: str, cancel_event: threading.Event):
        try:
            self.progress_queue.put((10, "Carregando planilha..."))
            file_ext = os.path.splitext(input_path)[1].lower()
            if file_ext == '.csv':
                try: df = pd.read_csv(input_path, sep=';', decimal=',', encoding='latin-1')
                except Exception:
                    try: df = pd.read_csv(input_path, sep=',', decimal='.', encoding='utf-8')
                    except Exception as e: raise IOError(f"Não foi possível ler o arquivo CSV.\nErro: {e}")
            elif file_ext == '.xlsx': df = pd.read_excel(input_path, engine='openpyxl')
            elif file_ext == '.xls': df = pd.read_excel(input_path, engine='xlrd')
            else: raise ValueError(f"Formato de arquivo não suportado: {file_ext}")
            original_row_count = len(df); df_processado = df.copy()
            self.progress_queue.put((20, "Iniciando filtros...")); total_filters = len(filters_config)
            for i, f_config in enumerate(filters_config):
                if cancel_event.is_set(): self.progress_queue.put(('CANCELLED', "Operação de filtragem cancelada pelo usuário.")); return
                progress = 25 + int((i / total_filters) * 65) if total_filters > 0 else 90
                col_name = f_config.get('p_col', 'Regra desconhecida')
                self.progress_queue.put((progress, f"Aplicando regra {i+1}/{total_filters}: '{col_name[:40]}...'"))
                if df_processado.empty: break
                col_config_str = f_config.get('p_col', '')
                is_numeric_filter = f_config.get('p_val1', '').lower() != 'vazio'
                if is_numeric_filter:
                    cols_to_convert = [c.strip() for c in col_config_str.split(';')]
                    for col in cols_to_convert:
                        if col in df_processado.columns: df_processado[col] = self._safe_to_numeric(df_processado[col])
                if f_config.get('c_idade_check'):
                    col_idade = global_config.get('coluna_idade')
                    if col_idade and col_idade in df_processado.columns: df_processado[col_idade] = self._safe_to_numeric(df_processado[col_idade])
                if ';' in col_config_str:
                    cols_to_check = [c.strip() for c in col_config_str.split(';')]
                    combined_mask = pd.Series([True] * len(df_processado), index=df_processado.index)
                    for sub_col in cols_to_check:
                        if sub_col not in df_processado.columns:
                            combined_mask = pd.Series([False] * len(df_processado), index=df_processado.index); break
                        combined_mask &= self._create_main_mask(df_processado, f_config, sub_col)
                    mascara_principal = combined_mask
                else:
                    if not col_config_str or col_config_str not in df_processado.columns: continue
                    mascara_principal = self._create_main_mask(df_processado, f_config, col_config_str)
                mascara_condicional = self._create_conditional_mask(df_processado, f_config, global_config)
                mascara_final_da_regra = mascara_principal & mascara_condicional
                df_processado = df_processado[~mascara_final_da_regra]
            if cancel_event.is_set(): self.progress_queue.put(('CANCELLED', "Operação de filtragem cancelada pelo usuário.")); return
            df_final = df_processado
            self.progress_queue.put((95, "Salvando arquivo..."))
            if 'Excel' in formato_saida: df_final.to_excel(output_path, index=False)
            else: df_final.to_csv(output_path, index=False, sep=';', decimal=',', encoding='utf-8-sig')
            info_tuple = ("Processo de FILTRAGEM concluído!", original_row_count, len(df_final), output_path)
            self.progress_queue.put(('DONE', info_tuple))
        except Exception as e:
            error_message = f"Ocorreu um erro inesperado: {e}"
            if "zip file" in str(e): error_message = "O arquivo .xlsx parece estar corrompido ou não é um arquivo Excel válido.\n\nTente abri-lo no Excel e salvá-lo novamente."
            self.progress_queue.put(('ERROR', error_message))
    def apply_stratification(self, input_path: str, output_folder: str, strata_config: Dict, global_config: Dict, formato_saida: str, cancel_event: threading.Event):
        try:
            self.progress_queue.put((10, "Carregando planilha para estratificação..."))
            file_ext = os.path.splitext(input_path)[1].lower()
            if file_ext == '.csv':
                try: df = pd.read_csv(input_path, sep=';', decimal=',', encoding='latin-1')
                except Exception:
                    try: df = pd.read_csv(input_path, sep=',', decimal='.', encoding='utf-8')
                    except Exception as e: raise IOError(f"Não foi possível ler o arquivo CSV.\nErro: {e}")
            elif file_ext == '.xlsx': df = pd.read_excel(input_path, engine='openpyxl')
            elif file_ext == '.xls': df = pd.read_excel(input_path, engine='xlrd')
            else: raise ValueError(f"Formato de arquivo não suportado: {file_ext}")
            col_idade = global_config.get('coluna_idade'); col_sexo = global_config.get('coluna_sexo')
            if not (col_idade and col_idade in df.columns): raise ValueError(f"Coluna de idade '{col_idade}' não encontrada na planilha.")
            if not (col_sexo and col_sexo in df.columns): raise ValueError(f"Coluna de sexo '{col_sexo}' não encontrada na planilha.")
            df[col_idade] = self._safe_to_numeric(df[col_idade])
            age_strata = strata_config.get('ages', []); sex_strata = strata_config.get('sexes', [])
            if not age_strata and not sex_strata:
                self.progress_queue.put(('DONE', ("Nenhum estrato definido. Operação cancelada.", 0, 0, ""))); return
            final_strata_to_process = []
            if not age_strata and sex_strata:
                for sex_rule in sex_strata: final_strata_to_process.append({'age': None, 'sex': sex_rule})
            elif age_strata and not sex_strata:
                for age_rule in age_strata: final_strata_to_process.append({'age': age_rule, 'sex': None})
            else:
                for sex_rule in sex_strata:
                    for age_rule in age_strata: final_strata_to_process.append({'age': age_rule, 'sex': sex_rule})
            total_files = len(final_strata_to_process); files_generated = 0
            self.progress_queue.put((20, f"Iniciando. {total_files} planilhas a serem geradas..."))
            for i, stratum in enumerate(final_strata_to_process):
                if cancel_event.is_set(): self.progress_queue.put(('CANCELLED', "Operação de estratificação cancelada pelo usuário.")); return
                combined_mask = pd.Series(True, index=df.index)
                age_rule = stratum.get('age'); sex_rule = stratum.get('sex')
                if age_rule:
                    age_mask = pd.Series(True, index=df.index)
                    if age_rule.get('op1') and age_rule.get('val1'):
                        op1 = self.OPERATOR_MAP.get(age_rule['op1'], age_rule['op1']); val1 = float(str(age_rule['val1']).replace(',', '.'))
                        age_mask &= eval(f"df['{col_idade}'] {op1} {val1}")
                    if age_rule.get('op2') and age_rule.get('val2'):
                        op2 = self.OPERATOR_MAP.get(age_rule['op2'], age_rule['op2']); val2 = float(str(age_rule['val2']).replace(',', '.'))
                        age_mask &= eval(f"df['{col_idade}'] {op2} {val2}")
                    combined_mask &= age_mask
                if sex_rule:
                    sex_val = sex_rule.get('value')
                    if sex_val: combined_mask &= self._build_single_mask(df[col_sexo], '==', sex_val)
                stratum_df = df[combined_mask]
                filename = self._generate_stratum_name(age_rule, sex_rule)
                if not filename: continue
                progress = 25 + int(((i + 1) / total_files) * 70)
                self.progress_queue.put((progress, f"Gerando: {filename}..."))
                extensao = '.xlsx' if 'Excel' in formato_saida else '.csv'
                full_filename = f"{filename}{extensao}"
                output_path = os.path.join(output_folder, full_filename)
                if not stratum_df.empty:
                    if 'Excel' in formato_saida: stratum_df.to_excel(output_path, index=False)
                    else: stratum_df.to_csv(output_path, index=False, sep=';', decimal=',', encoding='utf-8-sig')
                    files_generated += 1
            if cancel_event.is_set(): self.progress_queue.put(('CANCELLED', "Operação de estratificação cancelada pelo usuário.")); return
            info_tuple = (f"Processo de ESTRATIFICAÇÃO concluído!", total_files, files_generated, output_folder)
            self.progress_queue.put(('DONE', info_tuple))
        except Exception as e: self.progress_queue.put(('ERROR', f"Ocorreu um erro na estratificação: {e}"))
    def _generate_stratum_name(self, age_rule: Optional[Dict], sex_rule: Optional[Dict]) -> str:
        name_parts = []
        if age_rule:
            op1, val1 = age_rule.get('op1'), age_rule.get('val1')
            op2, val2 = age_rule.get('op2'), age_rule.get('val2')
            def get_int(val): return int(float(str(val).replace(',', '.')))
            if op1 and val1 and not (op2 and val2):
                v1 = get_int(val1)
                if op1 == '>': name_parts.append(f"Over_{v1}_years")
                elif op1 == '≥': name_parts.append(f"{v1}_and_over_years")
                elif op1 == '<': name_parts.append(f"Under_{v1}_years")
                elif op1 == '≤': name_parts.append(f"Up_to_{v1}_years")
            elif op1 and val1 and op2 and val2:
                v1_f, v2_f = float(str(val1).replace(',', '.')), float(str(val2).replace(',', '.'))
                ops = sorted([(v1_f, op1), (v2_f, op2)])
                low_val, low_op = ops[0]; high_val, high_op = ops[1]
                low_bound = int(low_val) if low_op == '≥' else int(low_val + 1) if low_op == '>' else int(low_val)
                high_bound = int(high_val) if high_op == '≤' else int(high_val - 1) if high_op == '<' else int(high_val)
                if low_bound > high_bound: name_parts.append("Invalid_range")
                else: name_parts.append(f"{low_bound}_to_{high_bound}_years")
        if sex_rule:
            sex_name = sex_rule.get('name')
            if sex_name: name_parts.append(sex_name)
        return "_".join(part for part in name_parts if part)

class FilterRowUI:
    def __init__(self, parent: tk.Widget, config: Optional[Dict], delete_callback, clone_callback):
        self.frame = ttk.Frame(parent, borderwidth=1, relief="groove")
        self.delete_callback, self.clone_callback = delete_callback, clone_callback
        cfg = {'p_check': True, 'p_col': "", 'p_op1': ">", 'p_val1': "", 'p_expand': False, 'p_op_central': "OU", 'p_op2': "<", 'p_val2': "", 'c_check': False, 'c_idade_check': False, 'c_idade_op1': ">", 'c_idade_val1': "", 'c_idade_op2': "<", 'c_idade_val2': "", 'c_sexo_check': False, 'c_sexo_val': ""}
        if config: cfg.update(config)
        self.p_check_var = tk.BooleanVar(value=cfg['p_check'])
        self.p_expand_var = tk.BooleanVar(value=cfg['p_expand'])
        self.c_check_var = tk.BooleanVar(value=cfg['c_check'])
        self.c_idade_check_var = tk.BooleanVar(value=cfg['c_idade_check'])
        self.c_sexo_check_var = tk.BooleanVar(value=cfg['c_sexo_check'])
        self._create_widgets(cfg)
        self._update_secondary_frame_view()
        self.toggle_cond_frame()
        self.update_widget_states()
    def _create_widgets(self, cfg: Dict):
        main_frame = ttk.Frame(self.frame); main_frame.pack(fill=tk.X, padx=5, pady=5)
        self.p_check = ttk.Checkbutton(main_frame, variable=self.p_check_var, command=self.update_widget_states); self.p_check.grid(row=0, column=0, padx=(0, 10))
        ttk.Label(main_frame, text="Coluna:").grid(row=0, column=1)
        self.p_col_entry = ttk.Entry(main_frame, width=30); self.p_col_entry.insert(0, cfg['p_col']); self.p_col_entry.grid(row=0, column=2, padx=5)
        ops = ["", ">", "<", "=", "Não é igual a", "≥", "≤"]
        self.p_op1_combo = ttk.Combobox(main_frame, values=ops, width=12, state="readonly"); self.p_op1_combo.set(cfg['p_op1']); self.p_op1_combo.grid(row=0, column=3)
        self.p_val1_entry = ttk.Entry(main_frame, width=10); self.p_val1_entry.insert(0, cfg['p_val1']); self.p_val1_entry.grid(row=0, column=4, padx=5)
        self.btn_toggle_expand = ttk.Button(main_frame, text="+", width=2, command=self.toggle_secondary_frame); self.btn_toggle_expand.grid(row=0, column=5, padx=5)
        self.secondary_frame = ttk.Frame(main_frame); self.secondary_frame.grid(row=0, column=6, padx=5)
        self.p_op_central_combo = ttk.Combobox(self.secondary_frame, values=["E", "OU", "ENTRE"], width=6, state="readonly"); self.p_op_central_combo.set(cfg['p_op_central']); self.p_op_central_combo.pack(side=tk.LEFT)
        self.p_op2_combo = ttk.Combobox(self.secondary_frame, values=ops, width=12, state="readonly"); self.p_op2_combo.set(cfg['p_op2']); self.p_op2_combo.pack(side=tk.LEFT, padx=5)
        self.p_val2_entry = ttk.Entry(self.secondary_frame, width=10); self.p_val2_entry.insert(0, cfg['p_val2']); self.p_val2_entry.pack(side=tk.LEFT)
        self.c_check = ttk.Checkbutton(main_frame, text="Condição", variable=self.c_check_var, command=self.toggle_cond_frame); self.c_check.grid(row=0, column=7, padx=(15, 5))
        self.cond_frame = ttk.Frame(main_frame); self.cond_frame.grid(row=0, column=8)
        self.c_idade_check = ttk.Checkbutton(self.cond_frame, text="Idade", variable=self.c_idade_check_var, command=self.update_widget_states); self.c_idade_check.pack(side=tk.LEFT, padx=(0, 5))
        ops_idade = ["", "=", "<", ">", "≥", "≤"]
        self.c_idade_op1_combo = ttk.Combobox(self.cond_frame, values=ops_idade, width=3, state="readonly"); self.c_idade_op1_combo.set(cfg['c_idade_op1']); self.c_idade_op1_combo.pack(side=tk.LEFT)
        self.c_idade_val1_entry = ttk.Entry(self.cond_frame, width=5); self.c_idade_val1_entry.insert(0, cfg['c_idade_val1']); self.c_idade_val1_entry.pack(side=tk.LEFT, padx=2)
        ttk.Label(self.cond_frame, text="E").pack(side=tk.LEFT, padx=2)
        self.c_idade_op2_combo = ttk.Combobox(self.cond_frame, values=[""] + ops_idade, width=3, state="readonly"); self.c_idade_op2_combo.set(cfg['c_idade_op2']); self.c_idade_op2_combo.pack(side=tk.LEFT, padx=2)
        self.c_idade_val2_entry = ttk.Entry(self.cond_frame, width=5); self.c_idade_val2_entry.insert(0, cfg['c_idade_val2']); self.c_idade_val2_entry.pack(side=tk.LEFT, padx=2)
        self.c_sexo_check = ttk.Checkbutton(self.cond_frame, text="Sexo", variable=self.c_sexo_check_var, command=self.update_widget_states); self.c_sexo_check.pack(side=tk.LEFT, padx=(10, 5))
        self.c_sexo_val_entry = ttk.Entry(self.cond_frame, width=10); self.c_sexo_val_entry.insert(0, cfg['c_sexo_val']); self.c_sexo_val_entry.pack(side=tk.LEFT)
        action_frame = ttk.Frame(main_frame); action_frame.grid(row=0, column=9, padx=(15, 0))
        ttk.Button(action_frame, text="+ clonar", style="Clone.TButton", command=lambda: self.clone_callback(self)).pack(side=tk.LEFT)
        ttk.Button(action_frame, text="X", style="Delete.TButton", width=3, command=lambda: self.delete_callback(self)).pack(side=tk.LEFT, padx=5)
    def is_active(self) -> bool: return self.p_check_var.get()
    def set_active_state(self, is_checked: bool): self.p_check_var.set(is_checked); self.update_widget_states()
    def get_config(self) -> Dict[str, Any]: return {'p_check': self.p_check_var.get(), 'p_col': self.p_col_entry.get(), 'p_op1': self.p_op1_combo.get(), 'p_val1': self.p_val1_entry.get(), 'p_expand': self.p_expand_var.get(), 'p_op_central': self.p_op_central_combo.get(), 'p_op2': self.p_op2_combo.get(), 'p_val2': self.p_val2_entry.get(), 'c_check': self.c_check_var.get(), 'c_idade_check': self.c_idade_check_var.get(), 'c_idade_op1': self.c_idade_op1_combo.get(), 'c_idade_val1': self.c_idade_val1_entry.get(), 'c_idade_op2': self.c_idade_op2_combo.get(), 'c_idade_val2': self.c_idade_val2_entry.get(), 'c_sexo_check': self.c_sexo_check_var.get(), 'c_sexo_val': self.c_sexo_val_entry.get()}
    def toggle_secondary_frame(self): self.p_expand_var.set(not self.p_expand_var.get()); self._update_secondary_frame_view()
    def _update_secondary_frame_view(self):
        if self.p_expand_var.get(): self.secondary_frame.grid(); self.btn_toggle_expand.config(text="-")
        else: self.secondary_frame.grid_remove(); self.btn_toggle_expand.config(text="+")
        self.update_widget_states()
    def toggle_cond_frame(self):
        if self.c_check_var.get(): self.cond_frame.grid()
        else: self.cond_frame.grid_remove()
        self.update_widget_states()
    def update_widget_states(self):
        is_main_active=self.p_check_var.get(); main_state,combo_state=('normal','readonly')if is_main_active else('disabled','disabled')
        for widget in[self.p_col_entry,self.p_val1_entry,self.btn_toggle_expand,self.c_check]:widget.config(state=main_state)
        self.p_op1_combo.config(state=combo_state)
        is_expanded=self.p_expand_var.get()and is_main_active; expanded_state,expanded_combo=('normal','readonly')if is_expanded else('disabled','disabled')
        self.p_val2_entry.config(state=expanded_state);self.p_op_central_combo.config(state=expanded_combo);self.p_op2_combo.config(state=expanded_combo)
        is_cond_active=self.c_check_var.get()and is_main_active; cond_check_state='normal'if is_cond_active else'disabled'
        self.c_idade_check.config(state=cond_check_state);self.c_sexo_check.config(state=cond_check_state)
        is_idade_active=self.c_idade_check_var.get()and is_cond_active; idade_state,idade_combo=('normal','readonly')if is_idade_active else('disabled','disabled')
        self.c_idade_val1_entry.config(state=idade_state);self.c_idade_val2_entry.config(state=idade_state)
        self.c_idade_op1_combo.config(state=idade_combo);self.c_idade_op2_combo.config(state=idade_combo)
        is_sexo_active=self.c_sexo_check_var.get()and is_cond_active; sexo_state='normal'if is_sexo_active else'disabled'
        self.c_sexo_val_entry.config(state=sexo_state)

class StratumRowUI(ttk.Frame):
    def __init__(self, parent: tk.Widget, delete_callback):
        super().__init__(parent, relief="groove", borderwidth=1)
        self.delete_callback = delete_callback
        self._create_widgets()
    def _create_widgets(self):
        main_frame = ttk.Frame(self); main_frame.pack(fill=tk.X, padx=5, pady=5, expand=True)
        ttk.Label(main_frame, text="Faixa Etária:").pack(side=tk.LEFT, padx=5)
        ops = ["", ">", "<", "≥", "≤"]
        self.age_op1_combo = ttk.Combobox(main_frame, values=ops, width=3, state="readonly"); self.age_op1_combo.pack(side=tk.LEFT)
        self.age_val1_entry = ttk.Entry(main_frame, width=5); self.age_val1_entry.pack(side=tk.LEFT, padx=2)
        ttk.Label(main_frame, text="E").pack(side=tk.LEFT, padx=2)
        self.age_op2_combo = ttk.Combobox(main_frame, values=ops, width=3, state="readonly"); self.age_op2_combo.pack(side=tk.LEFT, padx=2)
        self.age_val2_entry = ttk.Entry(main_frame, width=5); self.age_val2_entry.pack(side=tk.LEFT, padx=2)
        ttk.Button(main_frame, text="X", style="Delete.TButton", width=3, command=lambda: self.delete_callback(self)).pack(side=tk.RIGHT, padx=5)
    def get_config(self) -> Dict:
        return {'op1': self.age_op1_combo.get(), 'val1': self.age_val1_entry.get(), 'op2': self.age_op2_combo.get(), 'val2': self.age_val2_entry.get()}

class FilterApp(ttk.Frame):
    def __init__(self, parent, main_app_instance, **kwargs):
        super().__init__(parent, **kwargs)
        self.main_app = main_app_instance
        self.filter_rows_ui = []; self.processing_thread = None
        self._create_main_layout(); self._create_filters_header(); self._create_action_widgets()
        filtros_para_carregar = self._load_state(); self.populate_filters(filtros_para_carregar)
        if not self.filter_rows_ui: self.add_filter_row()
    def _create_main_layout(self):
        main_frame = ttk.Frame(self, padding=10); main_frame.pack(fill=tk.BOTH, expand=True)
        filters_container = ttk.LabelFrame(main_frame, text="Regras de Exclusão", padding=10); filters_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        canvas_frame = ttk.Frame(filters_container); canvas_frame.pack(fill=tk.BOTH, expand=True)
        self.filters_canvas = tk.Canvas(canvas_frame, highlightthickness=0)
        scrollbar_v = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.filters_canvas.yview)
        self.scrollbar_h = ttk.Scrollbar(filters_container, orient="horizontal", command=self.filters_canvas.xview)
        self.filters_frame = ttk.Frame(self.filters_canvas)
        self.filters_canvas.configure(yscrollcommand=scrollbar_v.set, xscrollcommand=self.scrollbar_h.set)
        scrollbar_v.pack(side="right", fill="y"); self.filters_canvas.pack(side="left", fill="both", expand=True)
        self.filters_canvas.create_window((0, 0), window=self.filters_frame, anchor="nw")
        def update_scroll_logic(event=None):
            bbox = self.filters_canvas.bbox("all")
            self.filters_canvas.configure(scrollregion=bbox)
            content_width = bbox[2] if bbox else 0
            canvas_width = self.filters_canvas.winfo_width()
            if content_width > canvas_width:
                if not self.scrollbar_h.winfo_ismapped(): self.scrollbar_h.pack(side="bottom", fill="x")
            else:
                if self.scrollbar_h.winfo_ismapped(): self.scrollbar_h.pack_forget()
        self.filters_frame.bind("<Configure>", update_scroll_logic)
        self.filters_canvas.bind("<Configure>", update_scroll_logic)
        self.bind_class("TCombobox", "<MouseWheel>", lambda e: "break")
        self.master.bind_all("<MouseWheel>", self._on_mousewheel, add="+")
        self.action_frame = ttk.Frame(main_frame); self.action_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=10)
    def _create_filters_header(self):
        header_frame = ttk.Frame(self.filters_frame); header_frame.pack(fill=tk.X, pady=(0, 5))
        self.check_all_var = tk.BooleanVar(value=True)
        check_all = ttk.Checkbutton(header_frame, text="Marcar/Desmarcar Todas as Regras", variable=self.check_all_var, command=self.toggle_all_filters)
        check_all.pack(side=tk.LEFT, padx=5)
    def _create_action_widgets(self):
        self.btn_gerar = ttk.Button(self.action_frame, text="Gerar Planilha Filtrada", command=self.start_processing)
        self.btn_gerar.pack(side=tk.RIGHT, padx=5)
    def _on_mousewheel(self, event):
        grab_current = self.winfo_toplevel().grab_current()
        if grab_current: return
        if not self.winfo_viewable(): return
        active_tab_text = self.main_app.notebook.tab(self.main_app.notebook.select(), "text")
        if "Filtro" not in active_tab_text: return
        view_info = self.filters_canvas.yview()
        is_at_top, is_at_bottom = view_info[0] <= 0.0, view_info[1] >= 1.0
        if sys.platform == "win32":
            if (event.delta > 0 and is_at_top) or (event.delta < 0 and is_at_bottom): return
            self.filters_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        else:
            if (event.num == 4 and is_at_top) or (event.num == 5 and is_at_bottom): return
            if event.num == 4: self.filters_canvas.yview_scroll(-1, "units")
            elif event.num == 5: self.filters_canvas.yview_scroll(1, "units")
    def start_processing(self):
        if self.main_app.is_processing: messagebox.showwarning("Processo em Andamento", "Outro processo já está em execução. Por favor, aguarde a sua conclusão."); return
        self.main_app.is_processing = True
        global_config = self.main_app.get_global_config()
        if not global_config['input_path']: messagebox.showerror("Erro", "Selecione um arquivo de entrada."); self.main_app.is_processing = False; return
        if not global_config['output_path']: messagebox.showerror("Erro", "Selecione uma pasta de saída."); self.main_app.is_processing = False; return
        filters_config = [row.get_config() for row in self.filter_rows_ui if row.is_active()]
        if not filters_config: messagebox.showwarning("Aviso", "Nenhuma regra de exclusão está ativa.")
        self.btn_gerar.config(state="disabled"); self.main_app.cancel_event.clear(); self.main_app.show_progress_bar()
        timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
        formato_escolhido = global_config['output_format']
        extensao = '.xlsx' if 'Excel' in formato_escolhido else '.csv'
        output_filename = f"Planilha_Filtrada_{timestamp}{extensao}"
        output_file_path = os.path.join(global_config['output_path'], output_filename)
        processor = DataProcessor(self.main_app.progress_queue)
        self.processing_thread = threading.Thread(target=processor.apply_filters, args=(global_config['input_path'], output_file_path, filters_config, global_config, formato_escolhido, self.main_app.cancel_event))
        self.processing_thread.daemon = True; self.processing_thread.start()
        self.main_app.monitor_progress(self.btn_gerar)
    def add_filter_row(self, config:Optional[Dict]=None, insert_after_index:Optional[int]=None):
        if hasattr(self, 'btn_add'): self.btn_add.pack_forget()
        row_ui = FilterRowUI(self.filters_frame, config, self.delete_filter_row, self.clone_filter_row)
        if insert_after_index is not None and insert_after_index < len(self.filter_rows_ui):
            self.filter_rows_ui.insert(insert_after_index + 1, row_ui)
            anchor_widget = self.filter_rows_ui[insert_after_index].frame
            row_ui.frame.pack(fill=tk.X, padx=5, pady=2, expand=True, after=anchor_widget)
        else:
            self.filter_rows_ui.append(row_ui); row_ui.frame.pack(fill=tk.X, padx=5, pady=2, expand=True)
        self.btn_add = ttk.Button(self.filters_frame, text="+ Adicionar Nova Regra", command=self.add_filter_row, style="Add.TButton")
        self.btn_add.pack(pady=10, anchor="w", padx=5)
    def delete_filter_row(self, row_to_delete:'FilterRowUI'):
        if len(self.filter_rows_ui) <= 1: messagebox.showwarning("Aviso", "Não é possível excluir a última regra."); return
        row_to_delete.frame.destroy(); self.filter_rows_ui.remove(row_to_delete)
    def clone_filter_row(self, row_to_clone:'FilterRowUI'):
        try:
            clone_index = self.filter_rows_ui.index(row_to_clone); config = row_to_clone.get_config()
            self.add_filter_row(config=config, insert_after_index=clone_index)
        except ValueError: self.add_filter_row(config=row_to_clone.get_config())
    def populate_filters(self, filter_list:List[Dict]):
        for row in self.filter_rows_ui: row.frame.destroy()
        self.filter_rows_ui.clear()
        for f_config in filter_list: self.add_filter_row(config=f_config)
    def toggle_all_filters(self):
        is_checked = self.check_all_var.get()
        for row_ui in self.filter_rows_ui: row_ui.set_active_state(is_checked)
    def on_closing(self):
        self._save_state()
        if self.processing_thread and self.processing_thread.is_alive(): return messagebox.askokcancel("Sair?", "O processo de filtragem ainda está em execução. Deseja mesmo sair?")
        return True
    def _save_state(self):
        current_state = [row.get_config() for row in self.filter_rows_ui if row.get_config()['p_col']]
        try:
            with open(CONFIG_FILE_FILTERS, "w", encoding='utf-8') as f: json.dump(current_state, f, indent=4, ensure_ascii=False)
        except Exception as e: print(f"Erro ao salvar o estado dos filtros: {e}")
    def _load_state(self) -> List[Dict]:
        if os.path.exists(CONFIG_FILE_FILTERS):
            try:
                with open(CONFIG_FILE_FILTERS, "r", encoding='utf-8') as f: return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Erro ao carregar configuração de filtros: {e}"); return DEFAULT_FILTERS
        return DEFAULT_FILTERS

class StratificationApp(ttk.Frame):
    def __init__(self, parent, main_app_instance, **kwargs):
        super().__init__(parent, **kwargs)
        self.main_app = main_app_instance
        self.stratum_rows_ui = []; self.processing_thread = None
        self._create_main_layout(); self.add_stratum_row()
    def _create_main_layout(self):
        main_frame = ttk.Frame(self, padding=10); main_frame.pack(fill=tk.BOTH, expand=True)
        options_frame = ttk.LabelFrame(main_frame, text="Opções de Estratificação", padding=10); options_frame.pack(fill=tk.X, padx=5, pady=5)
        sex_check_frame = ttk.Frame(options_frame); sex_check_frame.pack(anchor='w')
        ttk.Label(sex_check_frame, text="Estratificar por sexo:").pack(side=tk.LEFT, padx=(0, 10))
        self.stratify_male_var = tk.BooleanVar(value=True)
        self.stratify_female_var = tk.BooleanVar(value=True)
        male_check = ttk.Checkbutton(sex_check_frame, text="Masculino", variable=self.stratify_male_var); male_check.pack(side=tk.LEFT, padx=5)
        female_check = ttk.Checkbutton(sex_check_frame, text="Feminino", variable=self.stratify_female_var); female_check.pack(side=tk.LEFT, padx=5)
        strata_container = ttk.LabelFrame(main_frame, text="Definição das Faixas Etárias", padding=10); strata_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        canvas_frame = ttk.Frame(strata_container); canvas_frame.pack(fill=tk.BOTH, expand=True)
        self.strata_canvas = tk.Canvas(canvas_frame, highlightthickness=0)
        scrollbar_v = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.strata_canvas.yview)
        self.scrollbar_h = ttk.Scrollbar(strata_container, orient="horizontal", command=self.strata_canvas.xview)
        self.strata_frame = ttk.Frame(self.strata_canvas)
        self.strata_canvas.configure(yscrollcommand=scrollbar_v.set, xscrollcommand=self.scrollbar_h.set)
        scrollbar_v.pack(side="right", fill="y"); self.strata_canvas.pack(side="left", fill="both", expand=True)
        self.strata_canvas.create_window((0, 0), window=self.strata_frame, anchor="nw")
        def update_scroll_logic(event=None):
            bbox = self.strata_canvas.bbox("all")
            self.strata_canvas.configure(scrollregion=bbox)
            content_width = bbox[2] if bbox else 0; canvas_width = self.strata_canvas.winfo_width()
            if content_width > canvas_width:
                if not self.scrollbar_h.winfo_ismapped(): self.scrollbar_h.pack(side="bottom", fill="x")
            else:
                if self.scrollbar_h.winfo_ismapped(): self.scrollbar_h.pack_forget()
        self.strata_frame.bind("<Configure>", update_scroll_logic)
        self.strata_canvas.bind("<Configure>", update_scroll_logic)
        action_frame = ttk.Frame(main_frame); action_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=10)
        self.btn_gerar = ttk.Button(action_frame, text="Gerar Planilhas Estratificadas", command=self.start_processing)
        self.btn_gerar.pack(side=tk.RIGHT, padx=5)
    def add_stratum_row(self):
        if hasattr(self, 'btn_add'): self.btn_add.pack_forget()
        row_ui = StratumRowUI(self.strata_frame, self.delete_stratum_row)
        self.stratum_rows_ui.append(row_ui)
        row_ui.pack(fill=tk.X, padx=5, pady=2, expand=True)
        self.btn_add = ttk.Button(self.strata_frame, text="+ Adicionar Faixa Etária", command=self.add_stratum_row, style="Add.TButton")
        self.btn_add.pack(pady=10, anchor="w", padx=5)
    def delete_stratum_row(self, row_to_delete):
        if len(self.stratum_rows_ui) <= 1 and not (self.stratify_male_var.get() or self.stratify_female_var.get()):
             messagebox.showwarning("Aviso", "Não é possível excluir a última faixa etária se nenhuma estratificação por sexo estiver selecionada.")
             return
        row_to_delete.destroy(); self.stratum_rows_ui.remove(row_to_delete)
    def start_processing(self):
        if self.main_app.is_processing: messagebox.showwarning("Processo em Andamento", "Outro processo já está em execução. Por favor, aguarde a sua conclusão."); return
        if not messagebox.askyesno("Confirmação de Estratificação", "Você confirma que a planilha selecionada é a versão FILTRADA?\n\nRecomenda-se usar a planilha gerada pela \"Ferramenta de Filtro\" para garantir a consistência dos dados."): return
        self.main_app.is_processing = True
        global_config = self.main_app.get_global_config()
        if not global_config['input_path']: messagebox.showerror("Erro", "Selecione um arquivo de entrada."); self.main_app.is_processing = False; return
        if not global_config['output_path']: messagebox.showerror("Erro", "Selecione uma pasta de saída."); self.main_app.is_processing = False; return
        age_rules = [row.get_config() for row in self.stratum_rows_ui if row.get_config().get('val1')]
        sex_rules = []
        if self.stratify_male_var.get():
            val_m = global_config.get('val_masculino')
            if not val_m: messagebox.showerror("Erro", "Defina o valor para 'masculino' nas Configurações Globais."); self.main_app.is_processing = False; return
            sex_rules.append({'value': val_m, 'name': 'Male'})
        if self.stratify_female_var.get():
            val_f = global_config.get('val_feminino')
            if not val_f: messagebox.showerror("Erro", "Defina o valor para 'feminino' nas Configurações Globais."); self.main_app.is_processing = False; return
            sex_rules.append({'value': val_f, 'name': 'Female'})
        strata_config = {'ages': age_rules, 'sexes': sex_rules}
        self.btn_gerar.config(state="disabled"); self.main_app.cancel_event.clear(); self.main_app.show_progress_bar()
        processor = DataProcessor(self.main_app.progress_queue)
        self.processing_thread = threading.Thread(target=processor.apply_stratification, args=(global_config['input_path'], global_config['output_path'], strata_config, global_config, global_config['output_format'], self.main_app.cancel_event))
        self.processing_thread.daemon = True; self.processing_thread.start()
        self.main_app.monitor_progress(self.btn_gerar)
    def on_closing(self):
        if self.processing_thread and self.processing_thread.is_alive():
            return messagebox.askokcancel("Sair?", "O processo de estratificação ainda está em execução. Deseja mesmo sair?")
        return True

class ManualWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Manual do Usuário")
        self.transient(parent)
        self.grab_set()
        parent.update_idletasks()
        win_w, win_h = 900, 600
        parent_x, parent_y = parent.winfo_x(), parent.winfo_y()
        parent_w, parent_h = parent.winfo_width(), parent.winfo_height()
        pos_x = parent_x + (parent_w // 2) - (win_w // 2)
        pos_y = parent_y + (parent_h // 2) - (win_h // 2)
        self.geometry(f"{win_w}x{win_h}+{pos_x}+{pos_y}")
        self._create_widgets()
        self._populate_menu()
    def _create_widgets(self):
        main_panel = ttk.Frame(self)
        main_panel.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        menu_frame = ttk.Frame(main_panel, width=250)
        menu_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        menu_frame.pack_propagate(False)
        self.listbox = tk.Listbox(menu_frame, selectmode=tk.SINGLE, exportselection=False, background="#f0f0f0", borderwidth=0, highlightthickness=0)
        self.listbox.pack(fill=tk.BOTH, expand=True)
        self.listbox.bind("<<ListboxSelect>>", self._on_topic_select)
        content_frame = ttk.Frame(main_panel)
        content_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.text_content = tk.Text(content_frame, wrap=tk.WORD, padx=10, pady=10, relief="sunken", borderwidth=1)
        scrollbar = ttk.Scrollbar(content_frame, orient=tk.VERTICAL, command=self.text_content.yview)
        self.text_content.config(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.text_content.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.text_content.tag_configure("bold", font=("Helvetica", 10, "bold"))
        self.text_content.tag_configure("h1", font=("Helvetica", 14, "bold"), spacing3=10)
    def _populate_menu(self):
        for topic in MANUAL_CONTENT.keys():
            self.listbox.insert(tk.END, topic)
        self.listbox.select_set(0)
        self.after(50, self._on_topic_select)
    def _on_topic_select(self, event=None):
        selection_indices = self.listbox.curselection()
        if not selection_indices: return
        selected_topic = self.listbox.get(selection_indices[0])
        content = MANUAL_CONTENT.get(selected_topic, "Tópico não encontrado.")
        self.text_content.config(state="normal")
        self.text_content.delete("1.0", tk.END)
        content = content.strip()
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('**') and line.endswith('**'):
                self.text_content.insert(tk.END, line[2:-2] + "\n", "h1")
            else:
                parts = re.split(r'(\*\*.*?\*\*)', line)
                for part in parts:
                    if part.startswith("**") and part.endswith("**"): self.text_content.insert(tk.END, part[2:-2], "bold")
                    else: self.text_content.insert(tk.END, part)
                self.text_content.insert(tk.END, "\n")
        self.text_content.config(state="disabled")

class MainApp(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title("Ferramenta de Análise de Planilhas v11.8")
        self.geometry("1300x800")
        self.caminho_entrada = ""; self.caminho_pasta_saida = ""
        self.progress_queue = queue.Queue()
        self.cancel_event = threading.Event()
        self.is_processing = False
        self.manual_win = None
        self.help_icon = None
        self._configure_styles()
        self._create_main_layout()
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
    def _configure_styles(self):
        style = ttk.Style(self); style.theme_use('clam')
        style.configure("TProgressbar", thickness=20)
        style.configure("Delete.TButton", foreground="red", font=('Helvetica', '10', 'bold'))
        style.configure("Clone.TButton", foreground="blue"); style.configure("Add.TButton", foreground="green")
        style.configure("Cancel.TButton", foreground="darkred")
        style.configure("Toolbutton", padding=0, relief="flat", background=style.lookup("TFrame", "background"))
    def _create_main_layout(self):
        main_frame = ttk.Frame(self, padding=10); main_frame.pack(fill=tk.BOTH, expand=True)
        try:
            original_image = Image.open("ajuda.png")
            resized_image = original_image.resize((24, 24), Image.Resampling.LANCZOS)
            self.help_icon = ImageTk.PhotoImage(resized_image)
            help_button = ttk.Button(main_frame, image=self.help_icon, command=self.show_manual_window, style="Toolbutton")
            help_button.place(relx=1.0, rely=0, x=-10, y=10, anchor='ne')
        except Exception as e:
            print(f"Não foi possível carregar o ícone de ajuda: {e}")
            help_button = ttk.Button(main_frame, text="?", command=self.show_manual_window, width=3)
            help_button.place(relx=1.0, rely=0, x=-10, y=10, anchor='ne')
        global_frame = ttk.LabelFrame(main_frame, text="1. Configurações Globais", padding=10); global_frame.pack(fill=tk.X, padx=5, pady=(43, 5))
        self._create_global_widgets(global_frame)
        self.notebook = ttk.Notebook(main_frame); self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=(10,0))
        self.filter_app_frame = FilterApp(self.notebook, self)
        self.stratification_app_frame = StratificationApp(self.notebook, self)
        self.notebook.add(self.filter_app_frame, text="2. Ferramenta de Filtro")
        self.notebook.add(self.stratification_app_frame, text="3. Ferramenta de Estratificação")
        self.action_frame = ttk.Frame(main_frame); self.action_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=(10,0))
        self.progress_frame = ttk.Frame(self.action_frame)
        self.progress_bar = ttk.Progressbar(self.progress_frame, mode='determinate', length=400); self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.progress_label = ttk.Label(self.progress_frame, text="Pronto", width=70, anchor='w'); self.progress_label.pack(side=tk.LEFT, padx=10)
        self.cancel_button = ttk.Button(self.action_frame, text="Cancelar", command=self.on_cancel_clicked, style="Cancel.TButton")
    def _create_global_widgets(self, parent):
        parent.columnconfigure(1, weight=1); parent.columnconfigure(3, weight=1)
        btn_entrada = ttk.Button(parent, text="Selecionar Planilha...", command=self.selecionar_entrada); btn_entrada.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        self.lbl_entrada = ttk.Label(parent, text="Nenhum arquivo selecionado.", foreground="gray", anchor='w'); self.lbl_entrada.grid(row=0, column=1, columnspan=5, padx=5, pady=5, sticky="ew")
        btn_saida = ttk.Button(parent, text="Selecionar Pasta de Saída...", command=self.selecionar_pasta_saida); btn_saida.grid(row=1, column=0, padx=5, pady=5, sticky="ew")
        self.lbl_saida = ttk.Label(parent, text="Nenhuma pasta selecionada.", foreground="gray", anchor='w'); self.lbl_saida.grid(row=1, column=1, columnspan=5, padx=5, pady=5, sticky="ew")
        ttk.Label(parent, text="Coluna Idade:").grid(row=2, column=0, padx=5, pady=5, sticky='e')
        self.entry_col_idade = ttk.Entry(parent, width=20); self.entry_col_idade.insert(0, "Idade"); self.entry_col_idade.grid(row=2, column=1, padx=5, pady=5, sticky='w')
        ttk.Label(parent, text="Coluna Sexo:").grid(row=2, column=2, padx=(10, 5), pady=5, sticky='e')
        self.entry_col_sexo = ttk.Entry(parent, width=20); self.entry_col_sexo.insert(0, "Sexo"); self.entry_col_sexo.grid(row=2, column=3, padx=5, pady=5, sticky='w')
        ttk.Label(parent, text="Formato de Saída:").grid(row=2, column=4, padx=(15, 5), pady=5, sticky='e')
        self.combo_formato_saida = ttk.Combobox(parent, values=["Excel (.xlsx)", "CSV (.csv)"], width=17, state="readonly"); self.combo_formato_saida.set("CSV (.csv)"); self.combo_formato_saida.grid(row=2, column=5, padx=5, pady=5, sticky='w')
        ttk.Label(parent, text="Valor para masculino:").grid(row=3, column=0, padx=5, pady=5, sticky='e')
        self.entry_val_masculino = ttk.Entry(parent, width=20); self.entry_val_masculino.insert(0, "Masculino"); self.entry_val_masculino.grid(row=3, column=1, padx=5, pady=5, sticky='w')
        ttk.Label(parent, text="Valor para feminino:").grid(row=3, column=2, padx=(10, 5), pady=5, sticky='e')
        self.entry_val_feminino = ttk.Entry(parent, width=20); self.entry_val_feminino.insert(0, "Feminino"); self.entry_val_feminino.grid(row=3, column=3, padx=5, pady=5, sticky='w')
    def show_manual_window(self):
        if self.manual_win and self.manual_win.winfo_exists():
            self.manual_win.lift()
        else:
            self.manual_win = ManualWindow(self)
    def on_cancel_clicked(self):
        self.cancel_event.set()
        self.cancel_button.config(state="disabled")
        self.progress_label.config(text="Cancelando... Aguarde a finalização da etapa atual.")
    def selecionar_entrada(self):
        path = filedialog.askopenfilename(title="Selecione uma planilha", filetypes=[("Planilhas Suportadas", "*.xlsx *.xls *.csv"), ("Todos os arquivos", "*.*")])
        if path:
            self.caminho_entrada = path; self.lbl_entrada.config(text=os.path.basename(path), foreground="black")
            if not self.caminho_pasta_saida: self.caminho_pasta_saida = os.path.dirname(path); self.lbl_saida.config(text=self.caminho_pasta_saida, foreground="black")
    def selecionar_pasta_saida(self):
        path = filedialog.askdirectory(title="Selecione uma pasta para salvar o arquivo")
        if path: self.caminho_pasta_saida = path; self.lbl_saida.config(text=path, foreground="black")
    def get_global_config(self) -> Dict:
        return {'input_path': self.caminho_entrada, 'output_path': self.caminho_pasta_saida, 'coluna_idade': self.entry_col_idade.get(), 'coluna_sexo': self.entry_col_sexo.get(), 'val_masculino': self.entry_val_masculino.get(), 'val_feminino': self.entry_val_feminino.get(), 'output_format': self.combo_formato_saida.get()}
    def show_progress_bar(self):
        self.progress_bar['value'] = 0; self.progress_label.config(text="Iniciando...")
        self.progress_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.cancel_button.config(state="normal"); self.cancel_button.pack(side=tk.RIGHT, padx=(0, 5))
    def hide_progress_bar(self, button_to_reactivate):
        self.progress_frame.pack_forget(); self.cancel_button.pack_forget()
        if button_to_reactivate: button_to_reactivate.config(state="normal")
    def monitor_progress(self, button_to_reactivate):
        try:
            message = self.progress_queue.get_nowait()
            if isinstance(message, tuple):
                if message[0] == 'DONE': self.on_processing_complete(button_to_reactivate, message[1]); return 
                elif message[0] == 'ERROR': self.on_processing_error(button_to_reactivate, message[1]); return
                elif message[0] == 'CANCELLED': self.on_processing_cancelled(button_to_reactivate, message[1]); return
                else: percent, msg = message; self.progress_bar['value'] = percent; self.progress_label.config(text=msg)
            self.after(100, lambda: self.monitor_progress(button_to_reactivate))
        except queue.Empty:
            active_threads = [self.filter_app_frame.processing_thread, self.stratification_app_frame.processing_thread]
            if any(t and t.is_alive() for t in active_threads): self.after(100, lambda: self.monitor_progress(button_to_reactivate))
    def on_processing_complete(self, button, result_tuple):
        self.is_processing = False
        self.hide_progress_bar(button)
        msg, total, generated, path = result_tuple
        if "FILTRAGEM" in msg:
            excluidas = total - generated
            messagebox.showinfo("Sucesso!", f"{msg}\n\nLinhas Originais: {total}\nLinhas Excluídas: {excluidas}\nLinhas Restantes: {generated}\n\nArquivo salvo em:\n{path}")
        else:
            messagebox.showinfo("Sucesso!", f"{msg}\n\nTotal de Combinações: {total}\nPlanilhas Geradas: {generated}\n\nArquivos salvos na pasta:\n{path}")
    def on_processing_cancelled(self, button, msg):
        self.is_processing = False
        self.hide_progress_bar(button)
        messagebox.showinfo("Operação Cancelada", msg)
    def on_processing_error(self, button, error):
        self.is_processing = False
        self.hide_progress_bar(button)
        messagebox.showerror("Erro no Processamento", str(error))
    def on_closing(self):
        if self.filter_app_frame.on_closing() and self.stratification_app_frame.on_closing(): self.destroy()

def launch_main_app():
    app = MainApp(); app.mainloop()

if __name__ == "__main__":
    lgpd_root = tk.Tk()
    lgpd_root.title("Termos de Uso e Conformidade com a LGPD")
    window_width, window_height = 600, 500
    screen_width, screen_height = lgpd_root.winfo_screenwidth(), lgpd_root.winfo_screenheight()
    center_x, center_y = int(screen_width/2 - window_width/2), int(screen_height/2 - window_height/2)
    lgpd_root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    lgpd_root.resizable(False, False); lgpd_root.protocol("WM_DELETE_WINDOW", lgpd_root.destroy)
    main_frame = ttk.Frame(lgpd_root, padding=20); main_frame.pack(fill=tk.BOTH, expand=True)
    title_label = ttk.Label(main_frame, text="Aviso sobre a Lei Geral de Proteção de Dados (LGPD)", font=("Helvetica", 12, "bold")); title_label.pack(pady=(0, 15))
    texto_lgpd = (
        "Esta ferramenta foi projetada para processar e filtrar dados de planilhas. "
        "É possível que os arquivos carregados por você contenham dados pessoais sensíveis "
        "(como nome completo, data de nascimento, CPF, informações de saúde, etc.), cujo tratamento é regulado pela "
        "Lei Geral de Proteção de Dados (LGPD - Lei nº 13.709/2018).\n\n"
        "É de sua inteira responsabilidade garantir que todos os dados utilizados nesta ferramenta estejam em "
        "conformidade com a LGPD. Recomendamos fortemente que você utilize apenas dados previamente "
        "anonimizados para proteger a privacidade dos titulares dos dados.\n\n"
        "A responsabilidade sobre a natureza dos dados processados é exclusivamente sua.\n\n"
        "Para prosseguir, você deve confirmar que os dados a serem utilizados foram devidamente tratados e anonimizados."
    )
    text_widget = tk.Text(main_frame, wrap=tk.WORD, height=16, width=70, relief="flat", background=lgpd_root.cget('bg')); text_widget.insert(tk.END, texto_lgpd)
    text_widget.config(state="disabled"); text_widget.pack(fill=tk.X, expand=True, pady=(0, 20))
    check_var = tk.BooleanVar()
    def toggle_button_state(*args):
        if check_var.get(): continue_button.config(state="normal")
        else: continue_button.config(state="disabled")
    check_var.trace("w", toggle_button_state)
    check_frame = ttk.Frame(main_frame); check_frame.pack(pady=(0, 15), fill=tk.X)
    check_button = ttk.Checkbutton(check_frame, variable=check_var); check_button.pack(side=tk.LEFT, anchor='nw', padx=(0, 5))
    check_text = "Ao confirmar, garanto que os dados inseridos estão anonimizados e que não há presença de dados sensíveis."
    check_label = ttk.Label(check_frame, text=check_text, wraplength=500, justify=tk.LEFT); check_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
    def toggle_check_on_label_click(event): check_var.set(not check_var.get())
    check_label.bind("<Button-1>", toggle_check_on_label_click)
    def on_continue(): lgpd_root.destroy(); launch_main_app()
    continue_button = ttk.Button(main_frame, text="Continuar", command=on_continue, state="disabled"); continue_button.pack()
    lgpd_root.mainloop()