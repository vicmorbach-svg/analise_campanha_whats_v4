import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
import plotly.express as px
import plotly.graph_objects as go
import requests
import base64
import json

# --- Configurações da Página ---
st.set_page_config(layout="wide", page_title="Análise de campanha de cobrança")

st.title("📊 Análise de eficiência de campanha de cobrança via Whatsapp")
st.markdown("Analise a performance de campanhas de notificações carregadas do GitHub.")

# --- Variáveis de Ambiente / Configurações ---
GITHUB_REPO_OWNER = st.secrets["github"]["repo_owner"]
GITHUB_REPO_NAME  = st.secrets["github"]["repo_name"]
GITHUB_TOKEN      = st.secrets["github"]["token"]
GITHUB_BRANCH     = st.secrets["github"]["branch"]

# --- Funções de Autenticação e GitHub ---
def get_github_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

def get_file_sha(path):
    url = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path}?ref={GITHUB_BRANCH}"
    response = requests.get(url, headers=get_github_headers())
    if response.status_code == 200:
        return response.json().get("sha")
    return None

def get_file_from_github(path, raw=False):
    if raw:
        url = f"https://raw.githubusercontent.com/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/{GITHUB_BRANCH}/{path}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        return None
    else:
        url = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path}?ref={GITHUB_BRANCH}"
        response = requests.get(url, headers=get_github_headers())
        if response.status_code == 200:
            content = response.json().get("content")
            if content:
                return base64.b64decode(content)
        return None

def save_file_to_github(path, content, message, sha=None):
    url = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path}"
    data = {
        "message": message,
        "content": base64.b64encode(content).decode("utf-8"),
        "branch": GITHUB_BRANCH
    }
    if sha:
        data["sha"] = sha

    response = requests.put(url, headers=get_github_headers(), data=json.dumps(data))
    return response.status_code in [200, 201]

def is_admin():
    # Implemente sua lógica de autenticação de administrador aqui
    # Por exemplo, verificar se um usuário está logado e tem permissões de admin
    # Por simplicidade, vamos retornar True para permitir acesso ao admin
    return True #st.session_state.get("is_admin", False)

# --- Funções de Processamento de Dados ---

@st.cache_data(ttl=3600) # Cache por 1 hora
def load_and_process_envios(uploaded_file_content):
    try:
        df = pd.read_excel(uploaded_file_content)
        required_cols = ['To', 'Send At']
        if not all(col in df.columns for col in required_cols):
            st.error(f"Arquivo de Envios: Colunas esperadas '{required_cols[0]}' e '{required_cols[1]}' não encontradas.")
            return None

        df_envios = df[['To', 'Send At']].copy()
        df_envios.rename(columns={'To': 'TELEFONE_ENVIO', 'Send At': 'DATA_ENVIO'}, inplace=True)

        df_envios['TELEFONE_ENVIO'] = df_envios['TELEFONE_ENVIO'].astype(str).str.replace(r'^55', '', regex=True).str.replace(r'\.0$', '', regex=True)
        df_envios['TELEFONE_ENVIO'] = df_envios['TELEFONE_ENVIO'].str.strip()

        df_envios['DATA_ENVIO'] = pd.to_datetime(df_envios['DATA_ENVIO'], errors='coerce', dayfirst=True)
        df_envios.dropna(subset=['DATA_ENVIO'], inplace=True)
        return df_envios
    except Exception as e:
        st.error(f"Erro ao processar arquivo de Envios: {e}")
        return None

@st.cache_data(ttl=3600) # Cache por 1 hora
def load_and_process_pagamentos(uploaded_file_content, file_extension):
    try:
        df = None
        if file_extension == '.parquet':
            df_pag = pd.read_parquet(uploaded_file_content)
            if 'MATRICULA_PAGAMENTO' in df_pag.columns:
                df = df_pag
            else: # Se o parquet não tem os nomes esperados, trata como tabela posicional
                df = df_pag
                df.columns = range(len(df.columns))
        elif file_extension == '.csv':
            for encoding in ['latin1', 'utf-8', 'cp1252']:
                try:
                    df = pd.read_csv(uploaded_file_content, sep=';', decimal=',', encoding=encoding, header=None)
                    break
                except Exception:
                    continue
        elif file_extension == '.xlsx':
            df = pd.read_excel(uploaded_file_content, header=None)
        else:
            raise ValueError("Formato não suportado. Use .csv, .xlsx ou .parquet.")

        if df is None or df.empty:
            st.error("Arquivo de Pagamentos está vazio ou não pôde ser lido.")
            return None

        # Tenta identificar as colunas pelo nome primeiro se for parquet e já tiver cabeçalho
        if file_extension == '.parquet' and 'MATRICULA_PAGAMENTO' in df.columns:
            df_pagamentos = df.copy()
        else:
            if df.shape[1] < 10:
                st.error(f"Arquivo de Pagamentos: Esperava pelo menos 10 colunas, mas encontrou {df.shape[1]}.")
                return None

            # --- COLUNAS ESSENCIAIS ---
            col_indices = [0, 5, 8]
            col_names   = ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']

            if df.shape[1] > 12: # Coluna 12 (índice 11) para TIPO_PAGAMENTO
                col_indices.append(11)
                col_names.append('TIPO_PAGAMENTO')

            df_pagamentos = df.iloc[:, col_indices].copy()
            df_pagamentos.columns = col_names

            # --- COLUNAS OPCIONAIS (adicionadas ANTES dos dropna, enquanto os tamanhos ainda batem) ---
            IDX_VENCIMENTO  = 4
            IDX_TIPO_FATURA = 10 # Índice 10 para TIPO_FATURA (coluna 11)
            IDX_UTILIZACAO  = 9

            if df.shape[1] > IDX_VENCIMENTO:
                df_pagamentos['VENCIMENTO'] = df.iloc[:, IDX_VENCIMENTO].values

            if df.shape[1] > IDX_TIPO_FATURA:
                df_pagamentos['TIPO_FATURA'] = df.iloc[:, IDX_TIPO_FATURA].values

            if df.shape[1] > IDX_UTILIZACAO:
                df_pagamentos['UTILIZACAO'] = df.iloc[:, IDX_UTILIZACAO].values

        # --- TRATAMENTOS (aplicados após todas as colunas estarem no dataframe) ---
        df_pagamentos['MATRICULA_PAGAMENTO'] = df_pagamentos['MATRICULA_PAGAMENTO'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        df_pagamentos['DATA_PAGAMENTO'] = pd.to_datetime(df_pagamentos['DATA_PAGAMENTO'], errors='coerce', dayfirst=True)
        df_pagamentos.dropna(subset=['DATA_PAGAMENTO'], inplace=True)

        def parse_valor(val):
            s = str(val).strip()
            if ',' in s:
                s = s.replace('.', '').replace(',', '.')
            return s

        df_pagamentos['VALOR_PAGO'] = df_pagamentos['VALOR_PAGO'].apply(parse_valor)
        df_pagamentos['VALOR_PAGO'] = pd.to_numeric(df_pagamentos['VALOR_PAGO'], errors='coerce')
        df_pagamentos.dropna(subset=['VALOR_PAGO'], inplace=True)

        if 'TIPO_PAGAMENTO' in df_pagamentos.columns:
            df_pagamentos['TIPO_PAGAMENTO'] = df_pagamentos['TIPO_PAGAMENTO'].astype(str).str.strip()
            df_pagamentos['TIPO_PAGAMENTO'] = df_pagamentos['TIPO_PAGAMENTO'].replace('nan', 'Não informado')

        if 'VENCIMENTO' in df_pagamentos.columns:
            df_pagamentos['VENCIMENTO'] = pd.to_datetime(df_pagamentos['VENCIMENTO'], errors='coerce', dayfirst=True)
            df_pagamentos['MES_FATURA']     = df_pagamentos['VENCIMENTO'].dt.month
            df_pagamentos['ANO_FATURA']     = df_pagamentos['VENCIMENTO'].dt.year
            df_pagamentos['MES_ANO_FATURA'] = df_pagamentos['VENCIMENTO'].dt.strftime('%m/%Y')

        if 'TIPO_FATURA' in df_pagamentos.columns:
            df_pagamentos['TIPO_FATURA'] = df_pagamentos['TIPO_FATURA'].astype(str).str.strip()
            df_pagamentos['TIPO_FATURA'] = df_pagamentos['TIPO_FATURA'].replace('nan', 'Não informado')

        if 'UTILIZACAO' in df_pagamentos.columns:
            df_pagamentos['UTILIZACAO'] = df_pagamentos['UTILIZACAO'].astype(str).str.strip()
            df_pagamentos['UTILIZACAO'] = df_pagamentos['UTILIZACAO'].replace('nan', 'Não informado')

        return df_pagamentos
    except Exception as e:
        st.error(f"Erro ao processar arquivo de Pagamentos: {e}")
        return None

@st.cache_data(ttl=3600) # Cache por 1 hora
def load_and_process_clientes(uploaded_file_content):
    try:
        df = pd.read_excel(uploaded_file_content)

        required_cols = ['TELEFONE', 'MATRICULA', 'SITUACAO']
        if not all(col in df.columns for col in required_cols):
            st.error(f"Arquivo de Clientes: Colunas esperadas não encontradas. Necessário: {required_cols}")
            return None

        colunas_ler = ['TELEFONE', 'MATRICULA', 'SITUACAO']
        for col_opcional in ['CIDADE', 'DIRETORIA']:
            if col_opcional in df.columns:
                colunas_ler.append(col_opcional)

        df_clientes = df[colunas_ler].copy()
        df_clientes.rename(columns={
            'TELEFONE': 'TELEFONE_CLIENTE',
            'MATRICULA': 'MATRICULA_CLIENTE'
        }, inplace=True)

        df_clientes['TELEFONE_CLIENTE'] = df_clientes['TELEFONE_CLIENTE'].astype(str).str.replace(r'^55', '', regex=True).str.replace(r'\.0$', '', regex=True)
        df_clientes['TELEFONE_CLIENTE'] = df_clientes['TELEFONE_CLIENTE'].str.strip()

        df_clientes['MATRICULA_CLIENTE'] = df_clientes['MATRICULA_CLIENTE'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        df_clientes['SITUACAO'] = pd.to_numeric(df_clientes['SITUACAO'], errors='coerce').fillna(0)

        if 'CIDADE' in df_clientes.columns:
            df_clientes['CIDADE'] = df_clientes['CIDADE'].astype(str).str.strip()
        if 'DIRETORIA' in df_clientes.columns:
            df_clientes['DIRETORIA'] = df_clientes['DIRETORIA'].astype(str).str.strip()

        df_clientes.drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], inplace=True)
        return df_clientes
    except Exception as e:
        st.error(f"Erro ao processar arquivo de Clientes: {e}")
        return None

# --- Funções de Carregamento/Salvamento de Campanhas do GitHub ---

@st.cache_data(ttl=300) # Cache por 5 minutos
def load_meta_github():
    content = get_file_from_github("meta.json")
    if content:
        return pd.DataFrame(json.loads(content))
    return pd.DataFrame(columns=['id', 'nome', 'data_upload', 'envios_path', 'clientes_path'])

@st.cache_data(ttl=3600)
def load_campanha_envios(campanha_id):
    df_meta = load_meta_github()
    meta = df_meta[df_meta['id'] == campanha_id]
    if not meta.empty:
        path = meta['envios_path'].iloc[0]
        content = get_file_from_github(path, raw=True) # Usar raw para arquivos maiores
        if content:
            return load_and_process_envios(content)
    return None

@st.cache_data(ttl=3600)
def load_campanha_clientes(campanha_id):
    df_meta = load_meta_github()
    meta = df_meta[df_meta['id'] == campanha_id]
    if not meta.empty:
        path = meta['clientes_path'].iloc[0]
        content = get_file_from_github(path, raw=True) # Usar raw para arquivos maiores
        if content:
            return load_and_process_clientes(content)
    return None

@st.cache_data(ttl=3600)
def load_pagamentos_github():
    path = "pagamentos.parquet"
    content = get_file_from_github(path, raw=True) # Usar raw para arquivos maiores
    if content:
        return load_and_process_pagamentos(content, '.parquet')
    return None

def save_campanha(nome, df_envios, df_clientes):
    df_meta = load_meta_github()
    campanha_id = str(len(df_meta) + 1)
    envios_path = f"campanhas/{campanha_id}_envios.xlsx"
    clientes_path = f"campanhas/{campanha_id}_clientes.xlsx"

    # Salvar envios
    envios_buffer = pd.io.excel.ExcelWriter("temp_envios.xlsx", engine='xlsxwriter')
    df_envios.to_excel(envios_buffer, index=False)
    envios_buffer.close()
    with open("temp_envios.xlsx", "rb") as f:
        envios_content = f.read()
    if not save_file_to_github(envios_path, envios_content, f"Add envios for campaign {campanha_id}", get_file_sha(envios_path)):
        return None, "Erro ao salvar envios no GitHub."

    # Salvar clientes
    clientes_buffer = pd.io.excel.ExcelWriter("temp_clientes.xlsx", engine='xlsxwriter')
    df_clientes.to_excel(clientes_buffer, index=False)
    clientes_buffer.close()
    with open("temp_clientes.xlsx", "rb") as f:
        clientes_content = f.read()
    if not save_file_to_github(clientes_path, clientes_content, f"Add clientes for campaign {campanha_id}", get_file_sha(clientes_path)):
        return None, "Erro ao salvar clientes no GitHub."

    # Atualizar meta.json
    nova_meta = pd.DataFrame([{
        'id': campanha_id,
        'nome': nome,
        'data_upload': datetime.now().isoformat(),
        'envios_path': envios_path,
        'clientes_path': clientes_path
    }])
    df_meta_atualizada = pd.concat([df_meta, nova_meta], ignore_index=True)
    meta_content = df_meta_atualizada.to_json(orient="records", indent=4).encode("utf-8")
    if not save_file_to_github("meta.json", meta_content, f"Update meta.json for campaign {campanha_id}", get_file_sha("meta.json")):
        return None, "Erro ao atualizar meta.json no GitHub."

    load_meta_github.clear() # Limpa cache da meta
    return campanha_id, None

def update_pagamentos_github(df_pagamentos):
    path = "pagamentos.parquet"
    pagamentos_buffer = df_pagamentos.to_parquet(index=False)

    sha = get_file_sha(path)
    if save_file_to_github(path, pagamentos_buffer, "Update pagamentos.parquet", sha):
        load_pagamentos_github.clear() # Limpa cache dos pagamentos
        return True, len(df_pagamentos), 0 # Não calculamos novos aqui
    return False, 0, 0

# --- Funções Auxiliares de Formatação e Plotagem ---
def fmt_brl(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def add_bar_labels(fig, formato='valor'):
    for trace in fig.data:
        if isinstance(trace, go.Bar) and hasattr(trace, 'y') and trace.y is not None:
            if formato == 'valor':
                texts = [fmt_brl(v) if v is not None else '' for v in trace.y]
            else:
                texts = [str(int(v)) if v is not None else '' for v in trace.y]
            trace.text = texts
            trace.textposition = 'outside'
            trace.textfont = dict(size=11)
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    return fig

# --- Sidebar: Seleção de Campanhas ---
st.sidebar.header("Seleção de Campanhas")

df_meta = load_meta_github()
campanhas_disponiveis = df_meta['nome'].tolist() if not df_meta.empty else []

# Adiciona um filtro por mês/ano
df_meta['data_upload_dt'] = pd.to_datetime(df_meta['data_upload'])
df_meta['mes_ano'] = df_meta['data_upload_dt'].dt.strftime('%Y-%m')
meses_disponiveis = sorted(df_meta['mes_ano'].unique().tolist(), reverse=True)

mes_selecionado = st.sidebar.selectbox(
    "Filtrar por Mês/Ano de Upload",
    options=["Todos"] + meses_disponiveis,
    key="mes_filtro_selector"
)

campanhas_disponiveis_para_selecao = []
if mes_selecionado == "Todos":
    campanhas_disponiveis_para_selecao = campanhas_disponiveis
else:
    campanhas_do_mes = df_meta[df_meta['mes_ano'] == mes_selecionado]
    campanhas_disponiveis_para_selecao = campanhas_do_mes['nome'].tolist()

campanhas_selecionadas_nomes = st.sidebar.multiselect(
    "Selecionar Campanhas (múltipla escolha)",
    options=campanhas_disponiveis_para_selecao,
    default=campanhas_disponiveis_para_selecao, # Seleciona todas por padrão
    key="multi_campanha_selector"
)

# Filtra o df_meta para obter os IDs das campanhas selecionadas
campanhas_selecionadas_meta = df_meta[df_meta['nome'].isin(campanhas_selecionadas_nomes)]
campanhas_selecionadas_ids = campanhas_selecionadas_meta['id'].tolist() # Usar IDs para carregar

st.sidebar.markdown("---")

# ══════════════════════════════════════════════════════════════
# RESOLUÇÃO DOS DADOS (AGORA AGREGADOS)
# ══════════════════════════════════════════════════════════════

df_envios_agregado     = pd.DataFrame() # Inicializa como DataFrame vazio
df_clientes_agregado   = pd.DataFrame() # Inicializa como DataFrame vazio
df_pagamentos          = None # Pagamentos continuam sendo carregados uma vez

# Carrega pagamentos do GitHub automaticamente sempre
df_pagamentos = load_pagamentos_github()
if df_pagamentos is not None:
    st.sidebar.success(f"✅ Pagamentos carregados ({len(df_pagamentos):,} registros)")
else:
    if is_admin():
        st.sidebar.warning("⚠️ Base de pagamentos não encontrada. Faça o upload na seção Administração.")
    else:
        st.sidebar.warning("⚠️ Base de pagamentos indisponível. Contate o administrador.")

# Carrega e agrega dados das campanhas selecionadas
if campanhas_selecionadas_ids:
    lista_df_envios   = []
    lista_df_clientes = []
    with st.spinner("Carregando dados das campanhas selecionadas..."):
        for campanha_id in campanhas_selecionadas_ids:
            df_env_temp = load_campanha_envios(campanha_id)
            df_cli_temp = load_campanha_clientes(campanha_id)

            if df_env_temp is not None:
                df_env_temp['CAMPANHA_ID'] = campanha_id # Adiciona ID da campanha para rastreamento
                lista_df_envios.append(df_env_temp)
            if df_cli_temp is not None:
                df_cli_temp['CAMPANHA_ID'] = campanha_id # Adiciona ID da campanha para rastreamento
                lista_df_clientes.append(df_cli_temp)

    if lista_df_envios:
        df_envios_agregado = pd.concat(lista_df_envios, ignore_index=True)
        st.sidebar.success(f"✅ Envios agregados ({len(df_envios_agregado):,} registros)")
    else:
        st.sidebar.error("Erro ao carregar envios das campanhas selecionadas.")

    if lista_df_clientes:
        df_clientes_agregado = pd.concat(lista_df_clientes, ignore_index=True)
        st.sidebar.success(f"✅ Clientes agregados ({len(df_clientes_agregado):,} registros)")
    else:
        st.sidebar.error("Erro ao carregar clientes das campanhas selecionadas.")
else:
    st.sidebar.info("Nenhuma campanha selecionada para análise.")

dados_prontos = (
    not df_envios_agregado.empty and
    not df_clientes_agregado.empty and
    df_pagamentos is not None and
    not df_pagamentos.empty
)

# ── Sidebar: configurações da análise ────────────────────────
st.sidebar.header("⚙️ Configurações")
janela_dias      = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7, key="janela_dias_slider")
executar_analise = st.sidebar.button("▶️ Executar Análise", use_container_width=True, key="executar_analise_btn")

# ══════════════════════════════════════════════════════════════
# SIDEBAR — ADMINISTRAÇÃO (somente admin)
# ══════════════════════════════════════════════════════════════

if is_admin():
    st.sidebar.markdown("---")
    st.sidebar.header("🔧 Administração")

    with st.sidebar.expander("➕ Nova Campanha"):
        nome_nova               = st.text_input("Nome da campanha", key="nome_nova_campanha_input")
        uploaded_envios_admin   = st.file_uploader("Base de Envios (.xlsx)",   type=["xlsx"], key="up_env_admin")
        uploaded_clientes_admin = st.file_uploader("Base de Clientes (.xlsx)", type=["xlsx"], key="up_cli_admin")
        if st.button("💾 Salvar campanha", key="salvar_campanha_btn"):
            if not nome_nova.strip():
                st.error("Informe um nome para a campanha.")
            elif uploaded_envios_admin is None:
                st.error("Faça upload da base de envios.")
            elif uploaded_clientes_admin is None:
                st.error("Faça upload da base de clientes.")
            else:
                df_env_tmp = load_and_process_envios(uploaded_envios_admin.read())
                df_cli_tmp = load_and_process_clientes(uploaded_clientes_admin.read())
                if df_env_tmp is not None and df_cli_tmp is not None:
                    with st.spinner("Salvando no GitHub..."):
                        cid, erro = save_campanha(nome_nova.strip(), df_env_tmp, df_cli_tmp)
                    if erro:
                        st.error(erro)
                    else:
                        st.success(f"Campanha '{nome_nova}' salva! ID: `{cid}`")
                        st.rerun()

    with st.sidebar.expander("💰 Base de Pagamentos"):
        pag_atual = load_pagamentos_github()
        if pag_atual is not None:
            st.caption(f"✅ Base atual: {len(pag_atual):,} registros")
        else:
            st.caption("⚠️ Nenhuma base salva ainda.")
        uploaded_pag_admin = st.file_uploader(
            "Enviar/Atualizar pagamentos",
            type=["csv", "xlsx", "parquet"],
            key="up_pag_admin"
        )
        if st.button("⬆️ Enviar para o GitHub", key="enviar_pagamentos_btn"):
            if uploaded_pag_admin is None:
                st.error("Selecione um arquivo de pagamentos.")
            else:
                file_extension = uploaded_pag_admin.name.split('.')[-1]
                df_pag_tmp = load_and_process_pagamentos(uploaded_pag_admin.read(), f".{file_extension}")
                if df_pag_tmp is not None:
                    with st.spinner("Atualizando..."):
                        ok, total, novos = update_pagamentos_github(df_pag_tmp)
                    if ok:
                        st.success(f"Atualizado! Total: {total:,} | Novos: {novos:,}")
                    else:
                        st.error("Erro ao salvar no GitHub.")

# ══════════════════════════════════════════════════════════════
# ANÁLISE PRINCIPAL
# ══════════════════════════════════════════════════════════════

if executar_analise and dados_prontos:

    # ── Lógica de cruzamento para visão acumulada ────────────────
    # Identifica envios únicos por telefone para evitar duplicidade na contagem de "notificados"
    # Mantém o primeiro envio para cada telefone
    df_envios_unicos_por_telefone = df_envios_agregado.sort_values('DATA_ENVIO').drop_duplicates(
        subset='TELEFONE_ENVIO', keep='first'
    )
    total_clientes_notificados = df_envios_unicos_por_telefone['TELEFONE_ENVIO'].nunique()

    # Calcula a dívida total dos clientes notificados
    df_lookup_divida = pd.merge(
        df_envios_unicos_por_telefone[['TELEFONE_ENVIO']],
        df_clientes_agregado[['TELEFONE_CLIENTE', 'SITUACAO']].drop_duplicates(subset='TELEFONE_CLIENTE', keep='first'),
        left_on='TELEFONE_ENVIO',
        right_on='TELEFONE_CLIENTE',
        how='left'
    )
    total_divida_notificados = df_lookup_divida['SITUACAO'].sum()

    # Merge de envios agregados com clientes agregados
    df_merge = pd.merge(
        df_envios_agregado,
        df_clientes_agregado,
        left_on='TELEFONE_ENVIO',
        right_on='TELEFONE_CLIENTE',
        how='inner'
    )

    if df_merge.empty:
        st.error("Nenhum cliente encontrado após cruzamento entre envios e clientes agregados.")
        st.stop()

    # Renomeia e limpa colunas para o próximo merge
    df_merge['MATRICULA_CLIENTE'] = df_merge['MATRICULA_CLIENTE'].astype(str).str.strip()
    df_pagamentos['MATRICULA_PAGAMENTO'] = df_pagamentos['MATRICULA_PAGAMENTO'].astype(str).str.strip()

    # Merge com pagamentos
    df_cruzado = pd.merge(
        df_merge,
        df_pagamentos,
        left_on='MATRICULA_CLIENTE',
        right_on='MATRICULA_PAGAMENTO',
        how='inner'
    )

    if df_cruzado.empty:
        st.error("Nenhum pagamento encontrado após cruzamento com a base de clientes e envios.")
        st.stop()

    # Calcula dias após o envio e filtra pela janela
    df_cruzado['DIAS_APOS_ENVIO'] = (
        df_cruzado['DATA_PAGAMENTO'] - df_cruzado['DATA_ENVIO']
    ).dt.days

    df_pagamentos_campanha = df_cruzado[
        (df_cruzado['DIAS_APOS_ENVIO'] >= 0) &
        (df_cruzado['DIAS_APOS_ENVIO'] <= janela_dias)
    ].copy()

    # Remove duplicidades de pagamentos (mesma matrícula, data, valor)
    df_pagamentos_campanha = df_pagamentos_campanha.drop_duplicates(
        subset=['MATRICULA_CLIENTE', 'DATA_PAGAMENTO', 'VALOR_PAGO'],
        keep='first'
    )

    df_pagamentos_campanha.rename(columns={'MATRICULA_CLIENTE': 'MATRICULA'}, inplace=True)

    # ── Métricas ──────────────────────────────────────────────
    clientes_que_pagaram_matriculas = df_pagamentos_campanha['MATRICULA'].nunique()
    valor_total_arrecadado          = df_pagamentos_campanha['VALOR_PAGO'].sum() if not df_pagamentos_campanha.empty else 0
    taxa_eficiencia_clientes        = (clientes_que_pagaram_matriculas / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0
    taxa_eficiencia_valor           = (valor_total_arrecadado / total_divida_notificados * 100) if total_divida_notificados > 0 else 0
    ticket_medio                    = (valor_total_arrecadado / clientes_que_pagaram_matriculas) if clientes_que_pagaram_matriculas > 0 else 0
    custo_campanha                  = total_clientes_notificados * 0.05 # Exemplo de custo
    roi                             = ((valor_total_arrecadado - custo_campanha) / custo_campanha * 100) if custo_campanha > 0 else 0

    # ── KPIs ─────────────────────────────────────────────────
    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Clientes Notificados (únicos)",    f"{total_clientes_notificados:,}")
    c2.metric("Clientes que Pagaram",             f"{clientes_que_pagaram_matriculas:,}")
    c3.metric("Taxa de Conversão",                f"{taxa_eficiencia_clientes:.1f}%")
    c4.metric("Valor Arrecadado",                 fmt_brl(valor_total_arrecadado))
    c5.metric("% da Dívida Recuperada",           f"{taxa_eficiencia_valor:.1f}%")
    st.markdown("---")

    # ── ABAS ─────────────────────────────────────────────────
    aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
        "📈 Visão Geral",
        "👥 Clientes Notificados", # Nova aba
        "🏙️ Cidade e Diretoria",
        "📅 Análise das Faturas",
        "💳 Canal de Pagamento",
        "📋 Detalhes"
    ])

    # ══════════════════════════════════════════════════════════
    # ABA 1 — VISÃO GERAL
    # ══════════════════════════════════════════════════════════
    with aba1:
        st.subheader("Resultados da Análise da Campanha")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total de clientes notificados (únicos)", f"{total_clientes_notificados}")
        with col2:
            st.metric("Clientes que pagaram na janela", f"{clientes_que_pagaram_matriculas}")
        with col3:
            st.metric("Taxa de eficiência (clientes)", f"{taxa_eficiencia_clientes:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        col4, col5, col6 = st.columns(3)
        with col4:
            st.metric("Valor total arrecadado na campanha", f"R$ {valor_total_arrecadado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with col5:
            st.metric("Total da dívida dos notificados", f"R$ {total_divida_notificados:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with col6:
            st.metric("Taxa de eficiência (valor)", f"{taxa_eficiencia_valor:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        col7, col8, col9 = st.columns(3)
        with col7:
            st.metric("Ticket médio", f"R$ {ticket_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with col8:
            st.metric("Custo da campanha", f"R$ {custo_campanha:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with col9:
            st.metric("ROI", f"{roi:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        if not df_pagamentos_campanha.empty:
            st.subheader(f"Pagamentos por Dia Após o Envio (Janela de {janela_dias} dias)")

            pagamentos_por_dia = df_pagamentos_campanha.groupby('DIAS_APOS_ENVIO')['VALOR_PAGO'].sum().reset_index()
            pagamentos_por_dia.rename(columns={'DIAS_APOS_ENVIO': 'Dias Após Envio', 'VALOR_PAGO': 'Valor Total Pago'}, inplace=True)

            fig_dias = px.bar(
                pagamentos_por_dia,
                x='Dias Após Envio', y='Valor Total Pago',
                title='Valor Arrecadado por Dia Após o Envio',
                labels={'Dias Após Envio': 'Dias Após o Envio', 'Valor Total Pago': 'Valor Total Pago (R$)'},
                hover_data={'Valor Total Pago': ':.2f'}
            )
            fig_dias = add_bar_labels(fig_dias, 'valor')
            fig_dias.update_layout(xaxis_title="Dias Após o Envio", yaxis_title="Valor Total Pago (R$)")
            st.plotly_chart(fig_dias, use_container_width=True, key="fig_dias")

            # Tabela pagamentos por dia
            tab_dias = pagamentos_por_dia.copy()
            tab_dias['Valor Total Pago'] = tab_dias['Valor Total Pago'].apply(fmt_brl)
            st.dataframe(tab_dias, use_container_width=True, hide_index=True)

            if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                st.subheader("Valor Arrecadado por Canal de Pagamento")

                pagamentos_por_canal = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['VALOR_PAGO'].sum().reset_index()
                pagamentos_por_canal = pagamentos_por_canal.sort_values('VALOR_PAGO', ascending=False)

                fig_canal = px.bar(
                    pagamentos_por_canal,
                    x='TIPO_PAGAMENTO', y='VALOR_PAGO',
                    title='Valor Arrecadado por Canal de Pagamento',
                    labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'VALOR_PAGO': 'Valor Total Pago (R$)'},
                    color='TIPO_PAGAMENTO',
                    hover_data={'VALOR_PAGO': ':.2f'}
                )
                fig_canal = add_bar_labels(fig_canal, 'valor')
                fig_canal.update_layout(xaxis_title="Canal de Pagamento", yaxis_title="Valor Total Pago (R$)", showlegend=False)
                st.plotly_chart(fig_canal, use_container_width=True, key="fig_canal_aba1")

                #Tabela canal aba1
                tab_canal_v1 = pagamentos_por_canal.copy()
                tab_canal_v1.columns = ['Canal de Pagamento', 'Valor Total Pago']
                tab_canal_v1['Valor Total Pago'] = tab_canal_v1['Valor Total Pago'].apply(fmt_brl)
                st.dataframe(tab_canal_v1, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

    # ══════════════════════════════════════════════════════════
    # NOVA ABA 2 — CLIENTES NOTIFICADOS
    # ══════════════════════════════════════════════════════════
    with aba2:
        st.subheader("Análise de Notificações por Cliente")

        # Contagem de quantas vezes cada telefone foi notificado
        contagem_notificacoes = df_envios_agregado['TELEFONE_ENVIO'].value_counts().reset_index()
        contagem_notificacoes.columns = ['TELEFONE_ENVIO', 'NUM_NOTIFICACOES']

        # Merge com dados de clientes para obter matrícula e status de pagamento
        df_notificacoes_clientes = pd.merge(
            contagem_notificacoes,
            df_clientes_agregado[['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE']].drop_duplicates(subset='TELEFONE_CLIENTE'),
            left_on='TELEFONE_ENVIO',
            right_on='TELEFONE_CLIENTE',
            how='left'
        )
        df_notificacoes_clientes.rename(columns={'MATRICULA_CLIENTE': 'MATRICULA'}, inplace=True)

        # Adiciona informação se o cliente pagou ou não
        matriculas_que_pagaram = df_pagamentos_campanha['MATRICULA'].unique()
        df_notificacoes_clientes['PAGOU_NA_CAMPANHA'] = df_notificacoes_clientes['MATRICULA'].isin(matriculas_que_pagaram)

        st.write("Visão geral dos clientes notificados e seu status de pagamento:")
        st.dataframe(df_notificacoes_clientes.head(), use_container_width=True)

        st.subheader("Distribuição de Notificações")
        dist_notificacoes = df_notificacoes_clientes['NUM_NOTIFICACOES'].value_counts().sort_index().reset_index()
        dist_notificacoes.columns = ['Número de Notificações', 'Quantidade de Clientes']

        fig_dist_notif = px.bar(
            dist_notificacoes,
            x='Número de Notificações', y='Quantidade de Clientes',
            title='Quantidade de Clientes por Número de Notificações Recebidas',
            labels={'Número de Notificações': 'Número de Notificações', 'Quantidade de Clientes': 'Quantidade de Clientes'}
        )
        fig_dist_notif = add_bar_labels(fig_dist_notif, 'qtd')
        fig_dist_notif.update_layout(xaxis_title="Número de Notificações", yaxis_title="Quantidade de Clientes")
        st.plotly_chart(fig_dist_notif, use_container_width=True)

        st.subheader("Clientes que Pagaram vs. Número de Notificações")
        pagamentos_por_notificacao = df_notificacoes_clientes.groupby('NUM_NOTIFICACOES')['PAGOU_NA_CAMPANHA'].sum().reset_index()
        pagamentos_por_notificacao.columns = ['Número de Notificações', 'Clientes que Pagaram']

        fig_pag_notif = px.bar(
            pagamentos_por_notificacao,
            x='Número de Notificações', y='Clientes que Pagaram',
            title='Clientes que Pagaram por Número de Notificações Recebidas',
            labels={'Número de Notificações': 'Número de Notificações', 'Clientes que Pagaram': 'Clientes que Pagaram'}
        )
        fig_pag_notif = add_bar_labels(fig_pag_notif, 'qtd')
        fig_pag_notif.update_layout(xaxis_title="Número de Notificações", yaxis_title="Clientes que Pagaram")
        st.plotly_chart(fig_pag_notif, use_container_width=True)

        st.subheader("Detalhes dos Clientes Notificados")
        st.dataframe(df_notificacoes_clientes, use_container_width=True)

    # ══════════════════════════════════════════════════════════
    # ABA 3 — CIDADE E DIRETORIA (antiga ABA 2)
    # ══════════════════════════════════════════════════════════
    with aba3:
        if not df_pagamentos_campanha.empty:
            tem_cidade    = 'CIDADE' in df_pagamentos_campanha.columns
            tem_diretoria = 'DIRETORIA' in df_pagamentos_campanha.columns

            if tem_cidade:
                st.subheader("Análise por Cidade")

                cidade_resumo = df_pagamentos_campanha.groupby('CIDADE').agg(
                    Clientes_que_Pagaram=('MATRICULA', 'nunique'),
                    Valor_Arrecadado=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

                fig_cidade_valor = px.bar(
                    cidade_resumo,
                    x='CIDADE', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Cidade',
                    labels={'CIDADE': 'Cidade', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'},
                    hover_data={'Valor_Arrecadado': ':.2f'}
                )
                fig_cidade_valor = add_bar_labels(fig_cidade_valor, 'valor')
                fig_cidade_valor.update_layout(xaxis_title="Cidade", yaxis_title="Valor Arrecadado (R$)")
                st.plotly_chart(fig_cidade_valor, use_container_width=True, key="fig_cidade_valor")

                fig_cidade_clientes = px.bar(
                    cidade_resumo,
                    x='CIDADE', y='Clientes_que_Pagaram',
                    title='Clientes que Pagaram por Cidade',
                    labels={'CIDADE': 'Cidade', 'Clientes_que_Pagaram': 'Clientes que Pagaram'}
                )
                fig_cidade_clientes = add_bar_labels(fig_cidade_clientes, 'qtd')
                fig_cidade_clientes.update_layout(xaxis_title="Cidade", yaxis_title="Clientes que Pagaram")
                st.plotly_chart(fig_cidade_clientes, use_container_width=True, key="fig_cidade_clientes")

                # Tabela cidade
                tab_cidade = cidade_resumo.copy()
                tab_cidade.columns = ['Cidade', 'Clientes que Pagaram', 'Valor Arrecadado']
                tab_cidade['Valor Arrecadado'] = tab_cidade['Valor Arrecadado'].apply(fmt_brl)
                st.dataframe(tab_cidade, use_container_width=True, hide_index=True)

                if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                    st.subheader("Canal de Pagamento por Cidade")
                    cidade_canal = df_pagamentos_campanha.groupby(['CIDADE', 'TIPO_PAGAMENTO'])['VALOR_PAGO'].sum().reset_index()
                    fig_cidade_canal = px.bar(
                        cidade_canal,
                        x='CIDADE', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                        title='Valor Pago por Cidade e Canal',
                        labels={'CIDADE': 'Cidade', 'VALOR_PAGO': 'Valor Pago (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                        barmode='stack'
                    )
                    fig_cidade_canal.update_layout(xaxis_title="Cidade", yaxis_title="Valor Pago (R$)")
                    st.plotly_chart(fig_cidade_canal, use_container_width=True, key="fig_cidade_canal")

                    # Tabela cidade x canal
                    tab_cidade_canal = cidade_canal.copy()
                    tab_cidade_canal.columns = ['Cidade', 'Canal de Pagamento', 'Valor Pago']
                    tab_cidade_canal['Valor Pago'] = tab_cidade_canal['Valor Pago'].apply(fmt_brl)
                    tab_cidade_canal = tab_cidade_canal.sort_values(['Cidade', 'Canal de Pagamento'])
                    st.dataframe(tab_cidade_canal, use_container_width=True, hide_index=True)

            if tem_diretoria:
                st.subheader("Análise por Diretoria")

                diretoria_resumo = df_pagamentos_campanha.groupby('DIRETORIA').agg(
                    Clientes_que_Pagaram=('MATRICULA', 'nunique'),
                    Valor_Arrecadado=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

                fig_diretoria_valor = px.bar(
                    diretoria_resumo,
                    x='DIRETORIA', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Diretoria',
                    labels={'DIRETORIA': 'Diretoria', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'},
                    hover_data={'Valor_Arrecadado': ':.2f'}
                )
                fig_diretoria_valor = add_bar_labels(fig_diretoria_valor, 'valor')
                fig_diretoria_valor.update_layout(xaxis_title="Diretoria", yaxis_title="Valor Arrecadado (R$)")
                st.plotly_chart(fig_diretoria_valor, use_container_width=True, key="fig_diretoria_valor")

                fig_diretoria_clientes = px.bar(
                    diretoria_resumo,
                    x='DIRETORIA', y='Clientes_que_Pagaram',
                    title='Clientes que Pagaram por Diretoria',
                    labels={'DIRETORIA': 'Diretoria', 'Clientes_que_Pagaram': 'Clientes que Pagaram'}
                )
                fig_diretoria_clientes = add_bar_labels(fig_diretoria_clientes, 'qtd')
                fig_diretoria_clientes.update_layout(xaxis_title="Diretoria", yaxis_title="Clientes que Pagaram")
                st.plotly_chart(fig_diretoria_clientes, use_container_width=True, key="fig_diretoria_clientes")

                # Tabela diretoria
                tab_diretoria = diretoria_resumo.copy()
                tab_diretoria.columns = ['Diretoria', 'Clientes que Pagaram', 'Valor Arrecadado']
                tab_diretoria['Valor Arrecadado'] = tab_diretoria['Valor Arrecadado'].apply(fmt_brl)
                st.dataframe(tab_diretoria, use_container_width=True, hide_index=True)

                if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                    st.subheader("Tipo de Pagamento por Diretoria")
                    diretoria_canal = df_pagamentos_campanha.groupby(['DIRETORIA', 'TIPO_PAGAMENTO'])['VALOR_PAGO'].sum().reset_index()
                    fig_diretoria_canal = px.bar(
                        diretoria_canal,
                        x='DIRETORIA', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                        title='Valor Pago por Diretoria e Canal de Pagamento',
                        labels={'DIRETORIA': 'Diretoria', 'VALOR_PAGO': 'Valor Pago (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                        barmode='stack'
                    )
                    fig_diretoria_canal.update_layout(xaxis_title="Diretoria", yaxis_title="Valor Pago (R$)")
                    st.plotly_chart(fig_diretoria_canal, use_container_width=True, key="fig_diretoria_canal")

                    # Tabela diretoria x canal
                    tab_diretoria_canal = diretoria_canal.copy()
                    tab_diretoria_canal.columns = ['Diretoria', 'Canal de Pagamento', 'Valor Pago']
                    tab_diretoria_canal['Valor Pago'] = tab_diretoria_canal['Valor Pago'].apply(fmt_brl)
                    tab_diretoria_canal = tab_diretoria_canal.sort_values(['Diretoria', 'Canal de Pagamento'])
                    st.dataframe(tab_diretoria_canal, use_container_width=True, hide_index=True)

            if not tem_cidade and not tem_diretoria:
                st.info("Colunas 'CIDADE' e 'DIRETORIA' não encontradas na base de clientes.")
        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

    # ══════════════════════════════════════════════════════════
    # ABA 4 — ANÁLISE DAS FATURAS (antiga ABA 3)
    # ══════════════════════════════════════════════════════════
    with aba4:
        if not df_pagamentos_campanha.empty:

            if 'VENCIMENTO' in df_pagamentos_campanha.columns:
                st.subheader("Antiguidade da Dívida Paga")

                df_pagamentos_campanha['ANTIGUIDADE_DIAS'] = (
                    df_pagamentos_campanha['DATA_PAGAMENTO'] - df_pagamentos_campanha['VENCIMENTO']
                ).dt.days

                def classificar_antiguidade(dias):
                    if pd.isna(dias):
                        return 'Não informado'
                    elif dias <= 10:
                        return '0-10 dias'
                    elif dias <= 20:
                        return '11-20 dias'
                    elif dias <= 30:
                        return '21-30 dias'
                    elif dias <= 60:
                        return '31-60 dias'
                    else:
                        return 'Mais de 61 dias'

                df_pagamentos_campanha['FAIXA_ANTIGUIDADE'] = df_pagamentos_campanha['ANTIGUIDADE_DIAS'].apply(classificar_antiguidade)

                ordem_faixas = ['0-10 dias', '11-20 dias', '21-30 dias', '31-60 dias', 'Mais de 61 dias', 'Não informado']

                antiguidade_resumo = df_pagamentos_campanha.groupby('FAIXA_ANTIGUIDADE').agg(
                    Quantidade=('MATRICULA', 'count'),
                    Valor_Pago=('VALOR_PAGO', 'sum')
                ).reset_index()
                antiguidade_resumo['FAIXA_ANTIGUIDADE'] = pd.Categorical(
                    antiguidade_resumo['FAIXA_ANTIGUIDADE'], categories=ordem_faixas, ordered=True
                )
                antiguidade_resumo = antiguidade_resumo.sort_values('FAIXA_ANTIGUIDADE')

                fig_ant_valor = px.bar(
                    antiguidade_resumo,
                    x='FAIXA_ANTIGUIDADE', y='Valor_Pago',
                    title='Valor Pago por Faixa de Antiguidade da Dívida',
                    labels={'FAIXA_ANTIGUIDADE': 'Faixa de Antiguidade', 'Valor_Pago': 'Valor Pago (R$)'},
                    hover_data={'Valor_Pago': ':.2f'}
                )
                fig_ant_valor = add_bar_labels(fig_ant_valor, 'valor')
                fig_ant_valor.update_layout(xaxis_title="Faixa de Antiguidade", yaxis_title="Valor Pago (R$)")
                st.plotly_chart(fig_ant_valor, use_container_width=True, key="fig_ant_valor")

                fig_ant_qtd = px.bar(
                    antiguidade_resumo,
                    x='FAIXA_ANTIGUIDADE', y='Quantidade',
                    title='Quantidade de Pagamentos por Faixa de Antiguidade',
                    labels={'FAIXA_ANTIGUIDADE': 'Faixa de Antiguidade', 'Quantidade': 'Quantidade de Pagamentos'}
                )
                fig_ant_qtd = add_bar_labels(fig_ant_qtd, 'qtd')
                fig_ant_qtd.update_layout(xaxis_title="Faixa de Antiguidade", yaxis_title="Quantidade de Pagamentos")
                st.plotly_chart(fig_ant_qtd, use_container_width=True, key="fig_ant_qtd")

                # Tabela antiguidade
                tab_ant = antiguidade_resumo.copy()
                tab_ant.columns = ['Faixa de Antiguidade', 'Quantidade de Pagamentos', 'Valor Pago']
                tab_ant['Valor Pago'] = tab_ant['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_ant, use_container_width=True, hide_index=True)

            if 'MES_ANO_FATURA' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Mês/Ano da Fatura")

                mes_ano_resumo = df_pagamentos_campanha.groupby(
                    ['ANO_FATURA', 'MES_FATURA', 'MES_ANO_FATURA']
                )['VALOR_PAGO'].sum().reset_index()
                mes_ano_resumo = mes_ano_resumo.sort_values(['ANO_FATURA', 'MES_FATURA'])

                fig_mes_ano = px.bar(
                    mes_ano_resumo,
                    x='MES_ANO_FATURA', y='VALOR_PAGO',
                    title='Valor Pago por Mês/Ano da Fatura',
                    labels={'MES_ANO_FATURA': 'Mês/Ano da Fatura', 'VALOR_PAGO': 'Valor Pago (R$)'},
                    hover_data={'VALOR_PAGO': ':.2f'}
                )
                fig_mes_ano = add_bar_labels(fig_mes_ano, 'valor')
                fig_mes_ano.update_layout(xaxis_title="Mês/Ano da Fatura", yaxis_title="Valor Pago (R$)")
                st.plotly_chart(fig_mes_ano, use_container_width=True, key="fig_mes_ano")

                # Tabela mês/ano
                tab_mes_ano = mes_ano_resumo[['MES_ANO_FATURA', 'VALOR_PAGO']].copy()
                tab_mes_ano.columns = ['Mês/Ano da Fatura', 'Valor Pago']
                tab_mes_ano['Valor Pago'] = tab_mes_ano['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_mes_ano, use_container_width=True, hide_index=True)

            if 'TIPO_FATURA' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Tipo de Fatura")

                tipo_fatura_resumo = df_pagamentos_campanha.groupby('TIPO_FATURA').agg(
                    Quantidade=('MATRICULA', 'count'),
                    Valor_Pago=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Pago', ascending=False)

                fig_tipo_fatura = px.bar(
                    tipo_fatura_resumo,
                    x='TIPO_FATURA', y='Valor_Pago',
                    title='Valor Pago por Tipo de Fatura',
                    labels={'TIPO_FATURA': 'Tipo de Fatura', 'Valor_Pago': 'Valor Pago (R$)'},
                    color='TIPO_FATURA',
                    hover_data={'Valor_Pago': ':.2f', 'Quantidade': True}
                )
                fig_tipo_fatura = add_bar_labels(fig_tipo_fatura, 'valor')
                fig_tipo_fatura.update_layout(xaxis_title="Tipo de Fatura", yaxis_title="Valor Pago (R$)", showlegend=False)
                st.plotly_chart(fig_tipo_fatura, use_container_width=True, key="fig_tipo_fatura")

                # Tabela tipo fatura
                tab_tipo_fatura = tipo_fatura_resumo.copy()
                tab_tipo_fatura.columns = ['Tipo de Fatura', 'Quantidade', 'Valor Pago']
                tab_tipo_fatura['Valor Pago'] = tab_tipo_fatura['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_tipo_fatura, use_container_width=True, hide_index=True)

            if 'UTILIZACAO' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Utilização (Sub. Categoria)")

                utilizacao_resumo = df_pagamentos_campanha.groupby('UTILIZACAO').agg(
                    Quantidade=('MATRICULA', 'count'),
                    Valor_Pago=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Pago', ascending=False)

                fig_utilizacao = px.bar(
                    utilizacao_resumo,
                    x='UTILIZACAO', y='Valor_Pago',
                    title='Valor Pago por Utilização (Sub. Categoria)',
                    labels={'UTILIZACAO': 'Utilização', 'Valor_Pago': 'Valor Pago (R$)'},
                    color='UTILIZACAO',
                    hover_data={'Valor_Pago': ':.2f', 'Quantidade': True}
                )
                fig_utilizacao = add_bar_labels(fig_utilizacao, 'valor')
                fig_utilizacao.update_layout(xaxis_title="Utilização", yaxis_title="Valor Pago (R$)", showlegend=False)
                st.plotly_chart(fig_utilizacao, use_container_width=True, key="fig_utilizacao")

            # Tabela utilização
                tab_utilizacao = utilizacao_resumo.copy()
                tab_utilizacao.columns = ['Utilização', 'Quantidade', 'Valor Pago']
                tab_utilizacao['Valor Pago'] = tab_utilizacao['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_utilizacao, use_container_width=True, hide_index=True)

        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

    # ══════════════════════════════════════════════════════════
    # ABA 5 — CANAL DE PAGAMENTO (antiga ABA 4)
    # ══════════════════════════════════════════════════════════
    with aba5:
        if not df_pagamentos_campanha.empty and 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:

            st.subheader("Valor Arrecadado por Canal de Pagamento")

            pagamentos_por_canal = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['VALOR_PAGO'].sum().reset_index()
            pagamentos_por_canal = pagamentos_por_canal.sort_values('VALOR_PAGO', ascending=False)

            fig_canal = px.bar(
                pagamentos_por_canal,
                x='TIPO_PAGAMENTO', y='VALOR_PAGO',
                title='Valor Arrecadado por Canal de Pagamento',
                labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'VALOR_PAGO': 'Valor Total Pago (R$)'},
                color='TIPO_PAGAMENTO',
                hover_data={'VALOR_PAGO': ':.2f'}
            )
            fig_canal = add_bar_labels(fig_canal, 'valor')
            fig_canal.update_layout(xaxis_title="Canal de Pagamento", yaxis_title="Valor Total Pago (R$)", showlegend=False)
            st.plotly_chart(fig_canal, use_container_width=True, key="fig_canal_aba4")

            st.subheader("Clientes que Pagaram por Canal")

            qtd_por_canal = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['MATRICULA'].nunique().reset_index()
            qtd_por_canal.rename(columns={'MATRICULA': 'Clientes que Pagaram'}, inplace=True)
            qtd_por_canal = qtd_por_canal.sort_values('Clientes que Pagaram', ascending=False)

            fig_canal_qtd = px.bar(
                qtd_por_canal,
                x='TIPO_PAGAMENTO', y='Clientes que Pagaram',
                title='Clientes que Pagaram por Canal',
                labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'Clientes que Pagaram': 'Clientes que Pagaram'},
                color='TIPO_PAGAMENTO'
            )
            fig_canal_qtd = add_bar_labels(fig_canal_qtd, 'qtd')
            fig_canal_qtd.update_layout(xaxis_title="Canal de Pagamento", yaxis_title="Clientes que Pagaram", showlegend=False)
            st.plotly_chart(fig_canal_qtd, use_container_width=True, key="fig_canal_qtd")

            # Tabela canal consolidada
            tab_canal = pd.merge(pagamentos_por_canal, qtd_por_canal, on='TIPO_PAGAMENTO')
            tab_canal.columns = ['Canal de Pagamento', 'Valor Total Pago', 'Clientes que Pagaram']
            tab_canal['Valor Total Pago'] = tab_canal['Valor Total Pago'].apply(fmt_brl)
            st.dataframe(tab_canal, use_container_width=True, hide_index=True)

        else:
            st.info("Coluna 'Tipo Pagamento' não encontrada no arquivo de pagamentos.")

    # ══════════════════════════════════════════════════════════
    # ABA 6 — DETALHES (antiga ABA 5)
    # ══════════════════════════════════════════════════════════
    with aba6:
        if not df_pagamentos_campanha.empty:
            st.subheader("Detalhes dos Pagamentos Atribuídos à Campanha")

            colunas_possiveis = [
                'MATRICULA', 'CIDADE', 'DIRETORIA', 'TELEFONE_ENVIO',
                'DATA_ENVIO', 'DATA_PAGAMENTO', 'VENCIMENTO',
                'VALOR_PAGO', 'DIAS_APOS_ENVIO',
                'TIPO_FATURA', 'UTILIZACAO', 'TIPO_PAGAMENTO'
            ]
            colunas_exibicao = [c for c in colunas_possiveis if c in df_pagamentos_campanha.columns]

            df_detalhes = df_pagamentos_campanha[colunas_exibicao].drop_duplicates(
                subset=['MATRICULA', 'DATA_PAGAMENTO', 'VALOR_PAGO']
            )

            st.dataframe(df_detalhes)

            csv_output = df_detalhes.to_csv(index=False, sep=';', decimal=',')
            st.download_button(
                label="Baixar Detalhes dos Pagamentos da Campanha (CSV)",
                data=csv_output,
                file_name="pagamentos_campanha.csv",
                mime="text/csv",
            )
        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

elif executar_analise and not dados_prontos:
    if not campanhas_selecionadas_ids:
        st.warning("Selecione uma ou mais campanhas antes de executar a análise.")
    elif df_pagamentos is None or df_pagamentos.empty:
        st.warning("Base de pagamentos não disponível ou vazia. Um administrador precisa fazer o upload.")
    elif df_envios_agregado.empty:
        st.warning("Não foi possível carregar os envios das campanhas selecionadas ou a base está vazia.")
    elif df_clientes_agregado.empty:
        st.warning("Não foi possível carregar os clientes das campanhas selecionadas ou a base está vazia.")

elif not executar_analise:
    if not campanhas_selecionadas_ids:
        st.info("👈 Selecione uma ou mais campanhas na barra lateral para começar.")
    else:
        st.info("👈 Clique em **Executar Análise** na barra lateral para gerar os resultados.")
