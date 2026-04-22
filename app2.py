import streamlit as st
import pandas as pd
from datetime import timedelta
import plotly.express as px
import io
import base64
import requests
import json
import uuid

# ══════════════════════════════════════════════════════════════
# SISTEMA DE LOGIN (Mantido como está no seu script)
# ══════════════════════════════════════════════════════════════

def get_users():
    users = {}
    try:
        secrets  = st.secrets["users"]
        prefixes = set()
        for key in secrets:
            if key.endswith("_user"):
                prefixes.add(key[:-5])
        for prefix in prefixes:
            username = secrets.get(f"{prefix}_user", "")
            password = secrets.get(f"{prefix}_password", "")
            role     = secrets.get(f"{prefix}_role", "user")
            if username:
                users[username] = {"password": password, "role": role}
    except Exception:
        pass
    return users

def login_screen():
    st.title("🔐 Login")
    st.markdown("Faça login para acessar o sistema.")
    with st.form("login_form"):
        username  = st.text_input("Usuário")
        password  = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
    if submitted:
        users = get_users()
        if username in users and users[username]["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["username"]  = username
            st.session_state["role"]      = users[username]["role"]
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos.")

def is_admin():
    return st.session_state.get("role") == "admin"

# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÕES GLOBAIS E GITHUB
# ══════════════════════════════════════════════════════════════

# --- Configurações da Página ---
st.set_page_config(layout="wide", page_title="Análise de campanha de cobrança")

# --- Variáveis de Ambiente e Segredos ---
# ATENÇÃO: Substitua "Victor", "analise_campanha_whats_v4" e "ghp_SEU_TOKEN_AQUI"
# pelos seus dados reais do GitHub.
# O token precisa ter permissões de 'repo' para ler e escrever arquivos.
GITHUB_REPO_OWNER = "Victor"
GITHUB_REPO_NAME  = "analise_campanha_whats_v4"
GITHUB_TOKEN      = "ghp_SEU_TOKEN_AQUI" # <--- SUBSTITUA ESTE TOKEN PELO SEU TOKEN REAL DO GITHUB

# Verifique se o token foi substituído
if GITHUB_TOKEN == "ghp_SEU_TOKEN_AQUI":
    st.error("🚨 ERRO: Por favor, substitua 'ghp_SEU_TOKEN_AQUI' pelo seu Personal Access Token (PAT) real do GitHub no código.")
    st.stop() # Interrompe a execução do aplicativo até que o token seja configurado.

GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE INTERAÇÃO COM GITHUB (com tratamento de erro 401)
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600) # Cache por 1 hora
def get_file_from_github(path):
    try:
        response = requests.get(f"{GITHUB_API_URL}/{path}", headers=HEADERS)
        if response.status_code == 401:
            st.error(f"Erro de autenticação (401) ao acessar {path}. Verifique seu GITHUB_TOKEN e suas permissões.")
            return None, None
        response.raise_for_status() # Levanta um erro para outros códigos de status HTTP
        content = response.json()
        if content and 'content' in content:
            # Para arquivos grandes, o GitHub pode retornar um URL de download direto
            if 'download_url' in content and content['size'] > (1024 * 1024): # Se for maior que 1MB
                raw_response = requests.get(content['download_url'], headers={"Authorization": f"token {GITHUB_TOKEN}"})
                raw_response.raise_for_status()
                return raw_response.content, content['sha']
            else:
                return base64.b64decode(content['content']), content['sha']
        return None, None
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao acessar GitHub para {path}: {e}")
        return None, None

def save_file_to_github(path, content_bytes, message):
    sha = None
    # Tenta obter o SHA apenas se o arquivo já existir
    try:
        response = requests.get(f"{GITHUB_API_URL}/{path}", headers=HEADERS)
        if response.status_code == 200:
            sha = response.json().get("sha")
        elif response.status_code == 404:
            sha = None # Arquivo não existe, é uma criação
        elif response.status_code == 401:
            st.error(f"Erro de autenticação (401) ao tentar salvar {path}. Verifique seu GITHUB_TOKEN e suas permissões.")
            return False
        else:
            response.raise_for_status() # Levanta erro para outros status
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao verificar SHA para {path}: {e}")
        return False

    url     = f"{GITHUB_API_URL}/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch":  "main" # Assumindo que a branch principal é 'main'
    }
    if sha:
        payload["sha"] = sha

    try:
        r = requests.put(url, headers=HEADERS, data=json.dumps(payload))
        if r.status_code == 401:
            st.error(f"Erro de autenticação (401) ao tentar salvar {path}. Verifique seu GITHUB_TOKEN e suas permissões.")
            return False
        r.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao salvar no GitHub para {path}: {e}")
        return False

def delete_file_from_github(path, message):
    sha = None
    try:
        response = requests.get(f"{GITHUB_API_URL}/{path}", headers=HEADERS)
        if response.status_code == 200:
            sha = response.json().get("sha")
        elif response.status_code == 404:
            return True # Arquivo já não existe
        elif response.status_code == 401:
            st.error(f"Erro de autenticação (401) ao tentar deletar {path}. Verifique seu GITHUB_TOKEN e suas permissões.")
            return False
        else:
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao verificar SHA para deletar {path}: {e}")
        return False

    if not sha:
        return True # Não há SHA, então o arquivo não existe ou já foi deletado

    url     = f"{GITHUB_API_URL}/{path}"
    payload = {"message": message, "sha": sha, "branch": "main"}
    try:
        r = requests.delete(url, headers=HEADERS, data=json.dumps(payload))
        if r.status_code == 401:
            st.error(f"Erro de autenticação (401) ao tentar deletar {path}. Verifique seu GITHUB_TOKEN e suas permissões.")
            return False
        r.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao deletar do GitHub para {path}: {e}")
        return False

def df_to_parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)
    return buf.getvalue()

def parquet_bytes_to_df(content_bytes):
    if content_bytes is None or len(content_bytes) == 0:
        # st.error("Conteúdo do arquivo está vazio (0 bytes).") # Removido para evitar spam de erro em arquivos vazios esperados
        return pd.DataFrame() # Retorna DataFrame vazio em vez de None
    try:
        buf = io.BytesIO(content_bytes)
        buf.seek(0)
        return pd.read_parquet(buf, engine='pyarrow')
    except Exception as e1:
        try:
            buf = io.BytesIO(content_bytes)
            buf.seek(0)
            return pd.read_parquet(buf, engine='fastparquet')
        except Exception as e2:
            st.error(f"Erro ao ler parquet — pyarrow: {e1} | fastparquet: {e2}")
            return pd.DataFrame() # Retorna DataFrame vazio em caso de erro

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE PROCESSAMENTO DE DADOS (Mantidas como estão no seu script)
# ══════════════════════════════════════════════════════════════

@st.cache_data
def load_and_process_envios(uploaded_file):
    try:
        df = pd.read_excel(uploaded_file)
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

        st.sidebar.success("Arquivo de Envios processado com sucesso!")
        return df_envios
    except Exception as e:
        st.sidebar.error(f"Erro ao processar arquivo de Envios: {e}")
        return None

@st.cache_data
def load_and_process_pagamentos(uploaded_file):
    try:
        df = None
        if uploaded_file.name.endswith('.parquet'):
            df_pag = pd.read_parquet(uploaded_file)

            if 'MATRICULA_PAGAMENTO' in df_pag.columns:
                df_pag['MATRICULA_PAGAMENTO'] = df_pag['MATRICULA_PAGAMENTO'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                df_pag['DATA_PAGAMENTO'] = pd.to_datetime(df_pag['DATA_PAGAMENTO'], errors='coerce', dayfirst=True)
                df_pag.dropna(subset=['DATA_PAGAMENTO'], inplace=True)
                df_pag['VALOR_PAGO'] = pd.to_numeric(df_pag['VALOR_PAGO'], errors='coerce')
                df_pag.dropna(subset=['VALOR_PAGO'], inplace=True)

                if 'TIPO_PAGAMENTO' in df_pag.columns:
                    df_pag['TIPO_PAGAMENTO'] = df_pag['TIPO_PAGAMENTO'].astype(str).str.strip().replace('nan', 'Não informado')
                if 'VENCIMENTO' in df_pag.columns:
                    df_pag['VENCIMENTO'] = pd.to_datetime(df_pag['VENCIMENTO'], errors='coerce', dayfirst=True)
                    df_pag['MES_FATURA']     = df_pag['VENCIMENTO'].dt.month
                    df_pag['ANO_FATURA']     = df_pag['VENCIMENTO'].dt.year
                    df_pag['MES_ANO_FATURA'] = df_pag['VENCIMENTO'].dt.strftime('%m/%Y')
                if 'TIPO_FATURA' in df_pag.columns:
                    df_pag['TIPO_FATURA'] = df_pag['TIPO_FATURA'].astype(str).str.strip().replace('nan', 'Não informado')
                if 'UTILIZACAO' in df_pag.columns:
                    df_pag['UTILIZACAO'] = df_pag['UTILIZACAO'].astype(str).str.strip().replace('nan', 'Não informado')

                return df_pag
            else:
                df = df_pag
                df.columns = range(len(df.columns))

        elif uploaded_file.name.endswith('.csv'):
            for encoding in ['latin1', 'utf-8', 'cp1252']:
                try:
                    df = pd.read_csv(uploaded_file, sep=';', decimal=',', encoding=encoding, header=None)
                    uploaded_file.seek(0)
                    break
                except Exception:
                    uploaded_file.seek(0)
                    continue
        elif uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file, header=None)
        else:
            raise ValueError("Formato não suportado. Use .csv, .xlsx ou .parquet.")

        if df is None or df.empty:
            st.sidebar.error("Arquivo de Pagamentos está vazio ou não pôde ser lido.")
            return None

        if df.shape[1] < 10:
            st.sidebar.error(f"Arquivo de Pagamentos: Esperava pelo menos 10 colunas, mas encontrou {df.shape[1]}.")
            return None

        col_indices = [0, 5, 8]
        col_names   = ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']

        if df.shape[1] > 12:
            col_indices.append(12)
            col_names.append('TIPO_PAGAMENTO')

        df_pagamentos = df.iloc[:, col_indices].copy()
        df_pagamentos.columns = col_names

        IDX_VENCIMENTO  = 4
        IDX_TIPO_FATURA = 11
        IDX_UTILIZACAO  = 9

        if df.shape[1] > IDX_VENCIMENTO:
            df_pagamentos['VENCIMENTO'] = df.iloc[:, IDX_VENCIMENTO].values

        if df.shape[1] > IDX_TIPO_FATURA:
            df_pagamentos['TIPO_FATURA'] = df.iloc[:, IDX_TIPO_FATURA].values

        if df.shape[1] > IDX_UTILIZACAO:
            df_pagamentos['UTILIZACAO'] = df.iloc[:, IDX_UTILIZACAO].values

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

        st.sidebar.success("Arquivo de Pagamentos processado com sucesso!")
        return df_pagamentos
    except Exception as e:
        st.sidebar.error(f"Erro ao processar arquivo de Pagamentos: {e}")
        return None

@st.cache_data
def load_and_process_clientes(uploaded_file):
    try:
        df = pd.read_excel(uploaded_file)

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

        st.sidebar.success("Arquivo de Clientes processado com sucesso!")
        return df_clientes
    except Exception as e:
        st.sidebar.error(f"Erro ao processar arquivo de Clientes: {e}")
        return None

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE GERENCIAMENTO DE CAMPANHAS
# ══════════════════════════════════════════════════════════════

META_PATH = "data/campanhas_meta.parquet"
PAG_PATH  = "data/pagamentos.parquet"

def load_campanhas_meta():
    content, _ = get_file_from_github(META_PATH)
    df = parquet_bytes_to_df(content)
    if df.empty:
        return pd.DataFrame(columns=['id', 'nome', 'criado_em', 'total_envios', 'total_clientes'])
    return df

def save_campanha(nome, df_envios, df_clientes):
    campanha_id = str(uuid.uuid4())[:8]
    ok_envios = save_file_to_github(
        f"data/campanhas/{campanha_id}_envios.parquet",
        df_to_parquet_bytes(df_envios),
        f"Campanha {nome}: envios"
    )
    if not ok_envios:
        return None, "Erro ao salvar envios no GitHub."
    ok_clientes = save_file_to_github(
        f"data/campanhas/{campanha_id}_clientes.parquet",
        df_to_parquet_bytes(df_clientes),
        f"Campanha {nome}: clientes"
    )
    if not ok_clientes:
        return None, "Envios salvos, mas erro ao salvar clientes."
    df_meta = load_campanhas_meta()
    nova = pd.DataFrame([{
        'id':             campanha_id,
        'nome':           nome,
        'criado_em':      pd.Timestamp.now(),
        'total_envios':   df_envios['TELEFONE_ENVIO'].nunique(),
        'total_clientes': len(df_clientes)
    }])
    df_meta = pd.concat([df_meta, nova], ignore_index=True)
    ok_meta = save_file_to_github(
        META_PATH,
        df_to_parquet_bytes(df_meta),
        f"Meta: campanha {nome} criada"
    )
    if not ok_meta:
        return None, "Arquivos salvos, mas erro ao salvar metadados."
    return campanha_id, None

def update_campanha_data(campanha_id, nome_campanha, new_df_envios, new_df_clientes):
    # Carregar dados existentes
    existing_df_envios = load_campanha_envios(campanha_id)
    existing_df_clientes = load_campanha_clientes(campanha_id)

    # Combinar e deduplicar envios
    combined_df_envios = pd.concat([existing_df_envios, new_df_envios], ignore_index=True)
    combined_df_envios.drop_duplicates(subset=['TELEFONE_ENVIO', 'DATA_ENVIO'], inplace=True)

    # Combinar e deduplicar clientes
    combined_df_clientes = pd.concat([existing_df_clientes, new_df_clientes], ignore_index=True)
    combined_df_clientes.drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], inplace=True)

    # Salvar dados atualizados
    ok_envios = save_file_to_github(
        f"data/campanhas/{campanha_id}_envios.parquet",
        df_to_parquet_bytes(combined_df_envios),
        f"Campanha {nome_campanha}: envios atualizados"
    )
    if not ok_envios:
        return False, "Erro ao atualizar envios no GitHub."

    ok_clientes = save_file_to_github(
        f"data/campanhas/{campanha_id}_clientes.parquet",
        df_to_parquet_bytes(combined_df_clientes),
        f"Campanha {nome_campanha}: clientes atualizados"
    )
    if not ok_clientes:
        return False, "Envios atualizados, mas erro ao atualizar clientes."

    # Atualizar metadados
    df_meta = load_campanhas_meta()
    idx = df_meta[df_meta['id'] == campanha_id].index
    if not idx.empty:
        df_meta.loc[idx, 'total_envios'] = combined_df_envios['TELEFONE_ENVIO'].nunique()
        df_meta.loc[idx, 'total_clientes'] = len(combined_df_clientes)
        ok_meta = save_file_to_github(
            META_PATH,
            df_to_parquet_bytes(df_meta),
            f"Meta: campanha {nome_campanha} atualizada"
        )
        if not ok_meta:
            return False, "Arquivos atualizados, mas erro ao salvar metadados."
    return True, None


def load_campanha_envios(campanha_id):
    path = f"data/campanhas/{campanha_id}_envios.parquet"
    content, _ = get_file_from_github(path)
    return parquet_bytes_to_df(content)

def load_campanha_clientes(campanha_id):
    path = f"data/campanhas/{campanha_id}_clientes.parquet"
    content, _ = get_file_from_github(path)
    return parquet_bytes_to_df(content)

def delete_campanha(campanha_id, nome):
    df_meta = load_campanhas_meta()
    df_meta = df_meta[df_meta['id'] != campanha_id]
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} removida")
    delete_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet",   f"Campanha {nome}: envios removidos")
    delete_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet", f"Campanha {nome}: clientes removidos")

# ══════════════════════════════════════════════════════════════
# FUNÇÕES AUXILIARES DE FORMATAÇÃO E GRÁFICOS
# ══════════════════════════════════════════════════════════════

def fmt_brl(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def add_bar_labels(fig, formato='valor'):
    for trace in fig.data:
        if hasattr(trace, 'y') and trace.y is not None:
            if formato == 'valor':
                texts = [fmt_brl(v) if v is not None else '' for v in trace.y]
            else:
                texts = [str(int(v)) if v is not None else '' for v in trace.y]
            trace.text = texts
            trace.textposition = 'outside'
            trace.textfont = dict(size=11)
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    return fig

# ══════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    login_screen()
else:
    st.sidebar.title(f"Bem-vindo, {st.session_state['username']}!")
    if st.sidebar.button("Sair"):
        st.session_state["logged_in"] = False
        st.session_state["username"]  = ""
        st.session_state["role"]      = ""
        st.rerun()

    st.sidebar.header("⚙️ Configurações")

    # Carregar metadados das campanhas
    df_meta_campanhas = load_campanhas_meta()
    campanhas_disponiveis = df_meta_campanhas.to_dict('records')
    campanha_nomes = {c['nome']: c['id'] for c in campanhas_disponiveis}

    # Seleção de campanhas para análise
    st.sidebar.subheader("Análise de Campanhas")
    campanhas_selecionadas_nomes = st.sidebar.multiselect(
        "Selecione as campanhas para análise:",
        options=list(campanha_nomes.keys()),
        key="multiselect_campanhas"
    )
    campanhas_selecionadas_ids = [campanha_nomes[nome] for nome in campanhas_selecionadas_nomes]

    janela_dias = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7, key="janela_dias_slider")
    executar_analise = st.sidebar.button("▶️ Executar Análise", use_container_width=True, key="executar_analise_btn")

    df_envios_agregado   = pd.DataFrame()
    df_clientes_agregado = pd.DataFrame()
    df_pagamentos        = pd.DataFrame()

    if campanhas_selecionadas_ids:
        with st.spinner("Carregando dados das campanhas selecionadas..."):
            lista_df_envios = []
            lista_df_clientes = []
            for c_id in campanhas_selecionadas_ids:
                df_envios_temp = load_campanha_envios(c_id)
                if not df_envios_temp.empty:
                    lista_df_envios.append(df_envios_temp)
                df_cli_temp = load_campanha_clientes(c_id)
                if not df_cli_temp.empty:
                    lista_df_clientes.append(df_cli_temp)

            if lista_df_envios:
                df_envios_agregado = pd.concat(lista_df_envios, ignore_index=True)
                df_envios_agregado.drop_duplicates(subset=['TELEFONE_ENVIO', 'DATA_ENVIO'], inplace=True)
                st.sidebar.success(f"✅ Envios agregados ({len(df_envios_agregado):,} registros únicos)")
            else:
                st.sidebar.error("Erro ao carregar envios das campanhas selecionadas ou bases vazias.")

            if lista_df_clientes:
                df_clientes_agregado = pd.concat(lista_df_clientes, ignore_index=True)
                df_clientes_agregado.drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], inplace=True)
                st.sidebar.success(f"✅ Clientes agregados ({len(df_clientes_agregado):,} registros únicos)")
            else:
                st.sidebar.error("Erro ao carregar clientes das campanhas selecionadas ou bases vazias.")

        # Carregar base de pagamentos (global, não por campanha)
        with st.spinner("Carregando base de pagamentos..."):
            content_pagamentos, _ = get_file_from_github(PAG_PATH)
            df_pagamentos = parquet_bytes_to_df(content_pagamentos)
            if not df_pagamentos.empty:
                st.sidebar.success(f"✅ Base de pagamentos carregada ({len(df_pagamentos):,} registros)")
            else:
                st.sidebar.warning("Base de pagamentos não disponível ou vazia.")
    else:
        st.sidebar.info("Nenhuma campanha selecionada para análise.")

    dados_prontos = (
        not df_envios_agregado.empty and
        not df_clientes_agregado.empty and
        not df_pagamentos.empty
    )

    # ══════════════════════════════════════════════════════════════
    # GERENCIAMENTO DE CAMPANHAS (APENAS PARA ADMIN)
    # ══════════════════════════════════════════════════════════════
    if is_admin():
        st.sidebar.markdown("---")
        st.sidebar.subheader("🛠️ Gerenciar Campanhas")

        gerenciar_tab = st.sidebar.radio(
            "Ações de Gerenciamento:",
            ["Criar Nova Campanha", "Adicionar Dados a Campanha Existente", "Remover Campanha"],
            key="gerenciar_tab_radio"
        )

        if gerenciar_tab == "Criar Nova Campanha":
            st.sidebar.markdown("##### Criar Nova Campanha")
            nome_nova_campanha = st.sidebar.text_input("Nome da nova campanha", key="nome_nova_campanha_input")
            uploaded_envios_nova = st.sidebar.file_uploader("Upload Envios (Nova Campanha)", type=["xlsx"], key="upload_envios_nova")
            uploaded_clientes_nova = st.sidebar.file_uploader("Upload Clientes (Nova Campanha)", type=["xlsx"], key="upload_clientes_nova")

            if st.sidebar.button("Salvar Nova Campanha", key="salvar_nova_campanha_btn"):
                if nome_nova_campanha and uploaded_envios_nova and uploaded_clientes_nova:
                    with st.spinner("Processando e salvando nova campanha..."):
                        df_envios_proc = load_and_process_envios(uploaded_envios_nova)
                        df_clientes_proc = load_and_process_clientes(uploaded_clientes_nova)

                        if df_envios_proc is not None and df_clientes_proc is not None:
                            camp_id, error = save_campanha(nome_nova_campanha, df_envios_proc, df_clientes_proc)
                            if camp_id:
                                st.sidebar.success(f"Campanha '{nome_nova_campanha}' criada com sucesso! ID: {camp_id}")
                                st.cache_data.clear() # Limpa cache para recarregar metadados
                                st.rerun()
                            else:
                                st.sidebar.error(f"Falha ao criar campanha: {error}")
                        else:
                            st.sidebar.error("Erro no processamento dos arquivos para a nova campanha.")
                else:
                    st.sidebar.warning("Preencha o nome e faça upload de ambos os arquivos para criar a campanha.")

        elif gerenciar_tab == "Adicionar Dados a Campanha Existente":
            st.sidebar.markdown("##### Adicionar Dados a Campanha Existente")
            if not campanhas_disponiveis:
                st.sidebar.info("Nenhuma campanha existente para adicionar dados.")
            else:
                campanha_para_atualizar_nome = st.sidebar.selectbox(
                    "Selecione a campanha para adicionar dados:",
                    options=[c['nome'] for c in campanhas_disponiveis],
                    key="select_campanha_add_data"
                )
                campanha_para_atualizar_id = next((c['id'] for c in campanhas_disponiveis if c['nome'] == campanha_para_atualizar_nome), None)

                uploaded_envios_add = st.sidebar.file_uploader("Upload Envios (Adicionar)", type=["xlsx"], key="upload_envios_add")
                uploaded_clientes_add = st.sidebar.file_uploader("Upload Clientes (Adicionar)", type=["xlsx"], key="upload_clientes_add")

                if st.sidebar.button("Adicionar Dados à Campanha", key="add_data_campanha_btn"):
                    if campanha_para_atualizar_id and uploaded_envios_add and uploaded_clientes_add:
                        with st.spinner(f"Adicionando dados à campanha '{campanha_para_atualizar_nome}'..."):
                            new_df_envios_proc = load_and_process_envios(uploaded_envios_add)
                            new_df_clientes_proc = load_and_process_clientes(uploaded_clientes_add)

                            if new_df_envios_proc is not None and new_df_clientes_proc is not None:
                                success, error = update_campanha_data(
                                    campanha_para_atualizar_id,
                                    campanha_para_atualizar_nome,
                                    new_df_envios_proc,
                                    new_df_clientes_proc
                                )
                                if success:
                                    st.sidebar.success(f"Dados adicionados e campanha '{campanha_para_atualizar_nome}' atualizada com sucesso!")
                                    st.cache_data.clear() # Limpa cache para recarregar metadados e dados
                                    st.rerun()
                                else:
                                    st.sidebar.error(f"Falha ao adicionar dados: {error}")
                            else:
                                st.sidebar.error("Erro no processamento dos arquivos para adicionar dados.")
                    else:
                        st.sidebar.warning("Selecione uma campanha e faça upload de ambos os arquivos para adicionar dados.")


        elif gerenciar_tab == "Remover Campanha":
            st.sidebar.markdown("##### Remover Campanha")
            if not campanhas_disponiveis:
                st.sidebar.info("Nenhuma campanha para remover.")
            else:
                campanha_para_remover_nome = st.sidebar.selectbox(
                    "Selecione a campanha para remover:",
                    options=[c['nome'] for c in campanhas_disponiveis],
                    key="select_campanha_remover"
                )
                campanha_para_remover_id = next((c['id'] for c in campanhas_disponiveis if c['nome'] == campanha_para_remover_nome), None)

                if st.sidebar.button("Remover Campanha", key="remover_campanha_btn"):
                    if campanha_para_remover_id:
                        if st.sidebar.checkbox(f"Confirmar remoção da campanha '{campanha_para_remover_nome}'?", key="confirm_delete"):
                            with st.spinner(f"Removendo campanha '{campanha_para_remover_nome}'..."):
                                delete_campanha(campanha_para_remover_id, campanha_para_remover_nome)
                                st.sidebar.success(f"Campanha '{campanha_para_remover_nome}' removida com sucesso!")
                                st.cache_data.clear() # Limpa cache para recarregar metadados
                                st.rerun()
                        else:
                            st.sidebar.warning("Confirme a remoção da campanha.")
                    else:
                        st.sidebar.warning("Selecione uma campanha para remover.")

    # ══════════════════════════════════════════════════════════════
    # ANÁLISE PRINCIPAL
    # ══════════════════════════════════════════════════════════════

    if executar_analise and dados_prontos:
        # Total de clientes notificados (únicos em todas as campanhas selecionadas)
        total_clientes_notificados = df_envios_agregado['TELEFONE_ENVIO'].nunique()

        # Total da dívida dos notificados (clientes únicos)
        df_telefones_unicos_envios = df_envios_agregado[['TELEFONE_ENVIO']].drop_duplicates()
        df_lookup_divida = pd.merge(
            df_telefones_unicos_envios,
            df_clientes_agregado[['TELEFONE_CLIENTE', 'SITUACAO']],
            left_on='TELEFONE_ENVIO',
            right_on='TELEFONE_CLIENTE',
            how='left'
        )
        total_divida_notificados = df_lookup_divida['SITUACAO'].sum()

        # 1. Cruzar Envios com Clientes
        df_campanha = pd.merge(
            df_envios_agregado,
            df_clientes_agregado,
            left_on='TELEFONE_ENVIO',
            right_on='TELEFONE_CLIENTE',
            how='left'
        )

        df_campanha.dropna(subset=['MATRICULA_CLIENTE'], inplace=True)
        df_campanha.rename(columns={'MATRICULA_CLIENTE': 'MATRICULA'}, inplace=True)
        df_campanha.drop(columns=['TELEFONE_CLIENTE'], inplace=True)

        # Identificar clientes notificados múltiplas vezes
        contagem_notificacoes = df_campanha.groupby('MATRICULA')['DATA_ENVIO'].nunique().reset_index()
        contagem_notificacoes.rename(columns={'DATA_ENVIO': 'NUM_NOTIFICACOES'}, inplace=True)
        clientes_multiplas_notificacoes = contagem_notificacoes[contagem_notificacoes['NUM_NOTIFICACOES'] > 1]

        st.subheader("Visão Geral das Campanhas Selecionadas")
        st.info(f"Clientes notificados múltiplas vezes: {len(clientes_multiplas_notificacoes)} de {total_clientes_notificados} clientes únicos.")

        # Remover duplicatas de notificação para a análise de pagamento
        df_campanha_unique_notifications = df_campanha.drop_duplicates(subset=['MATRICULA', 'DATA_ENVIO'])

        if not df_campanha_unique_notifications.empty:

            # 2. Cruzar com Pagamentos
            df_resultados = pd.merge(
                df_campanha_unique_notifications,
                df_pagamentos,
                left_on='MATRICULA',
                right_on='MATRICULA_PAGAMENTO',
                how='left'
            )

            # Filtrar pagamentos dentro da janela
            df_pagamentos_campanha = df_resultados[
                (df_resultados['DATA_PAGAMENTO'] > df_resultados['DATA_ENVIO']) &
                (df_resultados['DATA_PAGAMENTO'] <= df_resultados['DATA_ENVIO'] + timedelta(days=janela_dias))
            ].copy()

            # Calcular DIAS_APOS_ENVIO antes das abas
            if not df_pagamentos_campanha.empty:
                df_pagamentos_campanha['DIAS_APOS_ENVIO'] = (
                    df_pagamentos_campanha['DATA_PAGAMENTO'] - df_pagamentos_campanha['DATA_ENVIO']
                ).dt.days

            # Métricas
            clientes_que_pagaram_matriculas = df_pagamentos_campanha['MATRICULA'].nunique()
            valor_total_arrecadado   = df_pagamentos_campanha['VALOR_PAGO'].sum() if not df_pagamentos_campanha.empty else 0
            taxa_eficiencia_clientes = (clientes_que_pagaram_matriculas / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0
            taxa_eficiencia_valor    = (valor_total_arrecadado / total_divida_notificados * 100) if total_divida_notificados > 0 else 0
            ticket_medio             = (valor_total_arrecadado / clientes_que_pagaram_matriculas) if clientes_que_pagaram_matriculas > 0 else 0
            custo_campanha           = total_clientes_notificados * 0.05 # Custo hipotético
            roi                      = ((valor_total_arrecadado - custo_campanha) / custo_campanha *100) if custo_campanha > 0 else 0

            # ── ABAS ──────────────────────────────────────────
            aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
                "📊 Visão Geral",
                "👥 Clientes Notificados Múltiplas Vezes", # Nova aba
                "🏙️ Cidade e Diretoria",
                "📅 Análise das Faturas",
                "💳 Canal de Pagamento",
                "📋 Detalhes"
            ])

            # ══════════════════════════════════════════════════
            # ABA 1 — VISÃO GERAL
            # ══════════════════════════════════════════════════
            with aba1:
                st.subheader("Resultados da Análise das Campanhas Selecionadas")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total de clientes notificados (únicos)", f"{total_clientes_notificados}")
                with col2:
                    st.metric("Clientes que pagaram na janela", f"{clientes_que_pagaram_matriculas}")
                with col3:
                    st.metric("Taxa de eficiência (clientes)", f"{taxa_eficiencia_clientes:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

                col4, col5, col6 = st.columns(3)
                with col4:
                    st.metric("Valor total arrecadado na janela", f"R$ {valor_total_arrecadado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
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
                        fig_canal.update_layout(xaxis_title="Canal de Pagamento", yaxis_title="Valor Total Pago (R$)", showlegend=False)
                        st.plotly_chart(fig_canal, use_container_width=True, key="fig_canal_aba1")

                        #Tabela canal aba1
                        tab_canal_v1 = pagamentos_por_canal.copy()
                        tab_canal_v1.columns = ['Canal de Pagamento', 'Valor Total Pago']
                        tab_canal_v1['Valor Total Pago'] = tab_canal_v1['Valor Total Pago'].apply(fmt_brl)
                        st.dataframe(tab_canal_v1, use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

            # ══════════════════════════════════════════════════
            # ABA 2 — CLIENTES NOTIFICADOS MÚLTIPLAS VEZES (NOVA ABA)
            # ══════════════════════════════════════════════════
            with aba2:
                st.subheader("Clientes Notificados Múltiplas Vezes")
                if not clientes_multiplas_notificacoes.empty:
                    st.write(f"Total de clientes notificados mais de uma vez: {len(clientes_multiplas_notificacoes)}")
                    st.dataframe(clientes_multiplas_notificacoes, use_container_width=True, hide_index=True)

                    # Opcional: Mostrar detalhes de notificações para esses clientes
                    st.markdown("---")
                    st.subheader("Detalhes das Notificações Múltiplas")
                    df_multi_notif_details = df_campanha[df_campanha['MATRICULA'].isin(clientes_multiplas_notificacoes['MATRICULA'])].sort_values(['MATRICULA', 'DATA_ENVIO'])
                    st.dataframe(df_multi_notif_details[['MATRICULA', 'TELEFONE_ENVIO', 'DATA_ENVIO', 'CIDADE', 'DIRETORIA']], use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum cliente foi notificado múltiplas vezes nas campanhas selecionadas.")

            # ══════════════════════════════════════════════════
            # ABA 3 — CIDADE E DIRETORIA (antiga ABA 2)
            # ══════════════════════════════════════════════════
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
                        fig_cidade_valor.update_layout(xaxis_title="Cidade", yaxis_title="Valor Arrecadado (R$)")
                        st.plotly_chart(fig_cidade_valor, use_container_width=True, key="fig_cidade_valor")

                        fig_cidade_clientes = px.bar(
                            cidade_resumo,
                            x='CIDADE', y='Clientes_que_Pagaram',
                            title='Clientes que Pagaram por Cidade',
                            labels={'CIDADE': 'Cidade', 'Clientes_que_Pagaram': 'Clientes que Pagaram'}
                        )
                        fig_cidade_clientes.update_layout(xaxis_title="Cidade", yaxis_title="Clientes que Pagaram")
                        st.plotly_chart(fig_cidade_clientes, use_container_width=True, key="fig_cidade_clientes")

                        # Tabela cidade
                        tab_cidade = cidade_resumo.copy()
                        tab_cidade.columns = ['Cidade', 'Clientes que Pagaram', 'Valor Arrecadado']
                        tab_cidade['Valor Arrecadado'] = tab_cidade['Valor Arrecadado'].apply(fmt_brl)
                        st.dataframe(tab_cidade, use_container_width=True, hide_index=True)

                        if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                            st.subheader("Tipo de Pagamento por Cidade")
                            cidade_canal = df_pagamentos_campanha.groupby(['CIDADE', 'TIPO_PAGAMENTO'])['VALOR_PAGO'].sum().reset_index()
                            fig_cidade_canal = px.bar(
                                cidade_canal,
                                x='CIDADE', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                                title='Valor Pago por Cidade e Canal de Pagamento',
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
                        fig_diretoria_valor.update_layout(xaxis_title="Diretoria", yaxis_title="Valor Arrecadado (R$)")
                        st.plotly_chart(fig_diretoria_valor, use_container_width=True, key="fig_diretoria_valor")

                        fig_diretoria_clientes = px.bar(
                            diretoria_resumo,
                            x='DIRETORIA', y='Clientes_que_Pagaram',
                            title='Clientes que Pagaram por Diretoria',
                            labels={'DIRETORIA': 'Diretoria', 'Clientes_que_Pagaram': 'Clientes que Pagaram'}
                        )
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
                        st.info("As colunas 'CIDADE' e 'DIRETORIA' não foram encontradas nos dados de clientes.")
                else:
                    st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

            # ══════════════════════════════════════════════════
            # ABA 4 — ANÁLISE DAS FATURAS (antiga ABA 3)
            # ══════════════════════════════════════════════════
            with aba4:
                if not df_pagamentos_campanha.empty:
                    if 'MES_ANO_FATURA' in df_pagamentos_campanha.columns:
                        st.subheader("Valor Pago por Mês/Ano da Fatura")

                        mes_ano_resumo = df_pagamentos_campanha.groupby('MES_ANO_FATURA')['VALOR_PAGO'].sum().reset_index()
                        mes_ano_resumo['MES_ANO_ORDEM'] = pd.to_datetime(mes_ano_resumo['MES_ANO_FATURA'], format='%m/%Y')
                        mes_ano_resumo = mes_ano_resumo.sort_values('MES_ANO_ORDEM').drop(columns='MES_ANO_ORDEM')

                        fig_mes_ano = px.bar(
                            mes_ano_resumo,
                            x='MES_ANO_FATURA', y='VALOR_PAGO',
                            title='Valor Pago por Mês/Ano da Fatura',
                            labels={'MES_ANO_FATURA': 'Mês/Ano da Fatura', 'VALOR_PAGO': 'Valor Pago (R$)'},
                            hover_data={'VALOR_PAGO': ':.2f'}
                        )
                        fig_mes_ano.update_layout(xaxis_title="Mês/Ano da Fatura", yaxis_title="Valor Pago (R$)")
                        st.plotly_chart(fig_mes_ano, use_container_width=True, key="fig_mes_ano")

                        # Tabela mes/ano
                        tab_mes_ano = mes_ano_resumo.copy()
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

            # ══════════════════════════════════════════════════
            # ABA 5 — CANAL DE PAGAMENTO (antiga ABA 4)
            # ══════════════════════════════════════════════════
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

            # ══════════════════════════════════════════════════
            # ABA 6 — DETALHES (antiga ABA 5)
            # ══════════════════════════════════════════════════
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
        elif df_pagamentos.empty:
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
