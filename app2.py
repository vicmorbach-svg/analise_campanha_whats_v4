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

# --- Funções para obter configurações do GitHub ---
def get_github_config():
    try:
        repo_owner   = st.secrets["github"]["repo_owner"]
        repo_name    = st.secrets["github"]["repo_name"]
        access_token = st.secrets["github"]["access_token"]
        branch       = st.secrets["github"].get("branch", "main") # Default to 'main'
        return repo_owner, repo_name, access_token, branch
    except KeyError as e:
        st.error(f"Erro de configuração: A chave '{e}' não foi encontrada em `st.secrets['github']`. "
                 "Por favor, configure seus segredos do GitHub corretamente no Streamlit Cloud.")
        st.stop()
    except Exception as e:
        st.error(f"Erro ao carregar configurações do GitHub: {e}. "
                 "Verifique se seus segredos do GitHub estão configurados corretamente.")
        st.stop()

# Carregar configurações do GitHub no início
REPO_OWNER, REPO_NAME, ACCESS_TOKEN, BRANCH = get_github_config()

GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents"
HEADERS = {
    "Authorization": f"token {ACCESS_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE INTERAÇÃO COM GITHUB (com tratamento de erro 401)
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600) # Cache por 1 hora
def get_file_from_github(path):
    try:
        response = requests.get(f"{GITHUB_API_URL}/{path}?ref={BRANCH}", headers=HEADERS)
        if response.status_code == 200:
            content = base64.b64decode(response.json()['content'])
            sha = response.json()['sha']
            return content, sha
        elif response.status_code == 404:
            return None, None # Arquivo não encontrado
        elif response.status_code == 401:
            st.error("Erro de autenticação (401): Seu token do GitHub pode estar inválido ou sem permissões. "
                     "Verifique o 'access_token' em `st.secrets` e as permissões do token (escopo 'repo').")
            st.stop()
        else:
            st.error(f"Erro ao acessar GitHub para {path}: {response.status_code} {response.text}")
            st.stop()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão ao GitHub para {path}: {e}")
        st.stop()
    return None, None

def get_file_sha(path):
    try:
        response = requests.get(f"{GITHUB_API_URL}/{path}?ref={BRANCH}", headers=HEADERS)
        if response.status_code == 200:
            return response.json()['sha']
        elif response.status_code == 404:
            return None
        elif response.status_code == 401:
            st.error("Erro de autenticação (401): Seu token do GitHub pode estar inválido ou sem permissões. "
                     "Verifique o 'access_token' em `st.secrets` e as permissões do token (escopo 'repo').")
            st.stop()
        else:
            st.error(f"Erro ao obter SHA do GitHub para {path}: {response.status_code} {response.text}")
            st.stop()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão ao GitHub para {path}: {e}")
        st.stop()
    return None

def save_file_to_github(path, content_bytes, message):
    sha = get_file_sha(path)
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": BRANCH
    }
    if sha:
        payload["sha"] = sha

    try:
        response = requests.put(f"{GITHUB_API_URL}/{path}", headers=HEADERS, data=json.dumps(payload))
        if response.status_code in [200, 201]:
            st.cache_data.clear() # Limpa o cache após salvar
            return True
        elif response.status_code == 401:
            st.error("Erro de autenticação (401): Seu token do GitHub pode estar inválido ou sem permissões. "
                     "Verifique o 'access_token' em `st.secrets` e as permissões do token (escopo 'repo').")
            return False
        else:
            st.error(f"Erro ao salvar {path} no GitHub: {response.status_code} {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão ao GitHub ao salvar {path}: {e}")
        return False

def delete_file_from_github(path, message):
    sha = get_file_sha(path)
    if not sha:
        return True # Arquivo já não existe
    payload = {"message": message, "sha": sha, "branch": BRANCH}
    try:
        response = requests.delete(f"{GITHUB_API_URL}/{path}", headers=HEADERS, data=json.dumps(payload))
        if response.status_code == 200:
            st.cache_data.clear() # Limpa o cache após deletar
            return True
        elif response.status_code == 401:
            st.error("Erro de autenticação (401): Seu token do GitHub pode estar inválido ou sem permissões. "
                     "Verifique o 'access_token' em `st.secrets` e as permissões do token (escopo 'repo').")
            return False
        else:
            st.error(f"Erro ao deletar {path} do GitHub: {response.status_code} {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão ao GitHub ao deletar {path}: {e}")
        return False

def df_to_parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)
    return buf.getvalue()

def parquet_bytes_to_df(content_bytes):
    if content_bytes is None or len(content_bytes) == 0:
        return pd.DataFrame() # Retorna DataFrame vazio para conteúdo vazio
    try:
        buf = io.BytesIO(content_bytes)
        buf.seek(0)
        return pd.read_parquet(buf, engine='pyarrow')
    except Exception as e:
        st.error(f"Erro ao ler arquivo Parquet: {e}")
        return pd.DataFrame()

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE PROCESSAMENTO DE DADOS (Mantidas como estão)
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

# Função auxiliar para formatar valores em R$
def fmt_brl(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Função auxiliar para adicionar rótulos de valor nas barras
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
# CAMPANHAS (META, CARREGAMENTO, SALVAMENTO)
# ══════════════════════════════════════════════════════════════

META_PATH = "data/campanhas_meta.parquet"
PAG_PATH  = "data/pagamentos.parquet"

@st.cache_data(ttl=600) # Cache por 10 minutos
def load_campanhas_meta():
    content, _ = get_file_from_github(META_PATH)
    if content:
        df = parquet_bytes_to_df(content)
        if not df.empty:
            df['criado_em'] = pd.to_datetime(df['criado_em'])
            return df
    return pd.DataFrame(columns=['id', 'nome', 'criado_em', 'total_envios', 'total_clientes'])

@st.cache_data(ttl=600)
def load_pagamentos_geral():
    content, _ = get_file_from_github(PAG_PATH)
    if content:
        return parquet_bytes_to_df(content)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def load_campanha_envios(campanha_id):
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet")
    return parquet_bytes_to_df(content)

@st.cache_data(ttl=600)
def load_campanha_clientes(campanha_id):
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet")
    return parquet_bytes_to_df(content)

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
        return None, "Erro ao atualizar metadados da campanha."
    return campanha_id, "Campanha criada com sucesso!"

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
        f"Campanha {nome_campanha}: dados de envios atualizados"
    )
    if not ok_envios:
        return False, "Erro ao atualizar envios no GitHub."

    ok_clientes = save_file_to_github(
        f"data/campanhas/{campanha_id}_clientes.parquet",
        df_to_parquet_bytes(combined_df_clientes),
        f"Campanha {nome_campanha}: dados de clientes atualizados"
    )
    if not ok_clientes:
        return False, "Erro ao atualizar clientes no GitHub."

    # Atualizar metadados
    df_meta = load_campanhas_meta()
    idx = df_meta[df_meta['id'] == campanha_id].index
    if not idx.empty:
        df_meta.loc[idx, 'total_envios'] = combined_df_envios['TELEFONE_ENVIO'].nunique()
        df_meta.loc[idx, 'total_clientes'] = len(combined_df_clientes)
        ok_meta = save_file_to_github(
            META_PATH,
            df_to_parquet_bytes(df_meta),
            f"Meta: campanha {nome_campanha} metadados atualizados"
        )
        if not ok_meta:
            return False, "Erro ao atualizar metadados da campanha."
    else:
        return False, "Metadados da campanha não encontrados para atualização."

    return True, "Dados da campanha atualizados com sucesso!"

def delete_campanha(campanha_id, nome_campanha):
    ok_envios = delete_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet", f"Campanha {nome_campanha}: envios deletados")
    ok_clientes = delete_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet", f"Campanha {nome_campanha}: clientes deletados")

    if not ok_envios or not ok_clientes:
        return False, "Erro ao deletar arquivos da campanha."

    df_meta = load_campanhas_meta()
    df_meta = df_meta[df_meta['id'] != campanha_id]
    ok_meta = save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome_campanha} deletada")

    if not ok_meta:
        return False, "Erro ao atualizar metadados após deletar campanha."
    return True, "Campanha deletada com sucesso!"

# ══════════════════════════════════════════════════════════════
# INTERFACE DO USUÁRIO
# ══════════════════════════════════════════════════════════════

# --- Login ---
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    login_screen()
else:
    st.sidebar.write(f"Bem-vindo, {st.session_state['username']}!")
    if st.sidebar.button("Sair"):
        st.session_state["logged_in"] = False
        st.rerun()

    # --- Gerenciamento de Campanhas (Apenas para Admin) ---
    if is_admin():
        st.sidebar.header("🛠️ Gerenciar Campanhas")
        gerenciar_tab = st.sidebar.radio("Ações", ["Criar Nova", "Atualizar Existente", "Deletar"], key="gerenciar_campanha_radio")

        if gerenciar_tab == "Criar Nova":
            with st.sidebar.expander("➕ Criar Nova Campanha"):
                nome_nova_campanha = st.text_input("Nome da Nova Campanha", key="nome_nova_campanha_input")
                uploaded_envios_new = st.file_uploader("Upload Base de Envios (.xlsx)", type=["xlsx"], key="upload_envios_new")
                uploaded_clientes_new = st.file_uploader("Upload Base de Clientes (.xlsx)", type=["xlsx"], key="upload_clientes_new")

                if st.button("Criar Campanha", key="criar_campanha_btn"):
                    if nome_nova_campanha and uploaded_envios_new and uploaded_clientes_new:
                        df_envios_new = load_and_process_envios(uploaded_envios_new)
                        df_clientes_new = load_and_process_clientes(uploaded_clientes_new)

                        if df_envios_new is not None and df_clientes_new is not None:
                            with st.spinner("Criando campanha..."):
                                campanha_id, msg = save_campanha(nome_nova_campanha, df_envios_new, df_clientes_new)
                                if campanha_id:
                                    st.success(msg)
                                    st.cache_data.clear() # Limpa o cache para recarregar a meta
                                    st.rerun()
                                else:
                                    st.error(msg)
                        else:
                            st.error("Por favor, corrija os erros nos arquivos de upload.")
                    else:
                        st.warning("Preencha todos os campos para criar uma nova campanha.")

        elif gerenciar_tab == "Atualizar Existente":
            with st.sidebar.expander("🔄 Atualizar Campanha Existente"):
                df_meta_atualizar = load_campanhas_meta()
                campanhas_disponiveis_atualizar = df_meta_atualizar['nome'].tolist()
                campanha_selecionada_nome_atualizar = st.selectbox(
                    "Selecione a Campanha para Atualizar",
                    options=[""] + campanhas_disponiveis_atualizar,
                    key="select_campanha_atualizar"
                )

                if campanha_selecionada_nome_atualizar:
                    campanha_id_atualizar = df_meta_atualizar[df_meta_atualizar['nome'] == campanha_selecionada_nome_atualizar]['id'].iloc[0]
                    st.info(f"Adicionando dados à campanha: **{campanha_selecionada_nome_atualizar}** (ID: {campanha_id_atualizar})")

                    uploaded_envios_update = st.file_uploader("Upload Novos Envios (.xlsx)", type=["xlsx"], key="upload_envios_update")
                    uploaded_clientes_update = st.file_uploader("Upload Novos Clientes (.xlsx)", type=["xlsx"], key="upload_clientes_update")

                    if st.button("Adicionar Dados à Campanha", key="update_campanha_btn"):
                        if uploaded_envios_update and uploaded_clientes_update:
                            df_envios_update = load_and_process_envios(uploaded_envios_update)
                            df_clientes_update = load_and_process_clientes(uploaded_clientes_update)

                            if df_envios_update is not None and df_clientes_update is not None:
                                with st.spinner("Atualizando campanha..."):
                                    success, msg = update_campanha_data(campanha_id_atualizar, campanha_selecionada_nome_atualizar, df_envios_update, df_clientes_update)
                                    if success:
                                        st.success(msg)
                                        st.cache_data.clear() # Limpa o cache para recarregar a meta e os dados
                                        st.rerun()
                                    else:
                                        st.error(msg)
                            else:
                                st.error("Por favor, corrija os erros nos arquivos de upload.")
                        else:
                            st.warning("Faça o upload dos novos arquivos de envios e clientes.")
                else:
                    st.info("Selecione uma campanha para adicionar dados.")

        elif gerenciar_tab == "Deletar":
            with st.sidebar.expander("🗑️ Deletar Campanha"):
                df_meta_deletar = load_campanhas_meta()
                campanhas_disponiveis_deletar = df_meta_deletar['nome'].tolist()
                campanha_selecionada_nome_deletar = st.selectbox(
                    "Selecione a Campanha para Deletar",
                    options=[""] + campanhas_disponiveis_deletar,
                    key="select_campanha_deletar"
                )
                if campanha_selecionada_nome_deletar:
                    campanha_id_deletar = df_meta_deletar[df_meta_deletar['nome'] == campanha_selecionada_nome_deletar]['id'].iloc[0]
                    st.warning(f"Você está prestes a deletar a campanha: **{campanha_selecionada_nome_deletar}** (ID: {campanha_id_deletar}). Esta ação é irreversível.")
                    if st.button("Confirmar Deleção", key="deletar_campanha_btn"):
                        with st.spinner("Deletando campanha..."):
                            success, msg = delete_campanha(campanha_id_deletar, campanha_selecionada_nome_deletar)
                            if success:
                                st.success(msg)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                else:
                    st.info("Selecione uma campanha para deletar.")

    # --- Seleção de Campanhas para Análise ---
    st.sidebar.header("⚙️ Configurações da Análise")
    df_meta = load_campanhas_meta()
    campanhas_disponiveis = df_meta['nome'].tolist()

    # Adicionar opção para selecionar por mês
    df_meta['mes_ano'] = df_meta['criado_em'].dt.to_period('M').astype(str)
    meses_disponiveis = sorted(df_meta['mes_ano'].unique().tolist(), reverse=True)
    meses_disponiveis.insert(0, "Todas as Campanhas")

    modo_selecao = st.sidebar.radio(
        "Modo de Seleção de Campanhas",
        ["Por Mês", "Individual (Múltipla Seleção)"],
        key="modo_selecao_campanhas"
    )

    campanhas_selecionadas_ids = []

    if modo_selecao == "Por Mês":
        mes_selecionado = st.sidebar.selectbox(
            "Selecione o Mês/Ano",
            options=meses_disponiveis,
            key="mes_selecionado"
        )
        if mes_selecionado == "Todas as Campanhas":
            campanhas_selecionadas_ids = df_meta['id'].tolist()
        elif mes_selecionado:
            campanhas_selecionadas_ids = df_meta[df_meta['mes_ano'] == mes_selecionado]['id'].tolist()
    else: # Individual (Múltipla Seleção)
        campanhas_selecionadas_nomes = st.sidebar.multiselect(
            "Selecione as Campanhas",
            options=campanhas_disponiveis,
            default=campanhas_disponiveis if len(campanhas_disponiveis) <= 3 else [], # Seleciona todas se <=3, senão nenhuma
            key="campanhas_multiselect"
        )
        campanhas_selecionadas_ids = df_meta[df_meta['nome'].isin(campanhas_selecionadas_nomes)]['id'].tolist()

    janela_dias = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7, key="janela_dias_slider")
    executar_analise = st.sidebar.button("▶️ Executar Análise", use_container_width=True, key="executar_analise_btn")

    # --- Carregamento e Agregação de Dados ---
    df_envios_agregado = pd.DataFrame()
    df_clientes_agregado = pd.DataFrame()
    df_pagamentos = load_pagamentos_geral()

    if campanhas_selecionadas_ids:
        lista_df_envios = []
        lista_df_clientes = []
        with st.sidebar.spinner("Carregando dados das campanhas selecionadas..."):
            for c_id in campanhas_selecionadas_ids:
                df_e = load_campanha_envios(c_id)
                df_c = load_campanha_clientes(c_id)
                if not df_e.empty:
                    lista_df_envios.append(df_e)
                if not df_c.empty:
                    lista_df_clientes.append(df_c)

        if lista_df_envios:
            df_envios_agregado = pd.concat(lista_df_envios, ignore_index=True)
            df_envios_agregado.drop_duplicates(subset=['TELEFONE_ENVIO', 'DATA_ENVIO'], inplace=True)
            st.sidebar.success(f"✅ Envios agregados ({len(df_envios_agregado):,} registros)")
        else:
            st.sidebar.error("Erro ao carregar envios das campanhas selecionadas.")

        if lista_df_clientes:
            df_clientes_agregado = pd.concat(lista_df_clientes, ignore_index=True)
            df_clientes_agregado.drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], inplace=True)
            st.sidebar.success(f"✅ Clientes agregados ({len(df_clientes_agregado):,} registros)")
        else:
            st.sidebar.error("Erro ao carregar clientes das campanhas selecionadas.")
    else:
        st.sidebar.info("Nenhuma campanha selecionada para análise.")

    dados_prontos = (
        not df_envios_agregado.empty and
        not df_clientes_agregado.empty and
        not df_pagamentos.empty
    )

    # --- Lógica Principal da Análise ---
    if executar_analise and dados_prontos:
        st.subheader("Resultados da Análise da Campanha")

        # Total de clientes notificados (únicos no período agregado)
        total_clientes_notificados = df_envios_agregado['TELEFONE_ENVIO'].nunique()

        # Total da dívida dos notificados (apenas para clientes que foram notificados)
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

        # Garantir notificações únicas por matrícula e data de envio para evitar contagem duplicada
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
            custo_campanha           = total_clientes_notificados * 0.05 # Exemplo de custo
            roi                      = ((valor_total_arrecadado - custo_campanha) / custo_campanha *100) if custo_campanha > 0 else 0

            # ── ABAS ──────────────────────────────────────────
            aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
                "📊 Visão Geral",
                "🏙️ Cidade e Diretoria",
                "📅 Análise das Faturas",
                "📈 Utilização",
                "💳 Canal de Pagamento",
                "📋 Detalhes"
            ])

            # ══════════════════════════════════════════════════
            # ABA 1 — VISÃO GERAL
            # ══════════════════════════════════════════════════
            with aba1:
                st.subheader("Resultados da Análise da Campanha")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total de clientes notificados", f"{total_clientes_notificados}")
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

                    # Clientes notificados múltiplas vezes
                    st.subheader("Clientes Notificados Múltiplas Vezes")
                    notificacoes_por_cliente = df_campanha_unique_notifications.groupby('MATRICULA').size().reset_index(name='Num_Notificacoes')
                    clientes_multiplas_notificacoes = notificacoes_por_cliente[notificacoes_por_cliente['Num_Notificacoes'] > 1]

                    if not clientes_multiplas_notificacoes.empty:
                        st.dataframe(clientes_multiplas_notificacoes.sort_values('Num_Notificacoes', ascending=False), use_container_width=True, hide_index=True)
                    else:
                        st.info("Nenhum cliente foi notificado múltiplas vezes nas campanhas selecionadas.")

                else:
                    st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

            # ══════════════════════════════════════════════════
            # ABA 2 — CIDADE E DIRETORIA
            # ══════════════════════════════════════════════════
            with aba2:
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
                            st.subheader("Tipo de Pagamento por Cidade")
                            cidade_canal = df_pagamentos_campanha.groupby(['CIDADE', 'TIPO_PAGAMENTO'])['VALOR_PAGO'].sum().reset_index()
                            fig_cidade_canal = px.bar(
                                cidade_canal,
                                x='CIDADE', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                                title='Valor Pago por Cidade e Canal de Pagamento',
                                labels={'CIDADE': 'Cidade', 'VALOR_PAGO': 'Valor Pago (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                                hover_data={'VALOR_PAGO': ':.2f'}
                            )
                            fig_cidade_canal = add_bar_labels(fig_cidade_canal, 'valor')
                            fig_cidade_canal.update_layout(xaxis_title="Cidade", yaxis_title="Valor Pago (R$)", barmode='stack')
                            st.plotly_chart(fig_cidade_canal, use_container_width=True, key="fig_cidade_canal")

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
                                hover_data={'VALOR_PAGO': ':.2f'}
                            )
                            fig_diretoria_canal = add_bar_labels(fig_diretoria_canal, 'valor')
                            fig_diretoria_canal.update_layout(xaxis_title="Diretoria", yaxis_title="Valor Pago (R$)", barmode='stack')
                            st.plotly_chart(fig_diretoria_canal, use_container_width=True, key="fig_diretoria_canal")

                    if not tem_cidade and not tem_diretoria:
                        st.info("As colunas 'CIDADE' e 'DIRETORIA' não foram encontradas nos dados de clientes.")
                else:
                    st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

            # ══════════════════════════════════════════════════
            # ABA 3 — ANÁLISE DAS FATURAS
            # ══════════════════════════════════════════════════
            with aba3:
                if not df_pagamentos_campanha.empty:
                    tem_vencimento = 'VENCIMENTO' in df_pagamentos_campanha.columns
                    tem_tipo_fatura = 'TIPO_FATURA' in df_pagamentos_campanha.columns

                    if tem_vencimento:
                        st.subheader("Análise por Mês/Ano de Vencimento da Fatura")

                        fatura_resumo = df_pagamentos_campanha.groupby('MES_ANO_FATURA').agg(
                            Clientes_que_Pagaram=('MATRICULA', 'nunique'),
                            Valor_Arrecadado=('VALOR_PAGO', 'sum')
                        ).reset_index()
                        fatura_resumo['MES_ANO_FATURA'] = pd.to_datetime(fatura_resumo['MES_ANO_FATURA'], format='%m/%Y')
                        fatura_resumo = fatura_resumo.sort_values('MES_ANO_FATURA')

                        fig_fatura_valor = px.bar(
                            fatura_resumo,
                            x='MES_ANO_FATURA', y='Valor_Arrecadado',
                            title='Valor Arrecadado por Mês/Ano de Vencimento',
                            labels={'MES_ANO_FATURA': 'Mês/Ano de Vencimento', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'},
                            hover_data={'Valor_Arrecadado': ':.2f'}
                        )
                        fig_fatura_valor = add_bar_labels(fig_fatura_valor, 'valor')
                        fig_fatura_valor.update_layout(xaxis_title="Mês/Ano de Vencimento", yaxis_title="Valor Arrecadado (R$)")
                        st.plotly_chart(fig_fatura_valor, use_container_width=True, key="fig_fatura_valor")

                        fig_fatura_clientes = px.bar(
                            fatura_resumo,
                            x='MES_ANO_FATURA', y='Clientes_que_Pagaram',
                            title='Clientes que Pagaram por Mês/Ano de Vencimento',
                            labels={'MES_ANO_FATURA': 'Mês/Ano de Vencimento', 'Clientes_que_Pagaram': 'Clientes que Pagaram'}
                        )
                        fig_fatura_clientes = add_bar_labels(fig_fatura_clientes, 'qtd')
                        fig_fatura_clientes.update_layout(xaxis_title="Mês/Ano de Vencimento", yaxis_title="Clientes que Pagaram")
                        st.plotly_chart(fig_fatura_clientes, use_container_width=True, key="fig_fatura_clientes")

                        # Tabela fatura
                        tab_fatura = fatura_resumo.copy()
                        tab_fatura['MES_ANO_FATURA'] = tab_fatura['MES_ANO_FATURA'].dt.strftime('%m/%Y')
                        tab_fatura.columns = ['Mês/Ano de Vencimento', 'Clientes que Pagaram', 'Valor Arrecadado']
                        tab_fatura['Valor Arrecadado'] = tab_fatura['Valor Arrecadado'].apply(fmt_brl)
                        st.dataframe(tab_fatura, use_container_width=True, hide_index=True)

                    if tem_tipo_fatura:
                        st.subheader("Análise por Tipo de Fatura")

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

                    if not tem_vencimento and not tem_tipo_fatura:
                        st.info("As colunas 'VENCIMENTO' e 'TIPO_FATURA' não foram encontradas nos dados de pagamentos.")
                else:
                    st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

            # ══════════════════════════════════════════════════
            # ABA 4 — UTILIZAÇÃO
            # ══════════════════════════════════════════════════
            with aba4:
                if not df_pagamentos_campanha.empty:
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
                        st.info("A coluna 'UTILIZACAO' não foi encontrada nos dados de pagamentos.")
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
