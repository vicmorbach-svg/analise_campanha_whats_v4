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
# SISTEMA DE LOGIN
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

st.set_page_config(layout="wide", page_title="Análise de campanha de cobrança")

# --- Funções para obter configurações do GitHub ---
# Mantido como solicitado pelo usuário
def get_github_config():
    try:
        token  = st.secrets["github"]["token"]
        repo   = st.secrets["github"]["repo"] # Ex: "owner/repo_name"
        branch = st.secrets["github"].get("branch", "main")
        return token, repo, branch
    except KeyError as e:
        st.error(f"Erro de configuração: A chave '{e}' não foi encontrada em `st.secrets['github']`. "
                 "Por favor, configure seus segredos do GitHub corretamente no Streamlit Cloud.")
        st.stop()
    except Exception as e:
        st.error(f"Erro ao carregar configurações do GitHub: {e}. "
                 "Verifique se seus segredos do GitHub estão configurados corretamente.")
        st.stop()

# Carregar configurações do GitHub no início
GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH = get_github_config()

# Extrair owner e repo_name do GITHUB_REPO
try:
    REPO_OWNER, REPO_NAME = GITHUB_REPO.split('/')
except ValueError:
    st.error("Formato inválido para 'github.repo' em st.secrets. Deve ser 'owner/repo_name'.")
    st.stop()

GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE INTERAÇÃO COM GITHUB
# ══════════════════════════════════════════════════════════════

def get_file_sha(path):
    """Retorna apenas o SHA de um arquivo para operações de update."""
    url = f"{GITHUB_API_URL}/{path}?ref={GITHUB_BRANCH}"
    r   = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict): # Arquivo pequeno
            return data.get("sha")
    return None

def get_file_from_github(path):
    """
    Lê arquivo do GitHub via Raw URL (sem limite de tamanho).
    Retorna (bytes, sha) ou (None, None).
    """
    raw_url = f"https://raw.githubusercontent.com/{REPO_OWNER}/{REPO_NAME}/{GITHUB_BRANCH}/{path}"
    r = requests.get(raw_url, headers=HEADERS)
    if r.status_code == 200 and len(r.content) > 0:
        sha = get_file_sha(path) # Obtém o SHA separadamente para garantir que é o mais recente
        return r.content, sha
    elif r.status_code == 404:
        return None, None # Arquivo não encontrado
    else:
        st.error(f"Erro {r.status_code} ao baixar {path} do GitHub: {r.text}")
        return None, None

def save_file_to_github(path, content_bytes, message):
    """
    Salva arquivo no GitHub.
    Arquivos <= 50MB usam a Contents API com base64.
    """
    sha     = get_file_sha(path)
    url     = f"{GITHUB_API_URL}/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch":  GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=HEADERS, data=json.dumps(payload))
    if r.status_code in [200, 201]:
        return True
    else:
        st.error(f"Erro {r.status_code} ao salvar {path} no GitHub: {r.text}")
        return False

def delete_file_from_github(path, message):
    sha = get_file_sha(path)
    if not sha:
        return True # Arquivo já não existe
    url     = f"{GITHUB_API_URL}/{path}"
    payload = {"message": message, "sha": sha, "branch": GITHUB_BRANCH}
    r = requests.delete(url, headers=HEADERS, data=json.dumps(payload))
    if r.status_code == 200:
        return True
    else:
        st.error(f"Erro {r.status_code} ao deletar {path} do GitHub: {r.text}")
        return False

def df_to_parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)
    return buf.getvalue()

def parquet_bytes_to_df(content_bytes):
    if content_bytes is None or len(content_bytes) == 0:
        return pd.DataFrame() # Retorna DataFrame vazio se o conteúdo for nulo ou vazio
    try:
        buf = io.BytesIO(content_bytes)
        return pd.read_parquet(buf, engine='pyarrow')
    except Exception as e:
        st.error(f"Erro ao ler arquivo Parquet: {e}")
        return pd.DataFrame()

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE PROCESSAMENTO DE DADOS (Adaptadas para uso interno)
# ══════════════════════════════════════════════════════════════

def process_envios_df(uploaded_file):
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
        return df_envios
    except Exception as e:
        st.error(f"Erro ao processar arquivo de Envios: {e}")
        return None

def process_pagamentos_df(uploaded_file):
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
            st.error("Arquivo de Pagamentos está vazio ou não pôde ser lido.")
            return None

        if df.shape[1] < 10:
            st.error(f"Arquivo de Pagamentos: Esperava pelo menos 10 colunas, mas encontrou {df.shape[1]}.")
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
            df_pagamentos['TIPO_PAGAMENTO'] = df_pagamentos['TIPO_PAGAMENTO'].astype(str).str.strip().replace('nan', 'Não informado')
        if 'VENCIMENTO' in df_pagamentos.columns:
            df_pagamentos['VENCIMENTO'] = pd.to_datetime(df_pagamentos['VENCIMENTO'], errors='coerce', dayfirst=True)
            df_pagamentos['MES_FATURA']     = df_pagamentos['VENCIMENTO'].dt.month
            df_pagamentos['ANO_FATURA']     = df_pagamentos['VENCIMENTO'].dt.year
            df_pagamentos['MES_ANO_FATURA'] = df_pagamentos['VENCIMENTO'].dt.strftime('%m/%Y')
        if 'TIPO_FATURA' in df_pagamentos.columns:
            df_pagamentos['TIPO_FATURA'] = df_pagamentos['TIPO_FATURA'].astype(str).str.strip().replace('nan', 'Não informado')
        if 'UTILIZACAO' in df_pagamentos.columns:
            df_pagamentos['UTILIZACAO'] = df_pagamentos['UTILIZACAO'].astype(str).str.strip().replace('nan', 'Não informado')

        return df_pagamentos
    except Exception as e:
        st.error(f"Erro ao processar arquivo de Pagamentos: {e}")
        return None

def process_clientes_df(uploaded_file):
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
        return df_clientes
    except Exception as e:
        st.error(f"Erro ao processar arquivo de Clientes: {e}")
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
# GERENCIAMENTO DE CAMPANHAS (METADADOS)
# ══════════════════════════════════════════════════════════════

CAMPANHAS_META_PATH = "campanhas_meta.parquet"
PAGAMENTOS_GLOBAL_PATH = "pagamentos_global.parquet" # Novo arquivo para pagamentos globais

@st.cache_data(ttl=300) # Cache por 5 minutos
def load_campanhas_meta():
    content_bytes, _ = get_file_from_github(CAMPANHAS_META_PATH)
    if content_bytes:
        df_meta = parquet_bytes_to_df(content_bytes)
        if not df_meta.empty:
            df_meta['data_criacao'] = pd.to_datetime(df_meta['data_criacao'], errors='coerce')
            return df_meta
    return pd.DataFrame(columns=['id', 'nome', 'data_criacao', 'total_envios', 'total_clientes'])

def save_campanhas_meta(df_meta):
    content_bytes = df_to_parquet_bytes(df_meta)
    return save_file_to_github(CAMPANHAS_META_PATH, content_bytes, "Atualiza metadados das campanhas")

@st.cache_data(ttl=300) # Cache por 5 minutos
def load_campanha_envios(campanha_id):
    path = f"campanhas/{campanha_id}/envios.parquet"
    content_bytes, _ = get_file_from_github(path)
    return parquet_bytes_to_df(content_bytes)

@st.cache_data(ttl=300) # Cache por 5 minutos
def load_campanha_clientes(campanha_id):
    path = f"campanhas/{campanha_id}/clientes.parquet"
    content_bytes, _ = get_file_from_github(path)
    return parquet_bytes_to_df(content_bytes)

@st.cache_data(ttl=300) # Cache por 5 minutos
def load_pagamentos_global():
    content_bytes, _ = get_file_from_github(PAGAMENTOS_GLOBAL_PATH)
    df_pagamentos = process_pagamentos_df(io.BytesIO(content_bytes)) if content_bytes else pd.DataFrame()
    return df_pagamentos

# ══════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════

st.title("📊 Análise de eficiência de campanha de cobrança via Whatsapp")

# --- Carregar metadados das campanhas ---
df_campanhas_meta = load_campanhas_meta()
campanhas_disponiveis = df_campanhas_meta.to_dict('records')
campanha_nomes_ids = {c['nome']: c['id'] for c in campanhas_disponiveis}
campanha_ids_nomes = {c['id']: c['nome'] for c in campanhas_disponiveis}

# --- Sidebar ---
st.sidebar.header("Configurações da Análise")

# Seleção de campanhas para análise
campanhas_selecionadas_nomes = st.sidebar.multiselect(
    "Selecione uma ou mais campanhas para análise:",
    options=list(campanha_nomes_ids.keys()),
    key="multiselect_campanhas_analise"
)
campanhas_selecionadas_ids = [campanha_nomes_ids[nome] for nome in campanhas_selecionadas_nomes]

janela_dias = st.sidebar.slider("Janela de dias para considerar o pagamento após o envio da notificação:", 0, 30, 7, key="janela_dias_slider")
executar_analise = st.sidebar.button("Executar Análise", key="executar_analise_button")

# --- Seção de Administração (apenas para admins) ---
if is_admin():
    st.sidebar.header("🛠️ Gerenciar Campanhas")
    admin_tab_titles = ["Criar Nova", "Atualizar Existente", "Upload Pagamentos", "Deletar"]
    admin_tab_selected = st.sidebar.radio("Ações de Administração:", admin_tab_titles, key="admin_tabs")

    if admin_tab_selected == "Criar Nova":
        with st.sidebar.expander("Criar Nova Campanha", expanded=True):
            nova_campanha_nome = st.text_input("Nome da Nova Campanha:", key="nova_campanha_nome")
            uploaded_envios_new = st.file_uploader("Upload Base de Envios (Notificações - .xlsx)", type=["xlsx"], key="upload_envios_new")
            uploaded_clientes_new = st.file_uploader("Upload Base de Clientes (.xlsx)", type=["xlsx"], key="upload_clientes_new")

            if st.button("Criar Campanha", key="btn_criar_campanha"):
                if nova_campanha_nome and uploaded_envios_new and uploaded_clientes_new:
                    if nova_campanha_nome in campanha_nomes_ids:
                        st.error(f"Já existe uma campanha com o nome '{nova_campanha_nome}'.")
                    else:
                        with st.spinner(f"Criando campanha '{nova_campanha_nome}'..."):
                            df_envios_new = process_envios_df(uploaded_envios_new)
                            df_clientes_new = process_clientes_df(uploaded_clientes_new)

                            if df_envios_new is not None and df_clientes_new is not None:
                                campanha_id = str(uuid.uuid4())
                                envios_path = f"campanhas/{campanha_id}/envios.parquet"
                                clientes_path = f"campanhas/{campanha_id}/clientes.parquet"

                                if save_file_to_github(envios_path, df_to_parquet_bytes(df_envios_new), f"Adiciona envios para {nova_campanha_nome}") and \
                                   save_file_to_github(clientes_path, df_to_parquet_bytes(df_clientes_new), f"Adiciona clientes para {nova_campanha_nome}"):

                                    new_meta_entry = pd.DataFrame([{
                                        'id': campanha_id,
                                        'nome': nova_campanha_nome,
                                        'data_criacao': pd.Timestamp.now(),
                                        'total_envios': len(df_envios_new),
                                        'total_clientes': len(df_clientes_new)
                                    }])
                                    df_campanhas_meta = pd.concat([df_campanhas_meta, new_meta_entry], ignore_index=True)
                                    if save_campanhas_meta(df_campanhas_meta):
                                        st.success(f"Campanha '{nova_campanha_nome}' criada com sucesso!")
                                        st.cache_data.clear() # Limpa cache para recarregar metadados
                                        st.rerun()
                                    else:
                                        st.error("Erro ao atualizar metadados da campanha.")
                                else:
                                    st.error("Erro ao salvar arquivos da campanha no GitHub.")
                            else:
                                st.error("Erro no processamento dos arquivos de envios ou clientes.")
                else:
                    st.warning("Por favor, preencha o nome da campanha e faça o upload de ambos os arquivos.")

    elif admin_tab_selected == "Atualizar Existente":
        with st.sidebar.expander("Atualizar Campanha Existente", expanded=True):
            if not campanhas_disponiveis:
                st.info("Nenhuma campanha existente para atualizar.")
            else:
                campanha_para_atualizar_nome = st.selectbox(
                    "Selecione a campanha para atualizar:",
                    options=list(campanha_nomes_ids.keys()),
                    key="select_campanha_atualizar"
                )
                if campanha_para_atualizar_nome:
                    campanha_para_atualizar_id = campanha_nomes_ids[campanha_para_atualizar_nome]
                    st.info(f"Adicione novos dados à campanha '{campanha_para_atualizar_nome}'. Os dados existentes serão carregados, os novos serão anexados e duplicatas serão removidas.")

                    uploaded_envios_update = st.file_uploader("Upload Novos Envios (Notificações - .xlsx)", type=["xlsx"], key="upload_envios_update")
                    uploaded_clientes_update = st.file_uploader("Upload Novos Clientes (.xlsx)", type=["xlsx"], key="upload_clientes_update")

                    if st.button("Atualizar Campanha", key="btn_atualizar_campanha"):
                        if uploaded_envios_update and uploaded_clientes_update:
                            with st.spinner(f"Atualizando campanha '{campanha_para_atualizar_nome}'..."):
                                # Processar novos arquivos
                                df_envios_novos = process_envios_df(uploaded_envios_update)
                                df_clientes_novos = process_clientes_df(uploaded_clientes_update)

                                if df_envios_novos is not None and df_clientes_novos is not None:
                                    # Carregar dados existentes
                                    df_envios_existente = load_campanha_envios(campanha_para_atualizar_id)
                                    df_clientes_existente = load_campanha_clientes(campanha_para_atualizar_id)

                                    # Combinar e deduplicar
                                    df_envios_combinado = pd.concat([df_envios_existente, df_envios_novos]).drop_duplicates(subset=['TELEFONE_ENVIO', 'DATA_ENVIO'])
                                    df_clientes_combinado = pd.concat([df_clientes_existente, df_clientes_novos]).drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'])

                                    # Salvar de volta ao GitHub
                                    envios_path = f"campanhas/{campanha_para_atualizar_id}/envios.parquet"
                                    clientes_path = f"campanhas/{campanha_para_atualizar_id}/clientes.parquet"

                                    if save_file_to_github(envios_path, df_to_parquet_bytes(df_envios_combinado), f"Atualiza envios para {campanha_para_atualizar_nome}") and \
                                       save_file_to_github(clientes_path, df_to_parquet_bytes(df_clientes_combinado), f"Atualiza clientes para {campanha_para_atualizar_nome}"):

                                        # Atualizar metadados
                                        idx = df_campanhas_meta[df_campanhas_meta['id'] == campanha_para_atualizar_id].index
                                        if not idx.empty:
                                            df_campanhas_meta.loc[idx, 'total_envios'] = len(df_envios_combinado)
                                            df_campanhas_meta.loc[idx, 'total_clientes'] = len(df_clientes_combinado)
                                            if save_campanhas_meta(df_campanhas_meta):
                                                st.success(f"Campanha '{campanha_para_atualizar_nome}' atualizada com sucesso!")
                                                st.cache_data.clear() # Limpa cache para recarregar metadados
                                                st.rerun()
                                            else:
                                                st.error("Erro ao atualizar metadados da campanha.")
                                        else:
                                            st.error("Erro: Metadados da campanha não encontrados para atualização.")
                                    else:
                                        st.error("Erro ao salvar arquivos atualizados da campanha no GitHub.")
                                else:
                                    st.error("Erro no processamento dos novos arquivos de envios ou clientes.")
                        else:
                            st.warning("Por favor, faça o upload de ambos os arquivos para atualização.")

    elif admin_tab_selected == "Upload Pagamentos":
        with st.sidebar.expander("Upload Base de Pagamentos Global", expanded=True):
            st.info("Esta base de pagamentos será usada para todas as análises de campanha.")
            uploaded_pagamentos_global = st.file_uploader("Upload Base de Pagamentos (.csv, .xlsx ou .parquet)", type=["csv", "xlsx", "parquet"], key="upload_pagamentos_global")
            if st.button("Salvar Base de Pagamentos", key="btn_salvar_pagamentos_global"):
                if uploaded_pagamentos_global:
                    with st.spinner("Processando e salvando base de pagamentos..."):
                        df_pagamentos_global = process_pagamentos_df(uploaded_pagamentos_global)
                        if df_pagamentos_global is not None and not df_pagamentos_global.empty:
                            if save_file_to_github(PAGAMENTOS_GLOBAL_PATH, df_to_parquet_bytes(df_pagamentos_global), "Atualiza base de pagamentos global"):
                                st.success("Base de pagamentos global salva com sucesso!")
                                st.cache_data.clear() # Limpa cache para recarregar pagamentos
                                st.rerun()
                            else:
                                st.error("Erro ao salvar base de pagamentos no GitHub.")
                        else:
                            st.error("Erro no processamento da base de pagamentos.")
                else:
                    st.warning("Por favor, faça o upload do arquivo de pagamentos.")

    elif admin_tab_selected == "Deletar":
        with st.sidebar.expander("Deletar Campanha", expanded=True):
            if not campanhas_disponiveis:
                st.info("Nenhuma campanha existente para deletar.")
            else:
                campanha_para_deletar_nome = st.selectbox(
                    "Selecione a campanha para deletar:",
                    options=list(campanha_nomes_ids.keys()),
                    key="select_campanha_deletar"
                )
                if campanha_para_deletar_nome:
                    campanha_para_deletar_id = campanha_nomes_ids[campanha_para_deletar_nome]
                    st.warning(f"Atenção: Deletar a campanha '{campanha_para_deletar_nome}' removerá todos os seus dados do GitHub.")
                    if st.button("Confirmar Deleção", key="btn_deletar_campanha"):
                        with st.spinner(f"Deletando campanha '{campanha_para_deletar_nome}'..."):
                            envios_path = f"campanhas/{campanha_para_deletar_id}/envios.parquet"
                            clientes_path = f"campanhas/{campanha_para_deletar_id}/clientes.parquet"

                            if delete_file_from_github(envios_path, f"Deleta envios da campanha {campanha_para_deletar_nome}") and \
                               delete_file_from_github(clientes_path, f"Deleta clientes da campanha {campanha_para_deletar_nome}"):

                                df_campanhas_meta = df_campanhas_meta[df_campanhas_meta['id'] != campanha_para_deletar_id].reset_index(drop=True)
                                if save_campanhas_meta(df_campanhas_meta):
                                    st.success(f"Campanha '{campanha_para_deletar_nome}' deletada com sucesso!")
                                    st.cache_data.clear() # Limpa cache para recarregar metadados
                                    st.rerun()
                                else:
                                    st.error("Erro ao atualizar metadados após deleção.")
                            else:
                                st.error("Erro ao deletar arquivos da campanha no GitHub.")

# --- Carregar dados para análise ---
df_envios_agregado = pd.DataFrame()
df_clientes_agregado = pd.DataFrame()
df_pagamentos = load_pagamentos_global() # Carrega a base de pagamentos global

if campanhas_selecionadas_ids:
    with st.spinner("Carregando dados das campanhas selecionadas..."):
        all_envios = []
        all_clientes = []
        for c_id in campanhas_selecionadas_ids:
            df_e = load_campanha_envios(c_id)
            df_c = load_campanha_clientes(c_id)
            if not df_e.empty:
                all_envios.append(df_e)
            if not df_c.empty:
                all_clientes.append(df_c)

        if all_envios:
            df_envios_agregado = pd.concat(all_envios).drop_duplicates(subset=['TELEFONE_ENVIO', 'DATA_ENVIO']).reset_index(drop=True)
        if all_clientes:
            df_clientes_agregado = pd.concat(all_clientes).drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE']).reset_index(drop=True)

dados_prontos = (
    not df_envios_agregado.empty and
    not df_clientes_agregado.empty and
    not df_pagamentos.empty
)

# ══════════════════════════════════════════════════════════════
# LÓGICA PRINCIPAL DE ANÁLISE
# ══════════════════════════════════════════════════════════════

if executar_analise and dados_prontos:
    st.subheader(f"Resultados da Análise para Campanhas: {', '.join(campanhas_selecionadas_nomes)}")

    # Total de clientes notificados
    total_clientes_notificados = df_envios_agregado['TELEFONE_ENVIO'].nunique()

    # Total da dívida dos notificados
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
        total_pagamentos_atribuidos = df_pagamentos_campanha['VALOR_PAGO'].sum()
        clientes_que_pagaram = df_pagamentos_campanha['MATRICULA'].nunique()
        taxa_conversao = (clientes_que_pagaram / total_clientes_notificados) * 100 if total_clientes_notificados > 0 else 0

        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Clientes Notificados", f"{total_clientes_notificados:,}")
        col2.metric("Clientes que Pagaram", f"{clientes_que_pagaram:,}")
        col3.metric("Taxa de Conversão", f"{taxa_conversao:.2f}%")
        col4.metric("Valor Total Pago", fmt_brl(total_pagamentos_atribuidos))
        st.markdown("---")

        # Abas
        aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
            "Visão Geral", "Pagamentos por Dia", "Antiguidade da Dívida",
            "Clientes Múltiplas Notificações", "Canal de Pagamento", "Detalhes"
        ])

        # ══════════════════════════════════════════════════════════
        # ABA 1 — VISÃO GERAL
        # ══════════════════════════════════════════════════════════
        with aba1:
            st.subheader("Visão Geral da Campanha")

            if not df_pagamentos_campanha.empty:
                st.write(f"Total de pagamentos atribuídos: {df_pagamentos_campanha.shape[0]:,} (dentro da janela de {janela_dias} dias)")
                st.write(f"Valor médio por pagamento: {fmt_brl(df_pagamentos_campanha['VALOR_PAGO'].mean())}")
                st.write(f"Dias médios para pagamento: {df_pagamentos_campanha['DIAS_APOS_ENVIO'].mean():.1f} dias")

                st.subheader("Distribuição de Pagamentos por Dias Após Envio")
                pagamentos_por_dias = df_pagamentos_campanha.groupby('DIAS_APOS_ENVIO')['VALOR_PAGO'].sum().reset_index()
                fig_dias = px.bar(
                    pagamentos_por_dias,
                    x='DIAS_APOS_ENVIO', y='VALOR_PAGO',
                    title='Valor Pago por Dias Após o Envio da Notificação',
                    labels={'DIAS_APOS_ENVIO': 'Dias Após Envio', 'VALOR_PAGO': 'Valor Pago (R$)'},
                    hover_data={'VALOR_PAGO': ':.2f'}
                )
                fig_dias = add_bar_labels(fig_dias, 'valor')
                fig_dias.update_layout(xaxis_title="Dias Após Envio", yaxis_title="Valor Pago (R$)")
                st.plotly_chart(fig_dias, use_container_width=True, key="fig_dias_aba1")

                st.subheader("Distribuição de Clientes que Pagaram por Cidade")
                if 'CIDADE' in df_campanha_unique_notifications.columns:
                    clientes_por_cidade = df_pagamentos_campanha.groupby('CIDADE')['MATRICULA'].nunique().reset_index()
                    clientes_por_cidade.rename(columns={'MATRICULA': 'Clientes que Pagaram'}, inplace=True)
                    clientes_por_cidade = clientes_por_cidade.sort_values('Clientes que Pagaram', ascending=False)

                    fig_cidade = px.bar(
                        clientes_por_cidade,
                        x='CIDADE', y='Clientes que Pagaram',
                        title='Clientes que Pagaram por Cidade',
                        labels={'CIDADE': 'Cidade', 'Clientes que Pagaram': 'Clientes que Pagaram'},
                        color='CIDADE'
                    )
                    fig_cidade = add_bar_labels(fig_cidade, 'qtd')
                    fig_cidade.update_layout(xaxis_title="Cidade", yaxis_title="Clientes que Pagaram", showlegend=False)
                    st.plotly_chart(fig_cidade, use_container_width=True, key="fig_cidade_aba1")
                else:
                    st.info("Coluna 'CIDADE' não disponível na base de clientes.")

                st.subheader("Distribuição de Clientes que Pagaram por Diretoria")
                if 'DIRETORIA' in df_campanha_unique_notifications.columns:
                    clientes_por_diretoria = df_pagamentos_campanha.groupby('DIRETORIA')['MATRICULA'].nunique().reset_index()
                    clientes_por_diretoria.rename(columns={'MATRICULA': 'Clientes que Pagaram'}, inplace=True)
                    clientes_por_diretoria = clientes_por_diretoria.sort_values('Clientes que Pagaram', ascending=False)

                    fig_diretoria = px.bar(
                        clientes_por_diretoria,
                        x='DIRETORIA', y='Clientes que Pagaram',
                        title='Clientes que Pagaram por Diretoria',
                        labels={'DIRETORIA': 'Diretoria', 'Clientes que Pagaram': 'Clientes que Pagaram'},
                        color='DIRETORIA'
                    )
                    fig_diretoria = add_bar_labels(fig_diretoria, 'qtd')
                    fig_diretoria.update_layout(xaxis_title="Diretoria", yaxis_title="Clientes que Pagaram", showlegend=False)
                    st.plotly_chart(fig_diretoria, use_container_width=True, key="fig_diretoria_aba1")
                else:
                    st.info("Coluna 'DIRETORIA' não disponível na base de clientes.")
            else:
                st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

        # ══════════════════════════════════════════════════════════
        # ABA 2 — PAGAMENTOS POR DIA
        # ══════════════════════════════════════════════════════════
        with aba2:
            st.subheader("Pagamentos Atribuídos por Dia")
            if not df_pagamentos_campanha.empty:
                pagamentos_por_data = df_pagamentos_campanha.groupby(df_pagamentos_campanha['DATA_PAGAMENTO'].dt.date)['VALOR_PAGO'].sum().reset_index()
                pagamentos_por_data.rename(columns={'DATA_PAGAMENTO': 'Data do Pagamento'}, inplace=True)

                fig_pag_dia = px.line(
                    pagamentos_por_data,
                    x='Data do Pagamento', y='VALOR_PAGO',
                    title='Valor Total Pago por Dia',
                    labels={'Data do Pagamento': 'Data', 'VALOR_PAGO': 'Valor Pago (R$)'},
                    hover_data={'VALOR_PAGO': ':.2f'}
                )
                fig_pag_dia.update_layout(xaxis_title="Data do Pagamento", yaxis_title="Valor Pago (R$)")
                st.plotly_chart(fig_pag_dia, use_container_width=True, key="fig_pag_dia_aba2")

                st.subheader("Tabela de Pagamentos por Dia")
                tab_pag_dia = pagamentos_por_data.copy()
                tab_pag_dia['VALOR_PAGO'] = tab_pag_dia['VALOR_PAGO'].apply(fmt_brl)
                st.dataframe(tab_pag_dia, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

        # ══════════════════════════════════════════════════════════
        # ABA 3 — ANTIGUIDADE DA DÍVIDA E FATURA
        # ══════════════════════════════════════════════════════════
        with aba3:
            if not df_pagamentos_campanha.empty:
                if 'VENCIMENTO' in df_pagamentos_campanha.columns:
                    st.subheader("Valor Pago por Faixa de Antiguidade da Dívida (Vencimento vs. Pagamento)")
                    df_pagamentos_campanha['ANTIGUIDADE_DIAS'] = (
                        df_pagamentos_campanha['DATA_PAGAMENTO'] - df_pagamentos_campanha['VENCIMENTO']
                    ).dt.days

                    def classificar_antiguidade(dias):
                        if pd.isna(dias):
                            return 'Não informado'
                        if dias <= 10:
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

                    tab_utilizacao = utilizacao_resumo.copy()
                    tab_utilizacao.columns = ['Utilização', 'Quantidade', 'Valor Pago']
                    tab_utilizacao['Valor Pago'] = tab_utilizacao['Valor Pago'].apply(fmt_brl)
                    st.dataframe(tab_utilizacao, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

        # ══════════════════════════════════════════════════════════
        # ABA 4 — CLIENTES NOTIFICADOS MÚLTIPLAS VEZES
        # ══════════════════════════════════════════════════════════
        with aba4:
            st.subheader("Clientes Notificados Múltiplas Vezes")
            # Contar quantas vezes cada cliente foi notificado
            notificacoes_por_cliente = df_campanha.groupby('MATRICULA')['TELEFONE_ENVIO'].count().reset_index()
            notificacoes_por_cliente.rename(columns={'TELEFONE_ENVIO': 'Numero_Notificacoes'}, inplace=True)

            # Filtrar clientes notificados mais de uma vez
            clientes_multiplas_notificacoes = notificacoes_por_cliente[notificacoes_por_cliente['Numero_Notificacoes'] > 1]

            if not clientes_multiplas_notificacoes.empty:
                st.write(f"Total de clientes notificados múltiplas vezes: {len(clientes_multiplas_notificacoes)}")

                # Agrupar por número de notificações
                contagem_notificacoes = clientes_multiplas_notificacoes.groupby('Numero_Notificacoes').size().reset_index(name='Quantidade_Clientes')

                fig_multiplas = px.bar(
                    contagem_notificacoes,
                    x='Numero_Notificacoes',
                    y='Quantidade_Clientes',
                    title='Distribuição de Clientes Notificados Múltiplas Vezes',
                    labels={'Numero_Notificacoes': 'Número de Notificações', 'Quantidade_Clientes': 'Quantidade de Clientes'},
                    hover_data={'Quantidade_Clientes': True}
                )
                fig_multiplas.update_layout(xaxis_title="Número de Notificações", yaxis_title="Quantidade de Clientes")
                st.plotly_chart(fig_multiplas, use_container_width=True, key="fig_multiplas_notificacoes")

                st.subheader("Detalhes dos Clientes Notificados Múltiplas Vezes")
                # Mostrar detalhes dos clientes que foram notificados múltiplas vezes
                df_detalhes_multiplas = df_campanha[df_campanha['MATRICULA'].isin(clientes_multiplas_notificacoes['MATRICULA'])].copy()
                df_detalhes_multiplas = df_detalhes_multiplas.sort_values(by=['MATRICULA', 'DATA_ENVIO'])

                # Adicionar coluna de contagem para cada notificação
                df_detalhes_multiplas['Contagem_Notificacao'] = df_detalhes_multiplas.groupby('MATRICULA').cumcount() + 1

                colunas_multiplas = ['MATRICULA', 'TELEFONE_ENVIO', 'DATA_ENVIO', 'Contagem_Notificacao']
                if 'CIDADE' in df_detalhes_multiplas.columns:
                    colunas_multiplas.append('CIDADE')
                if 'DIRETORIA' in df_detalhes_multiplas.columns:
                    colunas_multiplas.append('DIRETORIA')

                st.dataframe(df_detalhes_multiplas[colunas_multiplas], use_container_width=True, hide_index=True)

                csv_output_multiplas = df_detalhes_multiplas[colunas_multiplas].to_csv(index=False, sep=';', decimal=',')
                st.download_button(
                    label="⬇️ Baixar Detalhes (CSV)",
                    data=csv_output_multiplas,
                    file_name="clientes_multiplas_notificacoes.csv",
                    mime="text/csv"
                )
            else:
                st.info("Nenhum cliente foi notificado múltiplas vezes nas campanhas selecionadas.")

        # ══════════════════════════════════════════════════════════
        # ABA 5 — CANAL DE PAGAMENTO
        # ══════════════════════════════════════════════════════════
        with aba5:
            if not df_pagamentos_campanha.empty and 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:

                st.subheader("Valor Arrecadado por Canal de Pagamento")
                pagamentos_por_canal = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['VALOR_PAGO'].sum().reset_index()
                pagamentos_por_canal = pagamentos_por_canal.sort_values('VALOR_PAGO', ascending=False)

                fig_canal = px.bar(
                    pagamentos_por_canal, x='TIPO_PAGAMENTO', y='VALOR_PAGO',
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
                    qtd_por_canal, x='TIPO_PAGAMENTO', y='Clientes que Pagaram',
                    title='Clientes que Pagaram por Canal',
                    labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'Clientes que Pagaram': 'Clientes que Pagaram'},
                    color='TIPO_PAGAMENTO'
                )
                fig_canal_qtd = add_bar_labels(fig_canal_qtd, 'qtd')
                fig_canal_qtd.update_layout(xaxis_title="Canal de Pagamento", yaxis_title="Clientes que Pagaram", showlegend=False)
                st.plotly_chart(fig_canal_qtd, use_container_width=True, key="fig_canal_qtd")

                tab_canal = pd.merge(pagamentos_por_canal, qtd_por_canal, on='TIPO_PAGAMENTO')
                tab_canal.columns = ['Canal de Pagamento', 'Valor Total Pago', 'Clientes que Pagaram']
                tab_canal['Valor Total Pago'] = tab_canal['Valor Total Pago'].apply(fmt_brl)
                st.dataframe(tab_canal, use_container_width=True, hide_index=True)
            else:
                st.info("Coluna 'Tipo Pagamento' não encontrada no arquivo de pagamentos.")

        # ══════════════════════════════════════════════════════════
        # ABA 6 — DETALHES
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

                st.dataframe(df_detalhes, use_container_width=True, hide_index=True)

                csv_output = df_detalhes.to_csv(index=False, sep=';', decimal=',')
                st.download_button(
                    label="⬇️ Baixar Detalhes dos Pagamentos (CSV)",
                    data=csv_output,
                    file_name="pagamentos_campanha.csv",
                    mime="text/csv"
                )
            else:
                st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

    else: # if not df_campanha_unique_notifications.empty:
        st.warning("Nenhum cliente notificado encontrado nas campanhas selecionadas após o cruzamento com a base de clientes.")

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
