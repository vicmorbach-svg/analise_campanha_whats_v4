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
# GITHUB — com suporte a arquivos grandes
# ══════════════════════════════════════════════════════════════

def get_github_config():
    try:
        token  = st.secrets["github"]["token"]
        repo   = st.secrets["github"]["repo"]
        branch = st.secrets["github"].get("branch", "main")
        return token, repo, branch
    except Exception:
        return None, None, None

def get_github_headers():
    token, _, _ = get_github_config()
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

def get_file_sha(path):
    """Retorna apenas o SHA de um arquivo para operações de update."""
    token, repo, branch = get_github_config()
    if not token:
        return None
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r   = requests.get(url, headers=get_github_headers())
    if r.status_code == 200:
        data = r.json()
        # arquivo pequeno
        if isinstance(data, dict):
            return data.get("sha")
    return None

def get_file_from_github(path):
    """
    Lê arquivo do GitHub via Raw URL (sem limite de tamanho).
    Retorna (bytes, sha) ou (None, None).
    """
    token, repo, branch = get_github_config()
    if not token:
        return None, None

    # Leitura via raw URL — sem limite de 1MB
    raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
    r = requests.get(raw_url, headers={"Authorization": f"token {token}"})
    if r.status_code == 200 and len(r.content) > 0:
        sha = get_file_sha(path)
        return r.content, sha
    return None, None

def save_file_to_github(path, content_bytes, message):
    """
    Salva arquivo no GitHub.
    Arquivos <= 50MB usam a Contents API com base64.
    """
    token, repo, branch = get_github_config()
    if not token:
        return False

    sha     = get_file_sha(path)
    url     = f"https://api.github.com/repos/{repo}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch":  branch
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=get_github_headers(), data=json.dumps(payload))
    return r.status_code in [200, 201]

def delete_file_from_github(path, message):
    token, repo, branch = get_github_config()
    if not token:
        return False
    sha = get_file_sha(path)
    if not sha:
        return True
    url     = f"https://api.github.com/repos/{repo}/contents/{path}"
    payload = {"message": message, "sha": sha, "branch": branch}
    r = requests.delete(url, headers=get_github_headers(), data=json.dumps(payload))
    return r.status_code == 200

def df_to_parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)
    return buf.getvalue()

def parquet_bytes_to_df(content_bytes):
    if content_bytes is None or len(content_bytes) == 0:
        st.error("Conteúdo do arquivo está vazio (0 bytes).")
        return None
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
            return None

# ══════════════════════════════════════════════════════════════
# CAMPANHAS
# ══════════════════════════════════════════════════════════════

META_PATH = "data/campanhas_meta.parquet"
PAG_PATH  = "data/pagamentos.parquet"

def load_campanhas_meta():
    content, _ = get_file_from_github(META_PATH)
    if content:
        df = parquet_bytes_to_df(content)
        if df is not None:
            return df
    return pd.DataFrame(columns=['id', 'nome', 'criado_em', 'total_envios', 'total_clientes'])

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

def load_campanha_envios(campanha_id):
    path = f"data/campanhas/{campanha_id}_envios.parquet"
    content, _ = get_file_from_github(path)
    if content is None:
        st.error(f"Arquivo não encontrado: {path}")
        return None
    return parquet_bytes_to_df(content)

def load_campanha_clientes(campanha_id):
    path = f"data/campanhas/{campanha_id}_clientes.parquet"
    content, _ = get_file_from_github(path)
    if content is None:
        st.error(f"Arquivo não encontrado: {path}")
        return None
    return parquet_bytes_to_df(content)

def delete_campanha(campanha_id, nome):
    df_meta = load_campanhas_meta()
    df_meta = df_meta[df_meta['id'] != campanha_id]
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} removida")
    delete_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet",   f"Campanha {nome}: envios removidos")
    delete_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet", f"Campanha {nome}: clientes removidos")

# ══════════════════════════════════════════════════════════════
# PAGAMENTOS
# ══════════════════════════════════════════════════════════════

def load_pagamentos_github():
    content, _ = get_file_from_github(PAG_PATH)
    if content is None or len(content) == 0:
        return None
    return parquet_bytes_to_df(content)

def update_pagamentos_github(df_novo):
    df_existente = load_pagamentos_github()
    if df_existente is not None and not df_existente.empty:
        df_combined = pd.concat([df_existente, df_novo], ignore_index=True)
        df_combined = df_combined.drop_duplicates(
            subset=['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO'],
            keep='last'
        )
    else:
        df_combined = df_novo.copy()
    total_antes = len(df_existente) if df_existente is not None else 0
    novos       = len(df_combined) - total_antes
    ok = save_file_to_github(PAG_PATH, df_to_parquet_bytes(df_combined), "Pagamentos: atualização")
    return ok, len(df_combined), novos

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE PROCESSAMENTO
# ══════════════════════════════════════════════════════════════

@st.cache_data
def load_and_process_envios(uploaded_file):
    try:
        df = pd.read_excel(uploaded_file)
        required_cols = ['To', 'Send At']
        if not all(col in df.columns for col in required_cols):
            st.error("Arquivo de Envios: colunas 'To' e 'Send At' não encontradas.")
            return None
        df_envios = df[['To', 'Send At']].copy()
        df_envios.rename(columns={'To': 'TELEFONE_ENVIO', 'Send At': 'DATA_ENVIO'}, inplace=True)
        df_envios['TELEFONE_ENVIO'] = (
            df_envios['TELEFONE_ENVIO']
            .astype(str)
            .str.replace(r'^55', '', regex=True)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
        )
        df_envios['DATA_ENVIO'] = pd.to_datetime(df_envios['DATA_ENVIO'], errors='coerce', dayfirst=True)
        df_envios.dropna(subset=['DATA_ENVIO'], inplace=True)
        return df_envios
    except Exception as e:
        st.error(f"Erro ao processar Envios: {e}")
        return None

@st.cache_data
def load_and_process_clientes(uploaded_file):
    try:
        df = pd.read_excel(uploaded_file)
        required_cols = ['TELEFONE', 'MATRICULA', 'SITUACAO']
        if not all(col in df.columns for col in required_cols):
            st.error(f"Arquivo de Clientes: colunas necessárias não encontradas: {required_cols}")
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
        df_clientes['TELEFONE_CLIENTE'] = (
            df_clientes['TELEFONE_CLIENTE']
            .astype(str)
            .str.replace(r'^55', '', regex=True)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
        )
        df_clientes['MATRICULA_CLIENTE'] = (
            df_clientes['MATRICULA_CLIENTE']
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
        )
        df_clientes['SITUACAO'] = pd.to_numeric(df_clientes['SITUACAO'], errors='coerce').fillna(0)
        if 'CIDADE' in df_clientes.columns:
            df_clientes['CIDADE']    = df_clientes['CIDADE'].astype(str).str.strip()
        if 'DIRETORIA' in df_clientes.columns:
            df_clientes['DIRETORIA'] = df_clientes['DIRETORIA'].astype(str).str.strip()
        df_clientes.drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], inplace=True)
        return df_clientes
    except Exception as e:
        st.error(f"Erro ao processar Clientes: {e}")
        return None

@st.cache_data
def load_and_process_pagamentos(uploaded_file):
    try:
        df = None
        if uploaded_file.name.endswith('.parquet'):
            df_pag = pd.read_parquet(uploaded_file, engine='pyarrow')
            if 'MATRICULA_PAGAMENTO' in df_pag.columns:
                df_pag['MATRICULA_PAGAMENTO'] = (
                    df_pag['MATRICULA_PAGAMENTO']
                    .astype(str)
                    .str.replace(r'\.0$', '', regex=True)
                    .str.strip()
                )
                df_pag['DATA_PAGAMENTO'] = pd.to_datetime(df_pag['DATA_PAGAMENTO'], errors='coerce', dayfirst=True)
                df_pag.dropna(subset=['DATA_PAGAMENTO'], inplace=True)
                df_pag['VALOR_PAGO'] = pd.to_numeric(df_pag['VALOR_PAGO'], errors='coerce')
                df_pag.dropna(subset=['VALOR_PAGO'], inplace=True)
                if 'TIPO_PAGAMENTO' in df_pag.columns:
                    df_pag['TIPO_PAGAMENTO'] = df_pag['TIPO_PAGAMENTO'].astype(str).str.strip().replace('nan', 'Não informado')
                if 'VENCIMENTO' in df_pag.columns:
                    df_pag['VENCIMENTO']     = pd.to_datetime(df_pag['VENCIMENTO'], errors='coerce', dayfirst=True)
                    df_pag['MES_FATURA']     = df_pag['VENCIMENTO'].dt.month
                    df_pag['ANO_FATURA']     = df_pag['VENCIMENTO'].dt.year
                    df_pag['MES_ANO_FATURA'] = df_pag['VENCIMENTO'].dt.strftime('%m/%Y')
                if 'TIPO_FATURA' in df_pag.columns:
                    df_pag['TIPO_FATURA'] = df_pag['TIPO_FATURA'].astype(str).str.strip().replace('nan', 'Não informado')
                if 'UTILIZACAO' in df_pag.columns:
                    df_pag['UTILIZACAO'] = df_pag['UTILIZACAO'].astype(str).str.strip().replace('nan', 'Não informado')
                return df_pag
            else:
                df         = df_pag
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
            if df is None:
                raise ValueError("Não foi possível ler o CSV.")
        elif uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file, header=None)
        else:
            raise ValueError("Formato não suportado.")

        if df is None or df.empty:
            st.error("Arquivo de Pagamentos está vazio.")
            return None
        if df.shape[1] < 10:
            st.error(f"Esperava pelo menos 10 colunas, encontrou {df.shape[1]}.")
            return None

        col_indices = [0, 5, 8]
        col_names   = ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']
        if df.shape[1] > 12:
            col_indices.append(12)
            col_names.append('TIPO_PAGAMENTO')

        df_pagamentos         = df.iloc[:, col_indices].copy()
        df_pagamentos.columns = col_names

        IDX_VENCIMENTO  = 4
        IDX_TIPO_FATURA = 11
        IDX_UTILIZACAO  = 9

        if df.shape[1] > IDX_VENCIMENTO:
            df_pagamentos['VENCIMENTO']  = df.iloc[:, IDX_VENCIMENTO].values
        if df.shape[1] > IDX_TIPO_FATURA:
            df_pagamentos['TIPO_FATURA'] = df.iloc[:, IDX_TIPO_FATURA].values
        if df.shape[1] > IDX_UTILIZACAO:
            df_pagamentos['UTILIZACAO']  = df.iloc[:, IDX_UTILIZACAO].values

        df_pagamentos['MATRICULA_PAGAMENTO'] = (
            df_pagamentos['MATRICULA_PAGAMENTO']
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
        )
        df_pagamentos['DATA_PAGAMENTO'] = pd.to_datetime(
            df_pagamentos['DATA_PAGAMENTO'], errors='coerce', dayfirst=True
        )
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
            df_pagamentos['TIPO_PAGAMENTO'] = (
                df_pagamentos['TIPO_PAGAMENTO'].astype(str).str.strip().replace('nan', 'Não informado')
            )
        if 'VENCIMENTO' in df_pagamentos.columns:
            df_pagamentos['VENCIMENTO']     = pd.to_datetime(df_pagamentos['VENCIMENTO'], errors='coerce', dayfirst=True)
            df_pagamentos['MES_FATURA']     = df_pagamentos['VENCIMENTO'].dt.month
            df_pagamentos['ANO_FATURA']     = df_pagamentos['VENCIMENTO'].dt.year
            df_pagamentos['MES_ANO_FATURA'] = df_pagamentos['VENCIMENTO'].dt.strftime('%m/%Y')
        if 'TIPO_FATURA' in df_pagamentos.columns:
            df_pagamentos['TIPO_FATURA'] = (
                df_pagamentos['TIPO_FATURA'].astype(str).str.strip().replace('nan', 'Não informado')
            )
        if 'UTILIZACAO' in df_pagamentos.columns:
            df_pagamentos['UTILIZACAO'] = (
                df_pagamentos['UTILIZACAO'].astype(str).str.strip().replace('nan', 'Não informado')
            )

        return df_pagamentos
    except Exception as e:
        st.error(f"Erro ao processar Pagamentos: {e}")
        return None

def fmt_brl(valor):
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "R$ 0,00"

def add_bar_labels(fig, formato='valor'):
    for trace in fig.data:
        if hasattr(trace, 'y') and trace.y is not None:
            if formato == 'valor':
                texts = [fmt_brl(v) if v is not None else '' for v in trace.y]
            else:
                try:
                    texts = [str(int(v)) if v is not None else '' for v in trace.y]
                except (ValueError, TypeError):
                    texts = ['' for _ in trace.y]
            trace.text         = texts
            trace.textposition = 'outside'
            trace.textfont     = dict(size=11)
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    return fig

# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÃO DA PÁGINA
# ══════════════════════════════════════════════════════════════

st.set_page_config(layout="wide", page_title="Análise de campanha de cobrança")

if not st.session_state.get("logged_in"):
    login_screen()
    st.stop()

st.title("📊 Análise de eficiência de campanha de cobrança via Whatsapp")

# ── Cabeçalho da sidebar ─────────────────────────────────────
st.sidebar.markdown(
    f"👤 **{st.session_state['username']}** "
    f"({'Admin' if is_admin() else 'Usuário'})"
)
if st.sidebar.button("Sair"):
    for key in ["logged_in", "username", "role"]:
        st.session_state.pop(key, None)
    st.rerun()

st.sidebar.markdown("---")

# ══════════════════════════════════════════════════════════════
# SIDEBAR — SELEÇÃO DE CAMPANHA (todos os perfis)
# ══════════════════════════════════════════════════════════════

# ── Sidebar: seleção de campanha ─────────────────────────────
# ── Sidebar: seleção de campanha ─────────────────────────────
st.sidebar.header("📋 Campanhas")

df_meta = load_campanhas_meta()
campanhas_disponiveis = df_meta['nome'].tolist() if not df_meta.empty else []

# Permite selecionar múltiplas campanhas
campanhas_selecionadas_nomes = st.sidebar.multiselect(
    "Selecionar uma ou mais campanhas",
    options=campanhas_disponiveis,
    default=[]
)

campanhas_selecionadas_ids = []
if campanhas_selecionadas_nomes and not df_meta.empty:
    campanhas_selecionadas_ids = df_meta[
        df_meta['nome'].isin(campanhas_selecionadas_nomes)
    ]['id'].tolist()

    # Exibe detalhes das campanhas selecionadas
    for nome_campanha in campanhas_selecionadas_nomes:
        campanha_info = df_meta[df_meta['nome'] == nome_campanha].iloc[0]
        criado_em = pd.to_datetime(campanha_info['criado_em']).strftime('%d/%m/%Y')
        st.sidebar.caption(
            f"**{nome_campanha}**  \n"
            f"📅 Criada em: {criado_em}  \n"
            f"📤 Envios: {int(campanha_info['total_envios']):,}  \n"
            f"👥 Clientes: {int(campanha_info['total_clientes']):,}"
        )
        if is_admin():
            if st.sidebar.button(f"🗑️ Excluir '{nome_campanha}'", key=f"del_{campanha_info['id']}"):
                delete_campanha(campanha_info['id'], nome_campanha)
                st.sidebar.success(f"Campanha '{nome_campanha}' excluída.")
                st.rerun()
    st.sidebar.markdown("---")

st.sidebar.markdown("---")

# ── Sidebar: configurações da análise ────────────────────────
st.sidebar.header("⚙️ Configurações")
janela_dias = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7, key="janela_dias_slider")
executar_analise = st.sidebar.button("▶️ Executar Análise", use_container_width=True, key="executar_analise_btn")

# ── Sidebar: área administrativa (somente admin) ──────────────
if is_admin():
    st.sidebar.markdown("---")
    st.sidebar.header("🔧 Administração")

    with st.sidebar.expander("➕ Nova Campanha"):
        nome_nova = st.text_input("Nome da campanha", key="nome_nova_campanha_input")
        uploaded_envios_admin   = st.file_uploader("Base de Envios (.xlsx)",   type=["xlsx"], key="up_env")
        uploaded_clientes_admin = st.file_uploader("Base de Clientes (.xlsx)", type=["xlsx"], key="up_cli")
        if st.button("💾 Salvar campanha"):
            if not nome_nova.strip():
                st.error("Informe um nome para a campanha.")
            elif uploaded_envios_admin is None:
                st.error("Faça upload da base de envios.")
            elif uploaded_clientes_admin is None:
                st.error("Faça upload da base de clientes.")
            else:
                df_env_tmp = load_and_process_envios(uploaded_envios_admin)
                df_cli_tmp = load_and_process_clientes(uploaded_clientes_admin)
                if df_env_tmp is not None and df_cli_tmp is not None:
                    with st.spinner("Salvando no GitHub..."):
                        cid, erro = save_campanha(nome_nova.strip(), df_env_tmp, df_cli_tmp)
                    if erro:
                        st.error(erro)
                    else:
                        st.success(f"Campanha '{nome_nova}' salva! ID: `{cid}`")
                        st.rerun()

    with st.sidebar.expander("💰 Base de Pagamentos"):
        pag_gh = load_pagamentos_github()
        if pag_gh is not None:
            st.caption(f"✅ Base atual: {len(pag_gh):,} registros")
        else:
            st.caption("⚠️ Nenhuma base salva ainda.")
        uploaded_pag = st.file_uploader(
            "Enviar/Atualizar (.csv, .xlsx, .parquet)",
            type=["csv", "xlsx", "parquet"],
            key="up_pag"
        )
        if st.button("⬆️ Enviar para o GitHub"):
            if uploaded_pag is None:
                st.error("Selecione um arquivo de pagamentos.")
            else:
                df_pag_tmp = load_and_process_pagamentos(uploaded_pag)
                if df_pag_tmp is not None:
                    with st.spinner("Atualizando..."):
                        ok, total, novos = update_pagamentos_github(df_pag_tmp)
                    if ok:
                        st.success(f"Atualizado! Total: {total:,} | Novos: {novos:,}")
                    else:
                        st.error("Erro ao salvar no GitHub.")

# ══════════════════════════════════════════════════════════════
# RESOLUÇÃO DOS DADOS PARA ANÁLISE ACUMULADA
# ══════════════════════════════════════════════════════════════

df_envios_agregado   = pd.DataFrame() # Inicializa como DataFrame vazio
df_clientes_agregado = pd.DataFrame() # Inicializa como DataFrame vazio
df_pagamentos        = None

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
            if df_env_temp is not None:
                df_env_temp['CAMPANHA_ID'] = campanha_id # Adiciona ID da campanha para rastreamento
                lista_df_envios.append(df_env_temp)

            df_cli_temp = load_campanha_clientes(campanha_id)
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
janela_dias      = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7)
executar_analise = st.sidebar.button("▶️ Executar Análise", use_container_width=True)

# ══════════════════════════════════════════════════════════════
# SIDEBAR — ADMINISTRAÇÃO (somente admin)
# ══════════════════════════════════════════════════════════════

if is_admin():
    st.sidebar.markdown("---")
    st.sidebar.header("🔧 Administração")

    with st.sidebar.expander("➕ Nova Campanha"):
        nome_nova               = st.text_input("Nome da campanha")
        uploaded_envios_admin   = st.file_uploader("Base de Envios (.xlsx)",   type=["xlsx"], key="up_env_admin")
        uploaded_clientes_admin = st.file_uploader("Base de Clientes (.xlsx)", type=["xlsx"], key="up_cli_admin")
        if st.button("💾 Salvar campanha"):
            if not nome_nova.strip():
                st.error("Informe um nome para a campanha.")
            elif uploaded_envios_admin is None:
                st.error("Faça upload da base de envios.")
            elif uploaded_clientes_admin is None:
                st.error("Faça upload da base de clientes.")
            else:
                df_env_tmp = load_and_process_envios(uploaded_envios_admin)
                df_cli_tmp = load_and_process_clientes(uploaded_clientes_admin)
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
        if st.button("⬆️ Enviar para o GitHub"):
            if uploaded_pag_admin is None:
                st.error("Selecione um arquivo de pagamentos.")
            else:
                df_pag_tmp = load_and_process_pagamentos(uploaded_pag_admin)
                if df_pag_tmp is not None:
                    with st.spinner("Atualizando..."):
                        ok, total, novos = update_pagamentos_github(df_pag_tmp)
                    if ok:
                        st.success(f"Atualizado! Total: {total:,} | Novos: {novos:,}")
                    else:
                        st.error("Erro ao salvar no GitHub.")

# ══════════════════════════════════════════════════════════════
# CARREGAMENTO DOS DADOS
# ══════════════════════════════════════════════════════════════

df_envios     = None
df_clientes   = None
df_pagamentos = None

if campanha_selecionada is not None:
    with st.spinner("Carregando dados da campanha..."):
        df_envios   = load_campanha_envios(campanha_selecionada['id'])
        df_clientes = load_campanha_clientes(campanha_selecionada['id'])

    if df_envios is not None:
        st.sidebar.success(f"✅ Envios carregados ({len(df_envios):,})")
    else:
        st.sidebar.error("Erro ao carregar envios.")

    if df_clientes is not None:
        st.sidebar.success(f"✅ Clientes carregados ({len(df_clientes):,})")
    else:
        st.sidebar.error("Erro ao carregar clientes.")

    with st.spinner("Carregando pagamentos..."):
        df_pagamentos = load_pagamentos_github()

    if df_pagamentos is not None:
        st.sidebar.success(f"✅ Pagamentos carregados ({len(df_pagamentos):,})")
    else:
        st.sidebar.warning("⚠️ Base de pagamentos não encontrada. Faça o upload na seção Administração.")

dados_prontos = (
    df_envios     is not None and
    df_clientes   is not None and
    df_pagamentos is not None
)

# ══════════════════════════════════════════════════════════════
# ANÁLISE
# ══════════════════════════════════════════════════════════════

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
            x='Número de Notificacoes', y='Clientes que Pagaram',
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
    elif df_pagamentos is None:
        st.warning("Base de pagamentos não disponível. Um administrador precisa fazer o upload.")
    elif df_envios_agregado is None or df_envios_agregado.empty:
        st.warning("Não foi possível carregar os envios das campanhas selecionadas.")
    elif df_clientes_agregado is None or df_clientes_agregado.empty:
        st.warning("Não foi possível carregar os clientes das campanhas selecionadas.")

elif not executar_analise:
    if not campanhas_selecionadas_ids:
        st.info("👈 Selecione uma ou mais campanhas na barra lateral para começar.")
    else:
        st.info("👈 Clique em **Executar Análise** na barra lateral para gerar os resultados.")
