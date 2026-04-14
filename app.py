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
        secrets = st.secrets["users"]
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
# GITHUB
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

def get_file_from_github(path):
    token, repo, branch = get_github_config()
    if not token:
        return None, None
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r   = requests.get(url, headers=get_github_headers())
    if r.status_code == 200:
        data    = r.json()
        content = base64.b64decode(data["content"])
        return content, data["sha"]
    return None, None

def save_file_to_github(path, content_bytes, message):
    token, repo, branch = get_github_config()
    if not token:
        return False
    url     = f"https://api.github.com/repos/{repo}/contents/{path}"
    _, sha  = get_file_from_github(path)
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
    url    = f"https://api.github.com/repos/{repo}/contents/{path}"
    _, sha = get_file_from_github(path)
    if not sha:
        return True
    payload = {"message": message, "sha": sha, "branch": branch}
    r = requests.delete(url, headers=get_github_headers(), data=json.dumps(payload))
    return r.status_code == 200

def df_to_parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()

def parquet_bytes_to_df(content_bytes):
    return pd.read_parquet(io.BytesIO(content_bytes))

# ══════════════════════════════════════════════════════════════
# CAMPANHAS
# ══════════════════════════════════════════════════════════════

META_PATH = "data/campanhas_meta.parquet"
PAG_PATH  = "data/pagamentos.parquet"

def load_campanhas_meta():
    content, _ = get_file_from_github(META_PATH)
    if content:
        return parquet_bytes_to_df(content)
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
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet")
    if content:
        return parquet_bytes_to_df(content)
    return None

def load_campanha_clientes(campanha_id):
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet")
    if content:
        return parquet_bytes_to_df(content)
    return None

def delete_campanha(campanha_id, nome):
    df_meta = load_campanhas_meta()
    df_meta = df_meta[df_meta['id'] != campanha_id]
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} removida")
    delete_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet",   f"Campanha {nome}: envios removidos")
    delete_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet", f"Campanha {nome}: clientes removidos")

# ══════════════════════════════════════════════════════════════
# PAGAMENTOS
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_pagamentos_github():
    content, _ = get_file_from_github(PAG_PATH)
    if content:
        return parquet_bytes_to_df(content)
    return None

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
    load_pagamentos_github.clear()
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
            st.error(f"Arquivo de Envios: colunas 'To' e 'Send At' não encontradas.")
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
            df_pag = pd.read_parquet(uploaded_file)
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

# ── Sidebar: cabeçalho do usuário ────────────────────────────
st.sidebar.markdown(
    f"👤 **{st.session_state['username']}** "
    f"({'Admin' if is_admin() else 'Usuário'})"
)
if st.sidebar.button("Sair"):
    for key in ["logged_in", "username", "role"]:
        st.session_state.pop(key, None)
    st.rerun()

st.sidebar.markdown("---")

# ── Sidebar: seleção de campanha ─────────────────────────────
st.sidebar.header("📋 Campanha")

df_meta               = load_campanhas_meta()
campanhas_disponiveis = df_meta['nome'].tolist() if not df_meta.empty else []

campanha_selecionada_nome = st.sidebar.selectbox(
    "Selecionar campanha",
    options=["(nenhuma)"] + campanhas_disponiveis
)

campanha_selecionada = None
if campanha_selecionada_nome != "(nenhuma)" and not df_meta.empty:
    campanha_selecionada = df_meta[df_meta['nome'] == campanha_selecionada_nome].iloc[0]
    criado_em = pd.to_datetime(campanha_selecionada['criado_em']).strftime('%d/%m/%Y')
    st.sidebar.caption(
        f"📅 Criada em: {criado_em}  \n"
        f"📤 Envios: {int(campanha_selecionada['total_envios']):,}  \n"
        f"👥 Clientes: {int(campanha_selecionada['total_clientes']):,}"
    )
    if is_admin():
        if st.sidebar.button("🗑️ Excluir esta campanha"):
            delete_campanha(campanha_selecionada['id'], campanha_selecionada_nome)
            st.sidebar.success(f"Campanha '{campanha_selecionada_nome}' excluída.")
            st.rerun()

st.sidebar.markdown("---")

# ── Sidebar: configurações da análise ────────────────────────
st.sidebar.header("⚙️ Configurações")
janela_dias      = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7)
executar_analise = st.sidebar.button("▶️ Executar Análise", use_container_width=True)

# ── Sidebar: área administrativa (somente admin) ──────────────
if is_admin():
    st.sidebar.markdown("---")
    st.sidebar.header("🔧 Administração")

    with st.sidebar.expander("➕ Nova Campanha"):
        nome_nova               = st.text_input("Nome da campanha")
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
# RESOLUÇÃO DOS DADOS
# Campanha e pagamentos vêm sempre do GitHub.
# Upload local disponível apenas para admin e apenas
# para substituir pontualmente durante a sessão.
# ══════════════════════════════════════════════════════════════

df_envios     = None
df_clientes   = None
df_pagamentos = None

# Carrega envios e clientes da campanha selecionada
if campanha_selecionada is not None:
    with st.spinner("Carregando dados da campanha..."):
        df_envios   = load_campanha_envios(campanha_selecionada['id'])
        df_clientes = load_campanha_clientes(campanha_selecionada['id'])

# Pagamentos sempre do GitHub (cache de 5 min)
df_pagamentos = load_pagamentos_github()

# Validação e feedback
dados_prontos = (
    df_envios     is not None and
    df_clientes   is not None and
    df_pagamentos is not None
)

if campanha_selecionada is None:
    st.info("Selecione uma campanha na barra lateral para iniciar a análise.")
elif df_envios is None or df_clientes is None:
    st.error("Não foi possível carregar os dados da campanha do GitHub. Verifique o repositório.")
elif df_pagamentos is None:
    st.error("Base de pagamentos não encontrada no GitHub. Um administrador precisa fazer o upload.")

# ══════════════════════════════════════════════════════════════
# ANÁLISE PRINCIPAL
# ══════════════════════════════════════════════════════════════

if executar_analise and dados_prontos:

    total_clientes_notificados = df_envios['TELEFONE_ENVIO'].nunique()

    df_telefones_unicos = df_envios[['TELEFONE_ENVIO']].drop_duplicates()
    df_lookup_divida    = pd.merge(
        df_telefones_unicos,
        df_clientes[['TELEFONE_CLIENTE', 'SITUACAO']],
        left_on='TELEFONE_ENVIO',
        right_on='TELEFONE_CLIENTE',
        how='left'
    )
    total_divida_notificados = df_lookup_divida['SITUACAO'].sum()

    df_campanha = pd.merge(
        df_envios,
        df_clientes,
        left_on='TELEFONE_ENVIO',
        right_on='TELEFONE_CLIENTE',
        how='left'
    )
    df_campanha.dropna(subset=['MATRICULA_CLIENTE'], inplace=True)
    df_campanha.rename(columns={'MATRICULA_CLIENTE': 'MATRICULA'}, inplace=True)
    df_campanha.drop(columns=['TELEFONE_CLIENTE'], inplace=True)
    df_campanha_unique = df_campanha.drop_duplicates(subset=['MATRICULA', 'DATA_ENVIO'])

    if df_campanha_unique.empty:
        st.error("Nenhuma matrícula válida encontrada após o cruzamento dos dados.")
        st.stop()

    df_resultados = pd.merge(
        df_campanha_unique,
        df_pagamentos,
        left_on='MATRICULA',
        right_on='MATRICULA_PAGAMENTO',
        how='left'
    )

    df_pagamentos_campanha = df_resultados[
        (df_resultados['DATA_PAGAMENTO'] > df_resultados['DATA_ENVIO']) &
        (df_resultados['DATA_PAGAMENTO'] <= df_resultados['DATA_ENVIO'] + timedelta(days=janela_dias))
    ].copy()

    if not df_pagamentos_campanha.empty:
        df_pagamentos_campanha['DIAS_APOS_ENVIO'] = (
            df_pagamentos_campanha['DATA_PAGAMENTO'] - df_pagamentos_campanha['DATA_ENVIO']
        ).dt.days

    clientes_pagaram     = df_pagamentos_campanha['MATRICULA'].nunique()
    valor_arrecadado     = df_pagamentos_campanha['VALOR_PAGO'].sum() if not df_pagamentos_campanha.empty else 0
    taxa_cli             = (clientes_pagaram / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0
    taxa_val             = (valor_arrecadado / total_divida_notificados * 100) if total_divida_notificados > 0 else 0

    # ── KPIs ─────────────────────────────────────────────────
    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Clientes Notificados",    f"{total_clientes_notificados:,}")
    c2.metric("Clientes que Pagaram",    f"{clientes_pagaram:,}")
    c3.metric("Taxa de Conversão",       f"{taxa_cli:.1f}%")
    c4.metric("Valor Arrecadado",        fmt_brl(valor_arrecadado))
    c5.metric("% da Dívida Recuperada",  f"{taxa_val:.1f}%")
    st.markdown("---")

    aba1, aba2, aba3, aba4, aba5 = st.tabs([
        "📈 Visão Geral",
        "🗺️ Geográfica",
        "📄 Faturas",
        "💳 Canal de Pagamento",
        "🔍 Detalhes"
    ])

    # ── ABA 1 — VISÃO GERAL ───────────────────────────────────
    with aba1:
        if not df_pagamentos_campanha.empty:
            st.subheader("Pagamentos por Dia Após o Envio")
            pagamentos_por_dia = df_pagamentos_campanha.groupby('DIAS_APOS_ENVIO').agg(
                Quantidade=('MATRICULA', 'count'),
                Valor_Pago=('VALOR_PAGO', 'sum')
            ).reset_index()

            fig_dia_valor = px.bar(
                pagamentos_por_dia, x='DIAS_APOS_ENVIO', y='Valor_Pago',
                title='Valor Pago por Dia Após o Envio',
                labels={'DIAS_APOS_ENVIO': 'Dias Após o Envio', 'Valor_Pago': 'Valor Pago (R$)'},
                hover_data={'Valor_Pago': ':.2f'}
            )
            fig_dia_valor = add_bar_labels(fig_dia_valor, 'valor')
            fig_dia_valor.update_layout(xaxis_title="Dias Após o Envio", yaxis_title="Valor Pago (R$)")
            st.plotly_chart(fig_dia_valor, use_container_width=True, key="fig_dia_valor")

            fig_dia_qtd = px.bar(
                pagamentos_por_dia, x='DIAS_APOS_ENVIO', y='Quantidade',
                title='Quantidade de Pagamentos por Dia Após o Envio',
                labels={'DIAS_APOS_ENVIO': 'Dias Após o Envio', 'Quantidade': 'Quantidade de Pagamentos'}
            )
            fig_dia_qtd = add_bar_labels(fig_dia_qtd, 'qtd')
            fig_dia_qtd.update_layout(xaxis_title="Dias Após o Envio", yaxis_title="Quantidade de Pagamentos")
            st.plotly_chart(fig_dia_qtd, use_container_width=True, key="fig_dia_qtd")

            tab_dia = pagamentos_por_dia.copy()
            tab_dia.columns = ['Dias Após o Envio', 'Quantidade de Pagamentos', 'Valor Pago']
            tab_dia['Valor Pago'] = tab_dia['Valor Pago'].apply(fmt_brl)
            st.dataframe(tab_dia, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida.")

    # ── ABA 2 — GEOGRÁFICA ────────────────────────────────────
    with aba2:
        if not df_pagamentos_campanha.empty:
            tem_cidade    = 'CIDADE'    in df_pagamentos_campanha.columns
            tem_diretoria = 'DIRETORIA' in df_pagamentos_campanha.columns

            if tem_cidade:
                st.subheader("Análise por Cidade")
                cidade_resumo = df_pagamentos_campanha.groupby('CIDADE').agg(
                    Clientes_que_Pagaram=('MATRICULA', 'nunique'),
                    Valor_Arrecadado=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

                fig_cidade_valor = px.bar(
                    cidade_resumo, x='CIDADE', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Cidade',
                    labels={'CIDADE': 'Cidade', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'},
                    hover_data={'Valor_Arrecadado': ':.2f'}
                )
                fig_cidade_valor = add_bar_labels(fig_cidade_valor, 'valor')
                fig_cidade_valor.update_layout(xaxis_title="Cidade", yaxis_title="Valor Arrecadado (R$)")
                st.plotly_chart(fig_cidade_valor, use_container_width=True, key="fig_cidade_valor")

                fig_cidade_cli = px.bar(
                    cidade_resumo, x='CIDADE', y='Clientes_que_Pagaram',
                    title='Clientes que Pagaram por Cidade',
                    labels={'CIDADE': 'Cidade', 'Clientes_que_Pagaram': 'Clientes que Pagaram'}
                )
                fig_cidade_cli = add_bar_labels(fig_cidade_cli, 'qtd')
                fig_cidade_cli.update_layout(xaxis_title="Cidade", yaxis_title="Clientes que Pagaram")
                st.plotly_chart(fig_cidade_cli, use_container_width=True, key="fig_cidade_cli")

                tab_cidade = cidade_resumo.copy()
                tab_cidade.columns = ['Cidade', 'Clientes que Pagaram', 'Valor Arrecadado']
                tab_cidade['Valor Arrecadado'] = tab_cidade['Valor Arrecadado'].apply(fmt_brl)
                st.dataframe(tab_cidade, use_container_width=True, hide_index=True)

                if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                    st.subheader("Canal de Pagamento por Cidade")
                    cidade_canal = df_pagamentos_campanha.groupby(['CIDADE', 'TIPO_PAGAMENTO'])['VALOR_PAGO'].sum().reset_index()
                    fig_cc = px.bar(
                        cidade_canal, x='CIDADE', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                        title='Valor Pago por Cidade e Canal',
                        labels={'CIDADE': 'Cidade', 'VALOR_PAGO': 'Valor Pago (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                        barmode='stack'
                    )
                    fig_cc.update_layout(xaxis_title="Cidade", yaxis_title="Valor Pago (R$)")
                    st.plotly_chart(fig_cc, use_container_width=True, key="fig_cidade_canal")

            if tem_diretoria:
                st.subheader("Análise por Diretoria")
                diretoria_resumo = df_pagamentos_campanha.groupby('DIRETORIA').agg(
                    Clientes_que_Pagaram=('MATRICULA', 'nunique'),
                    Valor_Arrecadado=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

                fig_dir_valor = px.bar(
                    diretoria_resumo, x='DIRETORIA', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Diretoria',
                    labels={'DIRETORIA': 'Diretoria', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'},
                    hover_data={'Valor_Arrecadado': ':.2f'}
                )
                fig_dir_valor = add_bar_labels(fig_dir_valor, 'valor')
                fig_dir_valor.update_layout(xaxis_title="Diretoria", yaxis_title="Valor Arrecadado (R$)")
                st.plotly_chart(fig_dir_valor, use_container_width=True, key="fig_dir_valor")

                fig_dir_cli = px.bar(
                    diretoria_resumo, x='DIRETORIA', y='Clientes_que_Pagaram',
                    title='Clientes que Pagaram por Diretoria',
                    labels={'DIRETORIA': 'Diretoria', 'Clientes_que_Pagaram': 'Clientes que Pagaram'}
                )
                fig_dir_cli = add_bar_labels(fig_dir_cli, 'qtd')
                fig_dir_cli.update_layout(xaxis_title="Diretoria", yaxis_title="Clientes que Pagaram")
                st.plotly_chart(fig_dir_cli, use_container_width=True, key="fig_dir_cli")

                tab_dir = diretoria_resumo.copy()
                tab_dir.columns = ['Diretoria', 'Clientes que Pagaram', 'Valor Arrecadado']
                tab_dir['Valor Arrecadado'] = tab_dir['Valor Arrecadado'].apply(fmt_brl)
                st.dataframe(tab_dir, use_container_width=True, hide_index=True)

                if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                    st.subheader("Canal de Pagamento por Diretoria")
                    dir_canal = df_pagamentos_campanha.groupby(['DIRETORIA', 'TIPO_PAGAMENTO'])['VALOR_PAGO'].sum().reset_index()
                    fig_dc = px.bar(
                        dir_canal, x='DIRETORIA', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                        title='Valor Pago por Diretoria e Canal',
                        labels={'DIRETORIA': 'Diretoria', 'VALOR_PAGO': 'Valor Pago (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                        barmode='stack'
                    )
                    fig_dc.update_layout(xaxis_title="Diretoria", yaxis_title="Valor Pago (R$)")
                    st.plotly_chart(fig_dc, use_container_width=True, key="fig_dir_canal")

            if not tem_cidade and not tem_diretoria:
                st.info("Colunas 'CIDADE' e 'DIRETORIA' não encontradas na base de clientes.")
        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida.")

    # ── ABA 3 — FATURAS ───────────────────────────────────────
    with aba3:
        if not df_pagamentos_campanha.empty:

            if 'VENCIMENTO' in df_pagamentos_campanha.columns:
                st.subheader("Antiguidade da Dívida Paga")
                df_pagamentos_campanha['ANTIGUIDADE_DIAS'] = (
                    df_pagamentos_campanha['DATA_PAGAMENTO'] - df_pagamentos_campanha['VENCIMENTO']
                ).dt.days

                def classificar_antiguidade(dias):
                    if pd.isna(dias):  return 'Não informado'
                    elif dias <= 10:   return '0-10 dias'
                    elif dias <= 20:   return '11-20 dias'
                    elif dias <= 30:   return '21-30 dias'
                    elif dias <= 60:   return '31-60 dias'
                    else:              return 'Mais de 61 dias'

                df_pagamentos_campanha['FAIXA_ANTIGUIDADE'] = df_pagamentos_campanha['ANTIGUIDADE_DIAS'].apply(classificar_antiguidade)
                ordem_faixas = ['0-10 dias', '11-20 dias', '21-30 dias', '31-60 dias', 'Mais de 61 dias', 'Não informado']

                ant_resumo = df_pagamentos_campanha.groupby('FAIXA_ANTIGUIDADE').agg(
                    Quantidade=('MATRICULA', 'count'),
                    Valor_Pago=('VALOR_PAGO', 'sum')
                ).reset_index()
                ant_resumo['FAIXA_ANTIGUIDADE'] = pd.Categorical(
                    ant_resumo['FAIXA_ANTIGUIDADE'], categories=ordem_faixas, ordered=True
                )
                ant_resumo = ant_resumo.sort_values('FAIXA_ANTIGUIDADE')

                fig_ant_v = px.bar(
                    ant_resumo, x='FAIXA_ANTIGUIDADE', y='Valor_Pago',
                    title='Valor Pago por Faixa de Antiguidade',
                    labels={'FAIXA_ANTIGUIDADE': 'Faixa de Antiguidade', 'Valor_Pago': 'Valor Pago (R$)'},
                    hover_data={'Valor_Pago': ':.2f'}
                )
                fig_ant_v = add_bar_labels(fig_ant_v, 'valor')
                fig_ant_v.update_layout(xaxis_title="Faixa de Antiguidade", yaxis_title="Valor Pago (R$)")
                st.plotly_chart(fig_ant_v, use_container_width=True, key="fig_ant_valor")

                fig_ant_q = px.bar(
                    ant_resumo, x='FAIXA_ANTIGUIDADE', y='Quantidade',
                    title='Quantidade de Pagamentos por Faixa de Antiguidade',
                    labels={'FAIXA_ANTIGUIDADE': 'Faixa de Antiguidade', 'Quantidade': 'Quantidade'}
                )
                fig_ant_q = add_bar_labels(fig_ant_q, 'qtd')
                fig_ant_q.update_layout(xaxis_title="Faixa de Antiguidade", yaxis_title="Quantidade de Pagamentos")
                st.plotly_chart(fig_ant_q, use_container_width=True, key="fig_ant_qtd")

                tab_ant = ant_resumo.copy()
                tab_ant.columns = ['Faixa de Antiguidade', 'Quantidade de Pagamentos', 'Valor Pago']
                tab_ant['Valor Pago'] = tab_ant['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_ant, use_container_width=True, hide_index=True)

            if 'MES_ANO_FATURA' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Mês/Ano da Fatura")
                mes_resumo = df_pagamentos_campanha.groupby(
                    ['ANO_FATURA', 'MES_FATURA', 'MES_ANO_FATURA']
                )['VALOR_PAGO'].sum().reset_index().sort_values(['ANO_FATURA', 'MES_FATURA'])

                fig_mes = px.bar(
                    mes_resumo, x='MES_ANO_FATURA', y='VALOR_PAGO',
                    title='Valor Pago por Mês/Ano da Fatura',
                    labels={'MES_ANO_FATURA': 'Mês/Ano', 'VALOR_PAGO': 'Valor Pago (R$)'},
                    hover_data={'VALOR_PAGO': ':.2f'}
                )
                fig_mes = add_bar_labels(fig_mes, 'valor')
                fig_mes.update_layout(xaxis_title="Mês/Ano da Fatura", yaxis_title="Valor Pago (R$)")
                st.plotly_chart(fig_mes, use_container_width=True, key="fig_mes_ano")

                tab_mes = mes_resumo[['MES_ANO_FATURA', 'VALOR_PAGO']].copy()
                tab_mes.columns = ['Mês/Ano da Fatura', 'Valor Pago']
                tab_mes['Valor Pago'] = tab_mes['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_mes, use_container_width=True, hide_index=True)

            if 'TIPO_FATURA' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Tipo de Fatura")
                tf_resumo = df_pagamentos_campanha.groupby('TIPO_FATURA').agg(
                    Quantidade=('MATRICULA', 'count'),
                    Valor_Pago=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Pago', ascending=False)

                fig_tf = px.bar(
                    tf_resumo, x='TIPO_FATURA', y='Valor_Pago',
                    title='Valor Pago por Tipo de Fatura',
                    labels={'TIPO_FATURA': 'Tipo de Fatura', 'Valor_Pago': 'Valor Pago (R$)'},
                    color='TIPO_FATURA',
                    hover_data={'Valor_Pago': ':.2f', 'Quantidade': True}
                )
                fig_tf = add_bar_labels(fig_tf, 'valor')
                fig_tf.update_layout(xaxis_title="Tipo de Fatura", yaxis_title="Valor Pago (R$)", showlegend=False)
                st.plotly_chart(fig_tf, use_container_width=True, key="fig_tipo_fatura")

                tab_tf = tf_resumo.copy()
                tab_tf.columns = ['Tipo de Fatura', 'Quantidade', 'Valor Pago']
                tab_tf['Valor Pago'] = tab_tf['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_tf, use_container_width=True, hide_index=True)

            if 'UTILIZACAO' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Utilização (Sub. Categoria)")
                ut_resumo = df_pagamentos_campanha.groupby('UTILIZACAO').agg(
                    Quantidade=('MATRICULA', 'count'),
                    Valor_Pago=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Pago', ascending=False)

                fig_ut = px.bar(
                    ut_resumo, x='UTILIZACAO', y='Valor_Pago',
                    title='Valor Pago por Utilização',
                    labels={'UTILIZACAO': 'Utilização', 'Valor_Pago': 'Valor Pago (R$)'},
                    color='UTILIZACAO',
                    hover_data={'Valor_Pago': ':.2f', 'Quantidade': True}
                )
                fig_ut = add_bar_labels(fig_ut, 'valor')
                fig_ut.update_layout(xaxis_title="Utilização", yaxis_title="Valor Pago (R$)", showlegend=False)
                st.plotly_chart(fig_ut, use_container_width=True, key="fig_utilizacao")

                tab_ut = ut_resumo.copy()
                tab_ut.columns = ['Utilização', 'Quantidade', 'Valor Pago']
                tab_ut['Valor Pago'] = tab_ut['Valor Pago'].apply(fmt_brl)
                st.dataframe(tab_ut, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida.")

    # ── ABA 4 — CANAL DE PAGAMENTO ────────────────────────────
    with aba4:
        if not df_pagamentos_campanha.empty and 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:

            st.subheader("Valor Arrecadado por Canal de Pagamento")
            canal_valor = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['VALOR_PAGO'].sum().reset_index()
            canal_valor = canal_valor.sort_values('VALOR_PAGO', ascending=False)

            fig_cv = px.bar(
                canal_valor, x='TIPO_PAGAMENTO', y='VALOR_PAGO',
                title='Valor Arrecadado por Canal de Pagamento',
                labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'VALOR_PAGO': 'Valor Total Pago (R$)'},
                color='TIPO_PAGAMENTO',
                hover_data={'VALOR_PAGO': ':.2f'}
            )
            fig_cv = add_bar_labels(fig_cv, 'valor')
            fig_cv.update_layout(xaxis_title="Canal de Pagamento", yaxis_title="Valor Total Pago (R$)", showlegend=False)
            st.plotly_chart(fig_cv, use_container_width=True, key="fig_canal_valor")

            st.subheader("Clientes que Pagaram por Canal")
            canal_cli = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['MATRICULA'].nunique().reset_index()
            canal_cli.rename(columns={'MATRICULA': 'Clientes que Pagaram'}, inplace=True)
            canal_cli = canal_cli.sort_values('Clientes que Pagaram', ascending=False)

            fig_cc = px.bar(
                canal_cli, x='TIPO_PAGAMENTO', y='Clientes que Pagaram',
                title='Clientes que Pagaram por Canal',
                labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'Clientes que Pagaram': 'Clientes que Pagaram'},
                color='TIPO_PAGAMENTO'
            )
            fig_cc = add_bar_labels(fig_cc, 'qtd')
            fig_cc.update_layout(xaxis_title="Canal de Pagamento", yaxis_title="Clientes que Pagaram", showlegend=False)
            st.plotly_chart(fig_cc, use_container_width=True, key="fig_canal_cli")

            tab_canal = pd.merge(canal_valor, canal_cli, on='TIPO_PAGAMENTO')
            tab_canal.columns = ['Canal de Pagamento', 'Valor Total Pago', 'Clientes que Pagaram']
            tab_canal['Valor Total Pago'] = tab_canal['Valor Total Pago'].apply(fmt_brl)
            st.dataframe(tab_canal, use_container_width=True, hide_index=True)
        else:
            st.info("Coluna 'TIPO_PAGAMENTO' não encontrada nos dados de pagamento.")

    # ── ABA 5 — DETALHES ──────────────────────────────────────
    with aba5:
        if not df_pagamentos_campanha.empty:
            st.subheader("Detalhes dos Pagamentos Atribuídos à Campanha")

            colunas_possiveis = [
                'MATRICULA', 'CIDADE', 'DIRETORIA', 'TELEFONE_ENVIO',
                'DATA_ENVIO', 'DATA_PAGAMENTO', 'VENCIMENTO',
                'VALOR_PAGO', 'DIAS_APOS_ENVIO',
                'TIPO_FATURA', 'UTILIZACAO', 'TIPO_PAGAMENTO'
            ]
            colunas_exibir = [c for c in colunas_possiveis if c in df_pagamentos_campanha.columns]
            df_detalhes    = df_pagamentos_campanha[colunas_exibir].drop_duplicates(
                subset=['MATRICULA', 'DATA_PAGAMENTO', 'VALOR_PAGO']
            )

            st.dataframe(df_detalhes, use_container_width=True, hide_index=True)

            csv_output = df_detalhes.to_csv(index=False, sep=';', decimal=',')
            st.download_button(
                label="⬇️ Baixar CSV dos Pagamentos",
                data=csv_output,
                file_name="pagamentos_campanha.csv",
                mime="text/csv"
            )
        else:
            st.info("Nenhum pagamento encontrado dentro da janela definida.")

elif executar_analise and not dados_prontos:
    if campanha_selecionada is None:
        st.warning("Selecione uma campanha antes de executar a análise.")
    elif df_pagamentos is None:
        st.warning("Base de pagamentos não disponível. Um administrador precisa fazer o upload.")
