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
# CONFIGURAÇÃO GITHUB
# Configure em .streamlit/secrets.toml:
#
# [github]
# token  = "ghp_seu_token_aqui"
# repo   = "seu_usuario/seu_repositorio"
# branch = "main"
# ══════════════════════════════════════════════════════════════

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
    r = requests.get(url, headers=get_github_headers())
    if r.status_code == 200:
        data    = r.json()
        content = base64.b64decode(data["content"])
        return content, data["sha"]
    return None, None

def save_file_to_github(path, content_bytes, message):
    token, repo, branch = get_github_config()
    if not token:
        return False
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    _, sha = get_file_from_github(path)
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
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
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
        return None, "Envios salvos, mas erro ao salvar clientes no GitHub."
    df_meta = load_campanhas_meta()
    nova = pd.DataFrame([{
        'id':             campanha_id,
        'nome':           nome,
        'criado_em':      pd.Timestamp.now(),
        'total_envios':   df_envios['TELEFONE_ENVIO'].nunique(),
        'total_clientes': len(df_clientes)
    }])
    df_meta = pd.concat([df_meta, nova], ignore_index=True)
    ok_meta = save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} criada")
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
    return ok, len(df_combined), novos

# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÃO DA PÁGINA
# ══════════════════════════════════════════════════════════════

st.set_page_config(layout="wide", page_title="Análise de campanha de cobrança")

st.title("📊 Análise de eficiência de campanha de cobrança via Whatsapp")
st.markdown("Faça o upload dos seus arquivos para analisar a performance da campanha de notificações.")

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE PROCESSAMENTO (INALTERADAS)
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

                st.sidebar.success("Arquivo de Pagamentos processado com sucesso!")
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
            if df is None:
                raise ValueError("Não foi possível ler o arquivo CSV com as codificações tentadas.")
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
            trace.text = texts
            trace.textposition = 'outside'
            trace.textfont = dict(size=11)
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    return fig

# ══════════════════════════════════════════════════════════════
# INTERFACE — SIDEBAR
# ══════════════════════════════════════════════════════════════

# --- Interface Streamlit ---

token, repo, branch = get_github_config()
github_ok = token is not None

if github_ok:
    st.sidebar.success(f"✅ GitHub: `{repo}`")
else:
    st.sidebar.warning("⚠️ GitHub não configurado.")

# ── Campanhas salvas ──────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.header("📋 Campanhas")

df_meta = load_campanhas_meta() if github_ok else pd.DataFrame(columns=['id','nome','criado_em','total_envios','total_clientes'])
campanhas_disponiveis = df_meta['nome'].tolist() if not df_meta.empty else []

campanha_selecionada_nome = st.sidebar.selectbox(
    "Selecionar campanha salva",
    options=["(nenhuma)"] + campanhas_disponiveis
)

campanha_selecionada = None
if campanha_selecionada_nome != "(nenhuma)" and not df_meta.empty:
    campanha_selecionada = df_meta[df_meta['nome'] == campanha_selecionada_nome].iloc[0]
    st.sidebar.caption(
        f"Envios: {int(campanha_selecionada['total_envios']):,} | "
        f"Clientes: {int(campanha_selecionada['total_clientes']):,} | "
        f"Criada em: {pd.to_datetime(campanha_selecionada['criado_em']).strftime('%d/%m/%Y')}"
    )
    if st.sidebar.button("🗑️ Excluir esta campanha"):
        delete_campanha(campanha_selecionada['id'], campanha_selecionada_nome)
        st.sidebar.success(f"Campanha '{campanha_selecionada_nome}' excluída.")
        st.rerun()

# ── Uploads ───────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.header("Upload de Arquivos")
uploaded_envios     = st.sidebar.file_uploader("1. Base de Envios (.xlsx)", type=["xlsx"])
uploaded_pagamentos = st.sidebar.file_uploader("2. Base de Pagamentos (.csv, .xlsx ou .parquet)", type=["csv", "xlsx", "parquet"])
uploaded_clientes   = st.sidebar.file_uploader("3. Base de Clientes (.xlsx)", type=["xlsx"])

st.sidebar.header("Configurações da Análise")
janela_dias      = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7)
executar_analise = st.sidebar.button("▶️ Executar Análise")

# ── Resolver fontes de dados ──────────────────────────────────
df_envios     = None
df_pagamentos = None
df_clientes   = None

# Carrega do GitHub se campanha selecionada
if campanha_selecionada is not None:
    with st.spinner("Carregando dados da campanha..."):
        df_envios   = load_campanha_envios(campanha_selecionada['id'])
        df_clientes = load_campanha_clientes(campanha_selecionada['id'])
    if df_envios is not None:
        st.sidebar.success(f"✅ Envios do GitHub ({len(df_envios):,} registros)")
    if df_clientes is not None:
        st.sidebar.success(f"✅ Clientes do GitHub ({len(df_clientes):,} registros)")

# Upload local sobrescreve GitHub
if uploaded_envios:
    df_envios = load_and_process_envios(uploaded_envios)
if uploaded_clientes:
    df_clientes = load_and_process_clientes(uploaded_clientes)
if uploaded_pagamentos:
    df_pagamentos = load_and_process_pagamentos(uploaded_pagamentos)
elif 'df_pagamentos_github' in st.session_state:
    df_pagamentos = st.session_state['df_pagamentos_github']
    st.sidebar.info(f"Usando pagamentos do GitHub ({len(df_pagamentos):,} registros)")

# ── Salvar nova campanha (só após df_envios e df_clientes estarem definidos) ──
if github_ok:
    with st.sidebar.expander("➕ Salvar nova campanha"):
        nome_nova = st.text_input("Nome da campanha")
        if st.button("💾 Salvar campanha"):
            if not nome_nova.strip():
                st.error("Informe um nome para a campanha.")
            elif df_envios is None:
                st.error("Carregue a base de envios primeiro.")
            elif df_clientes is None:
                st.error("Carregue a base de clientes primeiro.")
            else:
                with st.spinner("Salvando no GitHub..."):
                    cid, erro = save_campanha(nome_nova.strip(), df_envios, df_clientes)
                if erro:
                    st.error(erro)
                else:
                    st.success(f"Campanha '{nome_nova}' salva! ID: `{cid}`")
                    st.rerun()

    with st.sidebar.expander("💰 Enviar pagamentos para o GitHub"):
        if st.button("⬆️ Enviar"):
            if df_pagamentos is None:
                st.error("Carregue a base de pagamentos primeiro.")
            else:
                with st.spinner("Atualizando..."):
                    ok, total, novos = update_pagamentos_github(df_pagamentos)
                if ok:
                    st.success(f"Atualizado! Total: {total:,} | Novos: {novos:,}")
                else:
                    st.error("Erro ao salvar no GitHub.")

    with st.sidebar.expander("☁️ Usar pagamentos do GitHub"):
        pag_gh = load_pagamentos_github()
        if pag_gh is not None:
            st.caption(f"Disponível: {len(pag_gh):,} registros")
            if st.button("📥 Carregar"):
                st.session_state['df_pagamentos_github'] = pag_gh
                st.success("Carregado!")
                st.rerun()
        else:
            st.caption("Nenhuma base salva no GitHub.")

if st.sidebar.checkbox("Mostrar pré-visualização dos dados processados"):
    if df_envios is not None:
        st.subheader("Pré-visualização da Base de Envios")
        st.dataframe(df_envios.head())
    if df_pagamentos is not None:
        st.subheader("Pré-visualização da Base de Pagamentos")
        st.dataframe(df_pagamentos.head())
    if df_clientes is not None:
        st.subheader("Pré-visualização da Base de Clientes")
        st.dataframe(df_clientes.head())

# ══════════════════════════════════════════════════════════════
# ANÁLISE PRINCIPAL (inalterada)
# ══════════════════════════════════════════════════════════════

if executar_analise:
    if df_envios is not None and df_pagamentos is not None and df_clientes is not None:

        total_clientes_notificados = df_envios['TELEFONE_ENVIO'].nunique()

        df_telefones_unicos_envios = df_envios[['TELEFONE_ENVIO']].drop_duplicates()
        df_lookup_divida = pd.merge(
            df_telefones_unicos_envios,
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

        df_campanha_unique_notifications = df_campanha.drop_duplicates(subset=['MATRICULA', 'DATA_ENVIO'])

        if not df_campanha_unique_notifications.empty:

            df_resultados = pd.merge(
                df_campanha_unique_notifications,
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

            clientes_que_pagaram_matriculas = df_pagamentos_campanha['MATRICULA'].nunique()
            valor_total_arrecadado   = df_pagamentos_campanha['VALOR_PAGO'].sum() if not df_pagamentos_campanha.empty else 0
            taxa_eficiencia_clientes = (clientes_que_pagaram_matriculas / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0
            taxa_eficiencia_valor    = (valor_total_arrecadado / total_divida_notificados * 100) if total_divida_notificados > 0 else 0
            ticket_medio             = (valor_total_arrecadado / clientes_que_pagaram_matriculas) if clientes_que_pagaram_matriculas > 0 else 0
            custo_campanha           = total_clientes_notificados * 0.05
            roi                      = ((valor_total_arrecadado - custo_campanha) / custo_campanha * 100) if custo_campanha > 0 else 0

            aba1, aba2, aba3, aba4, aba5 = st.tabs([
                "📊 Visão Geral",
                "🏙️ Cidade e Diretoria",
                "📅 Análise das Faturas",
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
                    st.metric("Total de clientes notificados", f"{total_clientes_notificados:,}")
                with col2:
                    st.metric("Clientes que pagaram na janela", f"{clientes_que_pagaram_matriculas:,}")
                with col3:
                    st.metric("Taxa de eficiência (clientes)", f"{taxa_eficiencia_clientes:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

                col4, col5, col6 = st.columns(3)
                with col4:
                    st.metric("Valor total arrecadado na campanha", fmt_brl(valor_total_arrecadado))
                with col5:
                    st.metric("Taxa de eficiência (valor)", f"{taxa_eficiencia_valor:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))
                with col6:
                    st.metric("Ticket médio por cliente", fmt_brl(ticket_medio))

                col7, col8 = st.columns(2)
                with col7:
                    st.metric("Custo estimado da campanha (R$ 0,05/msg)", fmt_brl(custo_campanha))
                with col8:
                    st.metric("ROI da campanha", f"{roi:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

                if not df_pagamentos_campanha.empty:
                    st.subheader("Pagamentos ao Longo do Tempo")

                    pagamentos_por_dia = df_pagamentos_campanha.groupby('DATA_PAGAMENTO').agg(
                        Valor_Pago=('VALOR_PAGO', 'sum'),
                        Quantidade=('MATRICULA', 'count')
                    ).reset_index()

                    fig_linha = px.line(
                        pagamentos_por_dia,
                        x='DATA_PAGAMENTO', y='Valor_Pago',
                        title='Valor Pago ao Longo do Tempo',
                        labels={'DATA_PAGAMENTO': 'Data do Pagamento', 'Valor_Pago': 'Valor Pago (R$)'},
                        markers=True
                    )
                    fig_linha.update_layout(xaxis_title="Data do Pagamento", yaxis_title="Valor Pago (R$)")
                    st.plotly_chart(fig_linha, use_container_width=True, key="fig_linha")

                    st.subheader("Distribuição dos Pagamentos por Dia após o Envio")
                    dist_dias = df_pagamentos_campanha.groupby('DIAS_APOS_ENVIO').agg(
                        Quantidade=('MATRICULA', 'count'),
                        Valor_Pago=('VALOR_PAGO', 'sum')
                    ).reset_index()

                    fig_dist = px.bar(
                        dist_dias,
                        x='DIAS_APOS_ENVIO', y='Valor_Pago',
                        title='Valor Pago por Dia após o Envio da Notificação',
                        labels={'DIAS_APOS_ENVIO': 'Dias após o Envio', 'Valor_Pago': 'Valor Pago (R$)'},
                        hover_data={'Quantidade': True}
                    )
                    fig_dist = add_bar_labels(fig_dist, 'valor')
                    fig_dist.update_layout(xaxis_title="Dias após o Envio", yaxis_title="Valor Pago (R$)")
                    st.plotly_chart(fig_dist, use_container_width=True, key="fig_dist")

                    tab_dist = dist_dias.copy()
                    tab_dist.columns = ['Dias após o Envio', 'Quantidade de Pagamentos', 'Valor Pago']
                    tab_dist['Valor Pago'] = tab_dist['Valor Pago'].apply(fmt_brl)
                    st.dataframe(tab_dist, use_container_width=True, hide_index=True)

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

                            tab_diretoria_canal = diretoria_canal.copy()
                            tab_diretoria_canal.columns = ['Diretoria', 'Canal de Pagamento', 'Valor Pago']
                            tab_diretoria_canal['Valor Pago'] = tab_diretoria_canal['Valor Pago'].apply(fmt_brl)
                            tab_diretoria_canal = tab_diretoria_canal.sort_values(['Diretoria', 'Canal de Pagamento'])
                            st.dataframe(tab_diretoria_canal, use_container_width=True, hide_index=True)

                    if not tem_cidade and not tem_diretoria:
                        st.info("Colunas 'CIDADE' e 'DIRETORIA' não encontradas na base de clientes.")
                else:
                    st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

            # ══════════════════════════════════════════════════
            # ABA 3 — ANÁLISE DAS FATURAS
            # ══════════════════════════════════════════════════
            with aba3:
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

            # ══════════════════════════════════════════════════
            # ABA 4 — CANAL DE PAGAMENTO
            # ══════════════════════════════════════════════════
            with aba4:
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

                    tab_canal = pd.merge(pagamentos_por_canal, qtd_por_canal, on='TIPO_PAGAMENTO')
                    tab_canal.columns = ['Canal de Pagamento', 'Valor Total Pago', 'Clientes que Pagaram']
                    tab_canal['Valor Total Pago'] = tab_canal['Valor Total Pago'].apply(fmt_brl)
                    st.dataframe(tab_canal, use_container_width=True, hide_index=True)

                else:
                    st.info("Coluna 'Tipo Pagamento' não encontrada no arquivo de pagamentos.")

            # ══════════════════════════════════════════════════
            # ABA 5 — DETALHES
            # ══════════════════════════════════════════════════
            with aba5:
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
                        label="Baixar Detalhes dos Pagamentos da Campanha (CSV)",
                        data=csv_output,
                        file_name="pagamentos_campanha.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

        else:
            st.error("Não foi possível processar um ou mais arquivos. Verifique os formatos e as colunas esperadas ou se há matrículas válidas após o cruzamento.")
    else:
        st.warning("Por favor, carregue todos os três arquivos para iniciar a análise.")


