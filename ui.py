import streamlit as st
import requests
import pandas as pd
from io import BytesIO

# =========================
# CONFIG
# =========================
API_URL = "https://web-production-285a2.up.railway.app"

st.set_page_config(
    page_title="ETL Finance",
    page_icon="💰",
    layout="wide"
)

# =========================
# CUSTOM CSS
# =========================
st.markdown("""
<style>

.main {
    background-color: #0f1117;
    color: white;
}

.stButton>button {
    width: 100%;
    border-radius: 10px;
    height: 45px;
    background-color: #4f46e5;
    color: white;
    border: none;
    font-weight: bold;
}

.stDownloadButton>button {
    width: 100%;
    border-radius: 10px;
    height: 45px;
    background-color: #16a34a;
    color: white;
    border: none;
    font-weight: bold;
}

.block-container {
    padding-top: 2rem;
}

.card {
    background-color: #1c1f26;
    padding: 20px;
    border-radius: 16px;
    margin-bottom: 20px;
    border: 1px solid #2d3748;
}

.metric {
    text-align: center;
    padding: 20px;
    border-radius: 12px;
    background: #151821;
}

.small-text {
    color: #9ca3af;
    font-size: 14px;
}

[data-testid="stDataFrame"] {
    border: 1px solid #2d3748;
    border-radius: 12px;
}

</style>
""", unsafe_allow_html=True)

# =========================
# HEADER
# =========================
st.title("💰 ETL Finance Platform")
st.caption(
    "Upload financeiro inteligente com auto-detecção, classificação e validação"
)

# =========================
# UPLOAD SECTION
# =========================
st.markdown("---")
st.markdown("## 📤 Upload de Arquivos")

col1, col2 = st.columns(2)

with col1:

    input_file = st.file_uploader(
        "Arquivo Financeiro",
        type=["csv", "xlsx", "xls"]
    )

with col2:

    fornecedores_file = st.file_uploader(
        "Tabela de Fornecedores",
        type=["xlsx", "xls", "csv"]
    )

# =========================
# PREVIEW
# =========================
if input_file:

    st.markdown("---")
    st.markdown("## 👀 Preview Inteligente")

    if st.button("🔍 Gerar Preview"):

        with st.spinner("Analisando arquivo financeiro..."):

            files = {
                "input_file": (
                    input_file.name,
                    input_file,
                    input_file.type
                )
            }

            try:

                # =====================================
                # REQUEST
                # =====================================
                res = requests.post(
                    f"{API_URL}/preview",
                    files=files,
                    timeout=180
                )

                # =====================================
                # STATUS CHECK
                # =====================================
                if res.status_code != 200:

                    st.error(
                        f"Erro da API: {res.status_code}"
                    )

                    st.stop()

                # =====================================
                # JSON PARSE
                # =====================================
                try:

                    data = res.json()

                except Exception:

                    st.error(
                        "Resposta inválida da API."
                    )

                    st.stop()

                # =====================================
                # SUCCESS
                # =====================================
                if data.get("status") == "success":

                    st.success(
                        "Preview gerado com sucesso"
                    )

                    # =================================
                    # DETECTED MAPPING
                    # =================================
                    st.markdown(
                        "### 🧠 Colunas Detectadas"
                    )

                    st.json(
                        data.get(
                            "auto_mapping",
                            {}
                        )
                    )

                    # =================================
                    # MONEY COLUMNS
                    # =================================
                    money_cols = data.get(
                        "money_columns",
                        []
                    )

                    if money_cols:

                        st.markdown(
                            "### 💰 Colunas Financeiras Detectadas"
                        )

                        st.success(
                            ", ".join(money_cols)
                        )

                    # =================================
                    # DATAFRAME
                    # =================================
                    sample_df = pd.DataFrame(
                        data.get(
                            "sample_data",
                            []
                        )
                    )

                    if not sample_df.empty:

                        # =============================
                        # FORMAT VALOR
                        # =============================
                        if "valor" in sample_df.columns:

                            sample_df["valor"] = pd.to_numeric(
                                sample_df["valor"],
                                errors="coerce"
                            ).round(2)

                            # =========================
                            # TRANSACTION TYPE
                            # =========================
                            sample_df["tipo"] = sample_df[
                                "valor"
                            ].apply(
                                lambda x:
                                "RECEITA"
                                if pd.notna(x) and x > 0
                                else (
                                    "DESPESA"
                                    if pd.notna(x)
                                    else ""
                                )
                            )

                        # =============================
                        # METRICS
                        # =============================
                        st.markdown("---")
                        st.markdown("## 📊 Métricas")

                        c1, c2, c3 = st.columns(3)

                        with c1:

                            st.metric(
                                "Linhas",
                                len(sample_df)
                            )

                        with c2:

                            st.metric(
                                "Colunas",
                                len(sample_df.columns)
                            )

                        with c3:

                            detected_fields = len([
                                x for x in data.get(
                                    "auto_mapping",
                                    {}
                                ).values()
                                if x
                            ])

                            st.metric(
                                "Campos Detectados",
                                detected_fields
                            )

                        # =============================
                        # RECEITAS / DESPESAS
                        # =============================
                        if "valor" in sample_df.columns:

                            total_receitas = sample_df[
                                sample_df["valor"] > 0
                            ]["valor"].sum()

                            total_despesas = sample_df[
                                sample_df["valor"] < 0
                            ]["valor"].sum()

                            r1, r2 = st.columns(2)

                            with r1:

                                st.metric(
                                    "💵 Receitas",
                                    f"R$ {total_receitas:,.2f}"
                                )

                            with r2:

                                st.metric(
                                    "💸 Despesas",
                                    f"R$ {abs(total_despesas):,.2f}"
                                )

                        # =============================
                        # TABLE
                        # =============================
                        st.markdown("---")
                        st.markdown(
                            "### 📄 Amostra Processada"
                        )

                        st.dataframe(
                            sample_df.head(50),
                            use_container_width=True,
                            height=500
                        )

                    else:

                        st.warning(
                            "Nenhum dado encontrado."
                        )

                else:

                    message = data.get(
                        "message",
                        "Erro desconhecido"
                    )

                    st.error(message)

            except requests.exceptions.Timeout:

                st.error(
                    "Timeout da API. O processamento demorou demais."
                )

            except Exception as e:

                st.error(str(e))

# =========================
# PROCESS
# =========================
st.markdown("---")
st.markdown("## ⚙️ Processamento")

if st.button("🚀 Processar Arquivos"):

    if not input_file or not fornecedores_file:

        st.warning(
            "Envie os dois arquivos."
        )

    else:

        with st.spinner(
            "Processando ETL financeiro..."
        ):

            files = {

                "input_file": (
                    input_file.name,
                    input_file,
                    input_file.type
                ),

                "fornecedores_file": (
                    fornecedores_file.name,
                    fornecedores_file,
                    fornecedores_file.type
                )
            }

            try:

                # =================================
                # REQUEST
                # =================================
                res = requests.post(
                    f"{API_URL}/process",
                    files=files,
                    timeout=300
                )

                # =================================
                # STATUS CHECK
                # =================================
                if res.status_code != 200:

                    st.error(
                        f"Erro da API: {res.status_code}"
                    )

                    st.stop()

                # =================================
                # JSON PARSE
                # =================================
                try:

                    data = res.json()

                except Exception:

                    st.error(
                        "Resposta inválida da API."
                    )

                    st.stop()

                # =================================
                # SUCCESS
                # =================================
                if data.get("status") == "success":

                    st.success(
                        "ETL processado com sucesso"
                    )

                    # =============================
                    # FINAL MAPPING
                    # =============================
                    if "detection" in data:

                        st.markdown(
                            "### 🧠 Mapeamento Final"
                        )

                        st.json(
                            data["detection"]
                        )

                    # =============================
                    # METRICS
                    # =============================
                    st.markdown("---")
                    st.markdown("## 📊 Resumo")

                    summary = data.get(
                        "summary",
                        {}
                    )

                    c1, c2, c3 = st.columns(3)

                    with c1:

                        st.metric(
                            "Total",
                            summary.get(
                                "total",
                                0
                            )
                        )

                    with c2:

                        st.metric(
                            "Válidos",
                            summary.get(
                                "valid",
                                0
                            )
                        )

                    with c3:

                        st.metric(
                            "Rejeitados",
                            summary.get(
                                "rejected",
                                0
                            )
                        )

                    # =============================
                    # DOWNLOADS
                    # =============================
                    st.markdown("---")
                    st.markdown("## ⬇️ Downloads")

                    importacao_url = (
                        API_URL
                        + data["files"]["importacao"]
                    )

                    rejeitados_url = (
                        API_URL
                        + data["files"]["rejeitados"]
                    )

                    d1, d2 = st.columns(2)

                    # =============================
                    # VALID FILE
                    # =============================
                    with d1:

                        file_res = requests.get(
                            importacao_url,
                            timeout=180
                        )

                        st.download_button(
                            label="📥 Download Importação",
                            data=BytesIO(
                                file_res.content
                            ),
                            file_name="importacao.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                    # =============================
                    # REJECTED FILE
                    # =============================
                    with d2:

                        rej_res = requests.get(
                            rejeitados_url,
                            timeout=180
                        )

                        st.download_button(
                            label="📥 Download Rejeitados",
                            data=BytesIO(
                                rej_res.content
                            ),
                            file_name="rejeitados.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                else:

                    message = data.get(
                        "message",
                        "Erro desconhecido"
                    )

                    st.error(message)

            except requests.exceptions.Timeout:

                st.error(
                    "Timeout da API durante o processamento."
                )

            except Exception as e:

                st.error(str(e))

# =========================
# FOOTER
# =========================
st.markdown("---")

st.caption(
    "ETL Finance • Intelligent Financial Processing"
)