import streamlit as st
import requests
import pandas as pd

# =========================
# CONFIG
# =========================
API_URL = "http://127.0.0.1:8000"

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

</style>
""", unsafe_allow_html=True)

# =========================
# HEADER
# =========================
st.title("💰 ETL Finance Platform")
st.caption("Upload financeiro inteligente com auto-detecção e validação")

# =========================
# UPLOAD SECTION
# =========================
st.markdown("## 📤 Upload de Arquivos")

col1, col2 = st.columns(2)

with col1:
    input_file = st.file_uploader(
        "Arquivo Financeiro",
        type=["csv", "xlsx"]
    )

with col2:
    fornecedores_file = st.file_uploader(
        "Tabela de Fornecedores",
        type=["xlsx", "csv"]
    )

# =========================
# PREVIEW
# =========================
if input_file:

    st.markdown("---")
    st.markdown("## 👀 Preview Inteligente")

    if st.button("Gerar Preview"):

        with st.spinner("Analisando arquivo..."):

            files = {
                "input_file": (
                    input_file.name,
                    input_file,
                    input_file.type
                )
            }

            try:
                res = requests.post(
                    f"{API_URL}/preview",
                    files=files
                )

                data = res.json()

                if data["status"] == "success":

                    st.success("Preview gerado com sucesso")

                    # Columns
                    st.markdown("### 🧠 Colunas Detectadas")

                    st.json(data["auto_mapping"])

                    # Sample
                    st.markdown("### 📄 Amostra dos Dados")

                    sample_df = pd.DataFrame(data["sample_data"])

                    st.dataframe(
                        sample_df,
                        use_container_width=True
                    )

                else:
                    st.error(data)

            except Exception as e:
                st.error(str(e))

# =========================
# PROCESS
# =========================
st.markdown("---")
st.markdown("## ⚙️ Processamento")

if st.button("🚀 Processar Arquivos"):

    if not input_file or not fornecedores_file:
        st.warning("Envie os dois arquivos.")
    else:

        with st.spinner("Processando ETL..."):

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

                res = requests.post(
                    f"{API_URL}/process",
                    files=files
                )

                data = res.json()

                if data["status"] == "success":

                    st.success("ETL processado com sucesso")

                    # =========================
                    # METRICS
                    # =========================
                    st.markdown("## 📊 Resumo")

                    c1, c2, c3 = st.columns(3)

                    with c1:
                        st.metric(
                            "Total",
                            data["summary"]["total"]
                        )

                    with c2:
                        st.metric(
                            "Válidos",
                            data["summary"]["valid"]
                        )

                    with c3:
                        st.metric(
                            "Rejeitados",
                            data["summary"]["rejected"]
                        )

                    # =========================
                    # DOWNLOADS
                    # =========================
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

                    # VALID FILE
                    with d1:

                        file_res = requests.get(importacao_url)

                        st.download_button(
                            label="📥 Download Importação",
                            data=file_res.content,
                            file_name="importacao.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                    # REJECTED FILE
                    with d2:

                        rej_res = requests.get(rejeitados_url)

                        st.download_button(
                            label="📥 Download Rejeitados",
                            data=rej_res.content,
                            file_name="rejeitados.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                else:
                    st.error(data)

            except Exception as e:
                st.error(str(e))

# =========================
# FOOTER
# =========================
st.markdown("---")
st.caption("ETL Finance • Intelligent Financial Processing")