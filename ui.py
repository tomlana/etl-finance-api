# =========================
# STREAMLIT FRONTEND (ADD ERROR TABLE PREVIEW)
# =========================

import streamlit as st
import requests
import time
import pandas as pd

API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="ETL", layout="wide", page_icon="⚡")

# =========================
# SIDEBAR AUTH
# =========================
st.sidebar.title("🔐ETL")

api_key = st.sidebar.text_input("API Key", type="password")

if not api_key:
    st.warning("Enter your API key to continue")
    st.stop()

headers = {"x-api-key": api_key}

# =========================
# HEADER
# =========================
st.title("⚡ Data Processor")
st.caption("Automated Financial Data Transformation Platform")

tab1, tab2 = st.tabs(["🔍 Preview", "⚙️ Process"])

# =========================
# PREVIEW
# =========================
with tab1:
    st.subheader("Preview Input File")

    input_file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="preview")

    if st.button("Run Preview"):
        if not input_file:
            st.warning("Please upload a file first")
        else:
            files = {
                "input_file": (input_file.name, input_file.getvalue())
            }

            with st.spinner("Analyzing file..."):
                res = requests.post(f"{API_URL}/preview", files=files, headers=headers)

            if res.status_code == 200:
                data = res.json()

                if data["status"] == "success":
                    st.success("Preview completed")

                    st.write("### Columns Detected")
                    st.write(data.get("columns_detected", []))

                    st.write("### Auto Mapping")
                    st.json(data.get("auto_mapping", {}))

                    st.write("### Sample Data")
                    st.dataframe(pd.DataFrame(data.get("sample_data", [])), use_container_width=True)

                else:
                    st.error(data.get("message", "Unknown error"))
            else:
                st.error(res.text)


# =========================
# PROCESS + ERROR TABLE
# =========================
with tab2:
    st.subheader("Process Data")

    col1, col2 = st.columns(2)

    with col1:
        input_file_proc = st.file_uploader("Input File", type=["csv", "xlsx"], key="process_input")

    with col2:
        forn_file = st.file_uploader("Fornecedores File", type=["xlsx"], key="forn")

    if st.button("Run Processing"):

        if not input_file_proc or not forn_file:
            st.warning("Upload both files before processing")

        else:
            files = {
                "input_file": (input_file_proc.name, input_file_proc.getvalue()),
                "fornecedores_file": (forn_file.name, forn_file.getvalue())
            }

            progress = st.progress(0)
            for i in range(100):
                time.sleep(0.005)
                progress.progress(i + 1)

            res = requests.post(f"{API_URL}/process", files=files, headers=headers)

            if res.status_code == 200:
                data = res.json()

                if data["status"] == "success":
                    st.success("Processing completed")

                    st.write("### Summary")
                    st.write(data["summary"])

                    import_url = API_URL + data["files"]["importacao"]
                    reject_url = API_URL + data["files"]["rejeitados"]

                    # =========================
                    # DOWNLOAD FILES
                    # =========================
                    import_file = requests.get(import_url, headers=headers)
                    reject_file = requests.get(reject_url, headers=headers)

                    st.download_button(
                        "⬇ Download Importação",
                        import_file.content,
                        file_name="importacao.xlsx"
                    )

                    st.download_button(
                        "⬇ Download Rejeitados",
                        reject_file.content,
                        file_name="rejeitados.xlsx"
                    )

                    # =========================
                    # ERROR TABLE PREVIEW
                    # =========================
                    st.write("### ❌ Rejected Rows Preview")

                    try:
                        df_rej = pd.read_excel(pd.io.common.BytesIO(reject_file.content))

                        if df_rej.empty:
                            st.success("No rejected rows 🎉")
                        else:
                            st.dataframe(df_rej, use_container_width=True)

                            # Error summary
                            if "erro" in df_rej.columns:
                                st.write("### 📊 Error Breakdown")
                                error_counts = df_rej["erro"].value_counts()
                                st.bar_chart(error_counts)

                    except Exception as e:
                        st.warning("Could not preview rejected file")

                else:
                    st.error(data.get("message", "Processing error"))

            else:
                st.error(res.text)
try:
    res = requests.post(f"{API_URL}/process", files=files, headers=headers)
except requests.exceptions.ConnectionError:
    st.error("🚨 Cannot connect to API. Is FastAPI running?")
    st.stop()