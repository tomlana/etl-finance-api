import pandas as pd
import csv
import unicodedata
import re
from difflib import get_close_matches


# =====================================================
# NORMALIZE TEXT
# =====================================================
def normalize_text(text):

    if pd.isna(text):
        return ""

    return (
        unicodedata
        .normalize("NFKD", str(text))
        .encode("ASCII", "ignore")
        .decode()
        .upper()
        .strip()
    )


# =====================================================
# CLEAN HEADER
# =====================================================
def clean_header(text):

    if text is None:
        return ""

    text = normalize_text(text).lower()

    replacements = [
        "_",
        "-",
        "/",
        "\\",
        ".",
        ":",
        ";",
        "(",
        ")",
        "[",
        "]"
    ]

    for r in replacements:
        text = text.replace(r, " ")

    return " ".join(text.split())


# =====================================================
# CLEAN MONEY
# =====================================================
def clean_money(value):

    if pd.isna(value):
        return None

    value = str(value)

    if value.strip() == "":
        return None

    value = (
        value
        .replace("R$", "")
        .replace("$", "")
        .replace('"', "")
        .replace(" ", "")
        .replace("\xa0", "")
        .strip()
    )

    # =============================================
    # BRAZILIAN FORMAT
    # 1.500,00
    # =============================================
    if "," in value and "." in value:

        if value.rfind(",") > value.rfind("."):

            value = value.replace(".", "")
            value = value.replace(",", ".")

    # =============================================
    # ONLY COMMA
    # 1500,00
    # =============================================
    elif "," in value:

        value = value.replace(",", ".")

    value = re.sub(r"[^0-9\.-]", "", value)

    try:
        return float(value)
    except:
        return None


# =====================================================
# LOAD FILE
# =====================================================
def load_file(path):

    # =================================================
    # EXCEL
    # =================================================
    if path.lower().endswith((".xlsx", ".xls")):

        excel = pd.ExcelFile(path)

        best_sheet = None
        best_header = 0
        best_score = -1

        for sheet in excel.sheet_names:

            try:

                temp = pd.read_excel(
                    path,
                    sheet_name=sheet,
                    header=None
                )

                for idx in range(min(15, len(temp))):

                    row = (
                        temp.iloc[idx]
                        .astype(str)
                        .apply(clean_header)
                    )

                    row_text = " ".join(row.tolist())

                    keywords = [
                        "data",
                        "movimentacao",
                        "entrada",
                        "saida",
                        "credito",
                        "debito",
                        "cliente",
                        "documento",
                        "historico"
                    ]

                    hits = sum(
                        1 for k in keywords
                        if k in row_text
                    )

                    if hits > best_score:

                        best_score = hits
                        best_sheet = sheet
                        best_header = idx

            except Exception as e:
                print(f"Sheet error: {e}")

        if best_sheet is None:
            raise Exception("Could not detect valid Excel sheet")

        df = pd.read_excel(
            path,
            sheet_name=best_sheet,
            header=best_header
        )

    # =================================================
    # CSV
    # =================================================
    else:

        encodings = [
            "utf-8-sig",
            "utf-8",
            "latin1",
            "cp1252"
        ]

        df = None

        for enc in encodings:

            try:

                with open(
                    path,
                    "r",
                    encoding=enc,
                    errors="ignore"
                ) as f:

                    sample = f.read(5000)

                    try:
                        delimiter = csv.Sniffer().sniff(sample).delimiter
                    except:
                        delimiter = ";"

                df = pd.read_csv(
                    path,
                    encoding=enc,
                    delimiter=delimiter,
                    engine="python"
                )

                break

            except Exception as e:

                print(f"Encoding {enc} failed: {e}")

        if df is None:
            raise Exception("Unable to read CSV")

        # =============================================
        # BROKEN CSV FIX
        # =============================================
        if len(df.columns) == 1:

            first_col = df.columns[0]

            exploded = (
                df[first_col]
                .astype(str)
                .str.split(",", expand=True)
            )

            header_parts = (
                str(first_col)
                .replace('"', "")
                .split(",")
            )

            if len(header_parts) == exploded.shape[1]:

                exploded.columns = [
                    clean_header(h)
                    for h in header_parts
                ]

            df = exploded

    # =================================================
    # CLEAN COLUMNS
    # =================================================
    df.columns = [
        clean_header(c)
        for c in df.columns
    ]

    # =================================================
    # REMOVE EMPTY
    # =================================================
    df = df.dropna(how="all")

    # =================================================
    # REMOVE UNNAMED
    # =================================================
    df = df.loc[
        :,
        ~df.columns.str.contains("^unnamed")
    ]

    return df


# =====================================================
# DETECT COLUMNS
# =====================================================
def detect_columns(df):

    if df is None or df.empty:
        return {}

    original_columns = list(df.columns)

    normalized_columns = {}

    for col in original_columns:

        clean_col = clean_header(col)

        normalized_columns[col] = clean_col

    print("\n========== NORMALIZED COLUMNS ==========")

    for k, v in normalized_columns.items():
        print(f"{k} -> {v}")

    mappings = {

        "data": [
            "data",
            "date",
            "data movimentacao",
            "data de movimentacao",
            "movimentacao"
        ],

        "valor": [
            "valor",
            "amount",
            "value",
            "price",
            "entrada",
            "saida",
            "credito",
            "debito",
            "saldo anterior",
            "saída",
            "entrada",
            "saldo atual"
        ],

        "fornecedor": [
            "fornecedor",
            "cliente",
            "vendor",
            "supplier",
            "empresa",
            "favorecido"
        ],

        "historico": [
            "historico",
            "descricao",
            "description",
            "memo",
            "observacao"
        ],

        "titulo/documento": [
            "titulo documento",
            "título documento",
            "documento",
            "document",
            "numero",
            "nf"
        ]
    }

    detected = {}

    for standard_name, aliases in mappings.items():

        found = None

        # =============================================
        # DIRECT MATCH
        # =============================================
        for original_col, normalized_col in normalized_columns.items():

            for alias in aliases:

                alias = clean_header(alias)

                if normalized_col == alias:

                    found = original_col
                    break

            if found:
                break

        # =============================================
        # PARTIAL MATCH
        # =============================================
        if not found:

            for original_col, normalized_col in normalized_columns.items():

                for alias in aliases:

                    alias = clean_header(alias)

                    if (
                        alias in normalized_col
                        or normalized_col in alias
                    ):

                        found = original_col
                        break

                if found:
                    break

        detected[standard_name] = found

    # =================================================
    # AUTO DETECT MONEY
    # =================================================
    if not detected["valor"]:

        for original_col, normalized_col in normalized_columns.items():

            if normalized_col in [
                "entrada",
                "saida",
                "credito",
                "debito"
            ]:

                detected["valor"] = original_col
                break

    print("\n========== DETECTED ==========")

    for k, v in detected.items():
        print(f"{k} -> {v}")

    return detected


# =====================================================
# BUILD OUTPUT
# =====================================================
def build_output(df):

    if df.empty:

        return pd.DataFrame(columns=[
            "Data",
            "Débito",
            "Crédito",
            "Valor",
            "Histórico",
            "Documento"
        ])

    output = pd.DataFrame()

    output["Data"] = (
        df["data"]
        .dt.strftime("%d/%m/%Y")
    )

    output["Débito"] = df["debito"]

    output["Crédito"] = df["credito"]

    output["Valor"] = (
        df["valor"]
        .abs()
        .round(2)
    )

    output["Histórico"] = df["historico_final"]

    output["Documento"] = df["titulo/documento"]

    return output


# =====================================================
# RUN ETL
# =====================================================
def run_etl(input_path, forn_path, output_prefix="output"):

    # =================================================
    # LOAD INPUT
    # =================================================
    df = load_file(input_path)

    if df is None or df.empty:
        raise Exception("Input file is empty")

    print("\n========== RAW DATA ==========")
    print(df.head())
    print(df.columns.tolist())

    # =================================================
    # DETECT COLUMNS
    # =================================================
    detection_report = detect_columns(df)

    # =================================================
    # RENAME
    # =================================================
    rename_map = {}

    for standard_col, detected_col in detection_report.items():

        if detected_col:
            rename_map[detected_col] = standard_col

    df.rename(columns=rename_map, inplace=True)

    # =================================================
    # FORCE CLEAN HEADERS
    # =================================================
    df.columns = [
        clean_header(c)
        for c in df.columns
    ]

    print("\n========== RENAMED ==========")
    print(df.columns.tolist())

    # =================================================
    # GENERATE VALOR
    # =================================================
    if "valor" not in df.columns:

        entrada_col = None
        saida_col = None

        for col in df.columns:

            c = clean_header(col)

            if c in ["entrada", "credito"]:
                entrada_col = col

            if c in ["saida", "debito"]:
                saida_col = col

        print("\n========== MONEY COLUMNS ==========")
        print(f"ENTRADA: {entrada_col}")
        print(f"SAIDA: {saida_col}")

        entrada_vals = (
            df[entrada_col].apply(clean_money)
            if entrada_col
            else pd.Series([0] * len(df))
        )

        saida_vals = (
            df[saida_col].apply(clean_money)
            if saida_col
            else pd.Series([0] * len(df))
        )

        entrada_vals = entrada_vals.fillna(0)
        saida_vals = saida_vals.fillna(0)

        df["valor"] = entrada_vals - saida_vals

    # =================================================
    # REQUIRED
    # =================================================
    required = ["data", "valor"]

    missing = []

    for col in required:

        if col not in df.columns:
            missing.append(col)

    if missing:

        print("\n========== AVAILABLE COLUMNS ==========")
        print(df.columns.tolist())

        raise Exception(
            f"Missing critical column: {', '.join(missing)}"
        )

    # =================================================
    # OPTIONAL
    # =================================================
    optional = [
        "fornecedor",
        "historico",
        "titulo/documento"
    ]

    for col in optional:

        if col not in df.columns:
            df[col] = ""

    # =================================================
    # CLEAN DATA
    # =================================================
    df["valor"] = df["valor"].apply(clean_money)

    df["data"] = pd.to_datetime(
        df["data"],
        errors="coerce",
        dayfirst=True
    )

    df["fornecedor"] = (
        df["fornecedor"]
        .fillna("")
        .apply(normalize_text)
    )

    df["historico"] = (
        df["historico"]
        .fillna("")
        .apply(normalize_text)
    )

    df["titulo/documento"] = (
        df["titulo/documento"]
        .fillna("")
        .astype(str)
    )

    print("\n========== CLEAN ==========")
    print(df.head())

    # =================================================
    # REJECT INVALID
    # =================================================
    rejeitados = df[
        (
            df["data"].isna()
        ) |
        (
            df["valor"].isna()
        )
    ].copy()

    rejeitados["erro"] = "DATA_OU_VALOR_INVALIDO"

    # =================================================
    # KEEP VALID
    # =================================================
    df = df[
        (
            df["data"].notna()
        ) &
        (
            df["valor"].notna()
        )
    ].copy()

    # =================================================
    # REMOVE ZERO
    # =================================================
    zeros = df[
        df["valor"] == 0
    ].copy()

    if not zeros.empty:

        zeros["erro"] = "VALOR_ZERO"

        rejeitados = pd.concat(
            [rejeitados, zeros],
            ignore_index=True
        )

    df = df[
        df["valor"] != 0
    ]

    # =================================================
    # LOAD FORNECEDORES
    # =================================================
    forn = pd.read_excel(forn_path)

    forn.columns = [
        "codigo",
        "nome"
    ]

    forn["nome"] = (
        forn["nome"]
        .astype(str)
        .apply(normalize_text)
    )

    forn_names = forn["nome"].tolist()

    # =================================================
    # MATCH FORNECEDOR
    # =================================================
    def match(name):

        if not name:
            return ""

        if name in forn_names:
            return name

        m = get_close_matches(
            name,
            forn_names,
            n=1,
            cutoff=0.75
        )

        return m[0] if m else name

    df["fornecedor"] = (
        df["fornecedor"]
        .apply(match)
    )

    # =================================================
    # SPLIT
    # =================================================
    receita = df[
        df["valor"] > 0
    ].copy()

    despesa = df[
        df["valor"] < 0
    ].copy()

    # =================================================
    # RECEITA
    # =================================================
    if not receita.empty:

        receita["debito"] = 10001
        receita["credito"] = 20001
        receita["historico_final"] = "RECEBIMENTO"

    # =================================================
    # DESPESA
    # =================================================
    if not despesa.empty:

        despesa = despesa.merge(
            forn,
            left_on="fornecedor",
            right_on="nome",
            how="left"
        )

        despesa["debito"] = (
            despesa["codigo"]
            .fillna(99999)
        )

        despesa["credito"] = 10001

        despesa["historico_final"] = "PAGAMENTO"

    # =================================================
    # FINAL
    # =================================================
    frames = []

    if not receita.empty:
        frames.append(receita)

    if not despesa.empty:
        frames.append(despesa)

    if frames:

        final_df = pd.concat(
            frames,
            ignore_index=True
        )

    else:

        final_df = pd.DataFrame(columns=df.columns)

    print("\n========== FINAL ==========")
    print(final_df.head())

    # =================================================
    # BUILD OUTPUT
    # =================================================
    importacao = build_output(final_df)

    # =================================================
    # EXPORT
    # =================================================
    valid_path = f"{output_prefix}_importacao.xlsx"

    rej_path = f"{output_prefix}_rejeitados.xlsx"

    with pd.ExcelWriter(
        valid_path,
        engine="openpyxl"
    ) as writer:

        importacao.to_excel(
            writer,
            sheet_name="Importação",
            index=False
        )

    with pd.ExcelWriter(
        rej_path,
        engine="openpyxl"
    ) as writer:

        rejeitados.to_excel(
            writer,
            sheet_name="Rejeitados",
            index=False
        )

    return {
        "valid": valid_path,
        "rejected": rej_path,
        "total": len(df),
        "valid_count": len(importacao),
        "rejected_count": len(rejeitados),
        "detection": detection_report
    }