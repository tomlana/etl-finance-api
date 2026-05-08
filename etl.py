import pandas as pd
import csv
import unicodedata
from difflib import get_close_matches


# =====================================================
# LOAD FILE
# =====================================================
def load_file(path):

    # =========================
    # EXCEL
    # =========================
    if path.lower().endswith((".xlsx", ".xls")):

        df = pd.read_excel(path)

        df.columns = [
            str(c)
            .replace("ï»¿", "")
            .replace('"', "")
            .strip()
            for c in df.columns
        ]

        return df

    # =========================
    # CSV
    # =========================
    encodings = [
        "utf-8-sig",
        "utf-8",
        "latin1",
        "cp1252"
    ]

    for enc in encodings:

        try:

            with open(path, "r", encoding=enc, errors="ignore") as f:

                sample = f.read(5000)

                try:
                    delimiter = csv.Sniffer().sniff(sample).delimiter
                except:
                    delimiter = ","

            df = pd.read_csv(
                path,
                encoding=enc,
                delimiter=delimiter,
                engine="python"
            )

            # CLEAN COLUMNS
            df.columns = [
                str(c)
                .replace("ï»¿", "")
                .replace('"', "")
                .strip()
                for c in df.columns
            ]

            # REMOVE EMPTY ROWS
            df = df.dropna(how="all")

            # =========================
            # BROKEN CSV FIX
            # =========================
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
                        h.strip()
                        for h in header_parts
                    ]

                df = exploded

            return df

        except Exception as e:

            print(f"Encoding {enc} failed: {e}")

    raise Exception("Unable to read file")


# =====================================================
# DETECT COLUMNS
# =====================================================
def detect_columns(df):

    if df is None or df.empty:
        return {}

    columns = [
        str(c).lower().strip()
        for c in df.columns
    ]

    mappings = {

        "data": [
            "data",
            "date",
            "transaction date",
            "dt"
        ],

        "valor": [
            "valor",
            "amount",
            "value",
            "price"
        ],

        "fornecedor": [
            "fornecedor",
            "vendor",
            "supplier",
            "client",
            "client name",
            "nome"
        ],

        "historico": [
            "historico",
            "history",
            "description",
            "desc"
        ],

        "titulo/documento": [
            "titulo/documento",
            "documento",
            "document",
            "doc",
            "document number"
        ]
    }

    detected = {}

    for standard_name, aliases in mappings.items():

        found = None

        for col in columns:

            for alias in aliases:

                if alias in col:
                    found = col
                    break

            if found:
                break

        detected[standard_name] = found

    return detected


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
# RUN ETL
# =====================================================
def run_etl(input_path, forn_path, output_prefix="output"):

    # =========================
    # LOAD FILE
    # =========================
    df = load_file(input_path)
    
    # FORCE LOWERCASE COLUMNS
    df.columns = [
        str(c).strip().lower()
        for c in df.columns
]

    if df is None or df.empty:
        raise Exception("Input file is empty")

    print("\n========== RAW DATA ==========")
    print(df.head())
    print(df.columns.tolist())

    # =========================
    # DETECT COLUMNS
    # =========================
    detection_report = detect_columns(df)

    print("\n========== DETECTION ==========")
    print(detection_report)

    if not detection_report:
        raise Exception("Could not detect columns")

    # =========================
    # AUTO RENAME
    # =========================
    rename_map = {}

    for standard_col, detected_col in detection_report.items():

        if detected_col:
            rename_map[detected_col] = standard_col

    df.rename(columns=rename_map, inplace=True)

    print("\n========== RENAMED ==========")
    print(df.head())
    print(df.columns.tolist())

    # =========================
    # REQUIRED FIELDS
    # =========================
    required = ["data", "valor"]

    for col in required:

        if col not in df.columns:
            raise Exception(f"Missing critical column: {col}")

    # =========================
    # OPTIONAL FIELDS
    # =========================
    optional = [
        "fornecedor",
        "historico",
        "titulo/documento"
    ]

    for col in optional:

        if col not in df.columns:
            df[col] = ""

    # =========================
    # CLEAN VALUE
    # =========================
    df["valor"] = (
        df["valor"]
        .astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace('"', "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )

    df["valor"] = pd.to_numeric(
        df["valor"],
        errors="coerce"
    )

    # =========================
    # CLEAN DATE
    # =========================
    df["data"] = pd.to_datetime(
        df["data"],
        errors="coerce",
        dayfirst=True
    )

    # =========================
    # NORMALIZE TEXT
    # =========================
    df["fornecedor"] = df["fornecedor"].apply(normalize_text)

    df["historico"] = df["historico"].apply(normalize_text)

    print("\n========== AFTER CONVERSION ==========")
    print(df[["data", "valor", "fornecedor"]].head())

    # =========================
    # REJECT INVALID
    # =========================
    rejeitados = df[
        df["data"].isna() |
        df["valor"].isna()
    ].copy()

    rejeitados["erro"] = "DATA_OU_VALOR_INVALIDO"

    df = df[
        df["data"].notna() &
        df["valor"].notna()
    ].copy()

    # =========================
    # REMOVE ZERO VALUES
    # =========================
    zeros = df[df["valor"] == 0].copy()

    if not zeros.empty:

        zeros["erro"] = "VALOR_ZERO"

        rejeitados = pd.concat(
            [rejeitados, zeros],
            ignore_index=True
        )

    df = df[df["valor"] != 0]

    print("\n========== AFTER CLEAN ==========")
    print(df.head())

    # =========================
    # LOAD FORNECEDORES
    # =========================
    forn = pd.read_excel(forn_path)

    forn.columns = ["codigo", "nome"]

    forn["nome"] = forn["nome"].apply(normalize_text)

    forn_names = forn["nome"].tolist()

    # =========================
    # MATCH FORNECEDOR
    # =========================
    def match(name):

        if not name:
            return ""

        if name in forn_names:
            return name

        m = get_close_matches(
            name,
            forn_names,
            n=1,
            cutoff=0.80
        )

        return m[0] if m else name

    df["fornecedor"] = df["fornecedor"].apply(match)

    # =========================
    # SPLIT
    # =========================
    receita = df[df["valor"] > 0].copy()

    despesa = df[df["valor"] < 0].copy()

    print("\n========== SPLIT ==========")
    print("Receita:", len(receita))
    print("Despesa:", len(despesa))

    # =========================
    # RECEITA
    # =========================
    if not receita.empty:

        receita["debito"] = 10001
        receita["credito"] = 20001
        receita["hist_padrao"] = 1
        receita["historico_final"] = "RECEBIMENTO"

    # =========================
    # DESPESA
    # =========================
    if not despesa.empty:

        despesa = despesa.merge(
            forn,
            left_on="fornecedor",
            right_on="nome",
            how="left"
        )

        despesa["debito"] = despesa["codigo"]
        despesa["credito"] = 10001
        despesa["hist_padrao"] = 1
        despesa["historico_final"] = "PAGAMENTO"

    # =========================
    # FINAL DATASET
    # =========================
    cols = [
        "data",
        "debito",
        "credito",
        "valor",
        "hist_padrao",
        "historico_final"
    ]

    frames = []

    if not receita.empty:
        frames.append(receita[cols])

    if not despesa.empty:
        frames.append(despesa[cols])

    if len(frames) == 0:

        importacao = pd.DataFrame(columns=cols)

    else:

        importacao = pd.concat(
            frames,
            ignore_index=True
        )

    # =========================
    # FORMAT DATE
    # =========================
    if not importacao.empty:

        importacao["data"] = (
            importacao["data"]
            .dt.strftime("%d/%m/%Y")
        )

    print("\n========== FINAL ==========")
    print(importacao.head())

    # =========================
    # EXPORT
    # =========================
    valid_path = f"{output_prefix}_importacao.xlsx"

    rej_path = f"{output_prefix}_rejeitados.xlsx"

    importacao.to_excel(
        valid_path,
        index=False
    )

    rejeitados.to_excel(
        rej_path,
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