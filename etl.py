import pandas as pd
import unicodedata
from difflib import get_close_matches
import csv

# =========================
# COLUMN SYNONYMS
# =========================
COLUMN_MAP = {
    "data": ["data", "date", "dt", "transaction", "data lançamento"],
    "valor": ["valor", "value", "amount", "vlr", "total", "price"],
    "fornecedor": ["fornecedor", "supplier", "vendor", "cliente", "nome", "favorecido"],
    "historico": ["historico", "history", "description", "desc", "memo"],
    "titulo/documento": ["documento", "doc", "titulo", "invoice", "numero"]
}

# =========================
# HELPERS
# =========================
def normalize_text(text):
    if pd.isna(text):
        return None
    text = str(text).strip().upper()
    return unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode()

def smart_read_csv(path):
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        sample = f.read(2048)
        delimiter = csv.Sniffer().sniff(sample).delimiter

    try:
        df = pd.read_csv(path, delimiter=delimiter, encoding='utf-8')
    except:
        df = pd.read_csv(path, delimiter=delimiter, encoding='latin1')

    df.columns = [str(c).strip() for c in df.columns]
    return df

def load_file(path):
    if path.lower().endswith(".csv"):
        return smart_read_csv(path)
    else:
        return pd.read_excel(path)

def find_column(df_cols, options):
    df_cols_norm = [c.lower().strip() for c in df_cols]

    # exact match first
    for opt in options:
        opt = opt.lower()
        for i, col in enumerate(df_cols_norm):
            if opt == col:
                return df_cols[i]

    # partial match fallback
    for opt in options:
        for i, col in enumerate(df_cols_norm):
            if opt in col:
                return df_cols[i]

    return None

# =========================
# MAIN ETL
# =========================
# =========================
# AUTO DETECTION REPORT
# =========================
def detect_columns(df):
    detection = {}

    for target, options in COLUMN_MAP.items():
        found = None
        match_type = None

        for col in df.columns:
            col_norm = col.lower().strip()

            # exact match
            if col_norm in [o.lower() for o in options]:
                found = col
                match_type = "exact"
                break

        if not found:
            for col in df.columns:
                col_norm = col.lower().strip()
                for opt in options:
                    if opt.lower() in col_norm:
                        found = col
                        match_type = "partial"
                        break
                if found:
                    break

        detection[target] = {
            "mapped_to": found,
            "match_type": match_type if found else "not_found"
        }

    return detection

def run_etl(input_path, forn_path, output_prefix="output"):

    # =========================
    # LOAD RAW DATA
    # =========================
    df = load_file(input_path)

    # Remove empty rows
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    # Handle header issues
    if str(df.columns[0]).startswith("Unnamed"):
        df.columns = df.iloc[0]
        df = df[1:]
    detection_report = detect_columns(df)

    # =========================
    # AUTO MAP COLUMNS
    # =========================
    mapped = {}

    for target, options in COLUMN_MAP.items():
        col = find_column(df.columns, options)
        if col:
            mapped[target] = col

    df = df.rename(columns={v: k for k, v in mapped.items()})

    # =========================
    # ENSURE REQUIRED STRUCTURE
    # =========================
    for col in ["data", "valor"]:
        if col not in df.columns:
            raise Exception(f"Missing critical column: {col}")

    for col in ["fornecedor", "historico", "titulo/documento"]:
        if col not in df.columns:
            df[col] = None

    # =========================
    # EARLY NORMALIZATION
    # =========================
    def fix_valor(v):
        if isinstance(v, str):
            v = v.replace("R$", "").replace(".", "").replace(",", ".")
        return pd.to_numeric(v, errors="coerce")

    df["valor"] = df["valor"].apply(fix_valor)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    df["fornecedor"] = df["fornecedor"].apply(normalize_text)
    df["historico"] = df["historico"].apply(normalize_text)

    # =========================
    # LOAD FORNECEDORES
    # =========================
    forn = pd.read_excel(forn_path)
    forn.columns = [str(c).lower().strip() for c in forn.columns]

    cod_col = find_column(forn.columns, ["codigo", "code", "id"])
    name_col = find_column(forn.columns, ["nome", "name", "fornecedor"])

    if not cod_col or not name_col:
        raise Exception("Fornecedores must have codigo + nome")

    forn = forn[[cod_col, name_col]]
    forn.columns = ["codigo", "nome"]
    forn["nome"] = forn["nome"].apply(normalize_text)

    forn_names = forn["nome"].dropna().unique().tolist()

    # =========================
    # FUZZY MATCH FORNECEDOR
    # =========================
    def match_fornecedor(name):
        if not name:
            return None
        if name in forn_names:
            return name
        match = get_close_matches(name, forn_names, n=1, cutoff=0.85)
        return match[0] if match else name

    df["fornecedor"] = df["fornecedor"].apply(match_fornecedor)

    # =========================
    # CLEAN DATA
    # =========================
    df = df[df["valor"].notna()]
    df = df[df["valor"] != 0]

    # =========================
    # SPLIT
    # =========================
    receita = df[df["valor"] > 0].copy()
    despesa = df[df["valor"] < 0].copy()

    # =========================
    # MERGE FORNECEDORES
    # =========================
    despesa = despesa.merge(
        forn,
        left_on="fornecedor",
        right_on="nome",
        how="left"
    )

    # =========================
    # ACCOUNTING LOGIC
    # =========================
    despesa["debito"] = despesa["codigo"]
    despesa["credito"] = 10001

    receita["debito"] = 10001
    receita["credito"] = 20001

    # =========================
    # DOCUMENT EXTRACTION
    # =========================
    if "titulo/documento" in df.columns:
        despesa["doc_num"] = despesa["titulo/documento"].astype(str).str.split(",").str[0]
    else:
        despesa["doc_num"] = None

    # =========================
    # HISTÓRICO
    # =========================
    despesa["hist_padrao"] = despesa["doc_num"].apply(
        lambda x: 7 if pd.notna(x) and x != "nan" else 1
    )

    despesa["historico_final"] = (
        "PAGO DUPLICATA "
        + despesa["doc_num"].fillna("")
        + " "
        + despesa["fornecedor"].fillna("")
    )

    receita["hist_padrao"] = 1
    receita["historico_final"] = "RECEBIMENTO"

    # =========================
    # FINAL DATASET
    # =========================
    cols = ["data", "debito", "credito", "valor", "hist_padrao", "historico_final"]

    importacao = pd.concat([
        receita[cols],
        despesa[cols]
    ])

    # =========================
    # VALIDATION
    # =========================
    def validate(row):
        errors = []

        if pd.isna(row["data"]):
            errors.append("DATA_INVALIDA")

        if pd.isna(row["valor"]):
            errors.append("VALOR_INVALIDO")

        if pd.isna(row["debito"]):
            errors.append("DEBITO_INVALIDO")

        return "; ".join(errors) if errors else None

    importacao["erro"] = importacao.apply(validate, axis=1)

    rejeitados = importacao[importacao["erro"].notna()]
    validados = importacao[importacao["erro"].isna()].drop(columns=["erro"])

    # =========================
    # EXPORT
    # =========================
    valid_path = f"{output_prefix}_importacao.xlsx"
    rej_path = f"{output_prefix}_rejeitados.xlsx"

    validados.to_excel(valid_path, index=False)
    rejeitados.to_excel(rej_path, index=False)

    return {
        "valid": valid_path,
        "rejected": rej_path,
        "total": len(importacao),
        "valid_count": len(validados),
        "rejected_count": len(rejeitados)
    }
