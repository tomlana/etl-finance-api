import pandas as pd
import unicodedata
from difflib import get_close_matches
from datetime import datetime

# =========================
# CONFIG
# =========================
INPUT_FILE = "input.xlsx"
FORN_FILE = "fornecedores.xlsx"
OUTPUT_FILE = "importacao.xlsx"

COLUMN_MAP = {
    "data": ["data", "date", "dt"],
    "valor": ["valor", "value", "amount"],
    "fornecedor": ["fornecedor", "supplier", "vendor", "cliente"],
    "historico": ["historico", "description"],
    "titulo/documento": ["documento", "doc", "titulo"]
}

# =========================
# HELPERS
# =========================
def normalize_text(text):
    if pd.isna(text):
        return None
    text = str(text).strip().upper()
    return unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode()

def find_column(df_cols, options):
    for col in df_cols:
        for opt in options:
            if col.lower() == opt.lower():
                return col
    return None

# =========================
# LOAD DATA
# =========================
df = pd.read_excel(INPUT_FILE)
df.columns = [c.strip() for c in df.columns]

# =========================
# AUTO MAP
# =========================
mapped = {}
for target, opts in COLUMN_MAP.items():
    col = find_column(df.columns, opts)
    if col:
        mapped[target] = col

df = df.rename(columns={v: k for k, v in mapped.items()})

# =========================
# AUTO CORRECTION (EARLY!)
# =========================
def fix_valor(v):
    if isinstance(v, str):
        v = v.replace("R$", "").replace(".", "").replace(",", ".")
    return pd.to_numeric(v, errors="coerce")

df["valor"] = df["valor"].apply(fix_valor)
df["data"] = pd.to_datetime(df["data"], errors="coerce")

df["fornecedor"] = df.get("fornecedor", "").apply(normalize_text)
df["historico"] = df.get("historico", "").apply(normalize_text)

# =========================
# LOAD FORNECEDORES
# =========================
forn = pd.read_excel(FORN_FILE)
forn.columns = [c.lower() for c in forn.columns]

forn = forn.rename(columns={
    find_column(forn.columns, ["codigo", "code"]): "codigo",
    find_column(forn.columns, ["nome", "name"]): "nome"
})

forn["nome"] = forn["nome"].apply(normalize_text)

# =========================
# FUZZY MATCH BEFORE MERGE
# =========================
forn_names = forn["nome"].dropna().unique().tolist()

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
# MERGE
# =========================
despesa = despesa.merge(
    forn,
    left_on="fornecedor",
    right_on="nome",
    how="left"
)

# =========================
# ACCOUNTING FIELDS
# =========================
despesa["debito"] = despesa["codigo"]
despesa["credito"] = 10001

# =========================
# DOC EXTRACTION
# =========================
if "titulo/documento" in df.columns:
    despesa["doc_num"] = despesa["titulo/documento"].astype(str).str.split(",").str[0]
else:
    despesa["doc_num"] = None

# =========================
# HISTÓRICO
# =========================
despesa["hist_padrao"] = despesa["doc_num"].apply(lambda x: 7 if pd.notna(x) else 1)

despesa["historico_final"] = (
    "PAGO DUPLICATA "
    + despesa["doc_num"].fillna("")
    + " "
    + despesa["fornecedor"].fillna("")
)

# =========================
# RECEITA
# =========================
receita["debito"] = 10001
receita["credito"] = 20001
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
    if pd.isna(row["data"]): errors.append("DATA_INVALIDA")
    if pd.isna(row["valor"]): errors.append("VALOR_INVALIDO")
    if pd.isna(row["debito"]): errors.append("DEBITO_INVALIDO")
    return "; ".join(errors) if errors else None

importacao["erro"] = importacao.apply(validate, axis=1)

rejeitados = importacao[importacao["erro"].notna()]
validados = importacao[importacao["erro"].isna()].drop(columns=["erro"])

# =========================
# EXPORT
# =========================
validados.to_excel("importacao.xlsx", index=False)
rejeitados.to_excel("rejeitados.xlsx", index=False)

# =========================
# REPORT
# =========================
with open("relatorio.txt", "w", encoding="utf-8") as f:
    f.write(f"Total: {len(importacao)}\n")
    f.write(f"Validos: {len(validados)}\n")
    f.write(f"Rejeitados: {len(rejeitados)}\n")

print("✅ Pipeline completed")
