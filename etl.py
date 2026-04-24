import pandas as pd
import unicodedata
from difflib import get_close_matches

def run_etl(input_path, forn_path, output_prefix="output"):
    
    def normalize_text(text):
        if pd.isna(text):
            return None
        text = str(text).strip().upper()
        return unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode()

    # LOAD
    df = pd.read_excel(input_path)
    df.columns = [c.strip() for c in df.columns]

    # BASIC FIXES
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df["fornecedor"] = df.get("fornecedor", "").apply(normalize_text)

    # LOAD FORNECEDORES
    forn = pd.read_excel(forn_path)
    forn.columns = ["codigo", "nome"]
    forn["nome"] = forn["nome"].apply(normalize_text)

    forn_names = forn["nome"].tolist()

    def match(name):
        if name in forn_names:
            return name
        m = get_close_matches(name, forn_names, 1, 0.85)
        return m[0] if m else name

    df["fornecedor"] = df["fornecedor"].apply(match)

    # CLEAN
    df = df[df["valor"].notna()]
    df = df[df["valor"] != 0]

    receita = df[df["valor"] > 0].copy()
    despesa = df[df["valor"] < 0].copy()

    despesa = despesa.merge(forn, left_on="fornecedor", right_on="nome", how="left")

    despesa["debito"] = despesa["codigo"]
    despesa["credito"] = 10001

    despesa["hist_padrao"] = 1
    despesa["historico_final"] = "PAGAMENTO"

    receita["debito"] = 10001
    receita["credito"] = 20001
    receita["hist_padrao"] = 1
    receita["historico_final"] = "RECEBIMENTO"

    cols = ["data","debito","credito","valor","hist_padrao","historico_final"]

    importacao = pd.concat([receita[cols], despesa[cols]])

    # VALIDATION
    def validate(row):
        errors = []
        if pd.isna(row["data"]): errors.append("DATA_INVALIDA")
        if pd.isna(row["debito"]): errors.append("DEBITO_INVALIDO")
        return "; ".join(errors) if errors else None

    importacao["erro"] = importacao.apply(validate, axis=1)

    rejeitados = importacao[importacao["erro"].notna()]
    validados = importacao[importacao["erro"].isna()].drop(columns=["erro"])

    # EXPORT
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
