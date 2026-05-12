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

    value = str(value).strip()

    if value == "":
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

    if value in ["", ".", "-", "-."]:
        return None

    try:
        return float(value)
    except:
        return None


# =====================================================
# CLEAN DATE
# =====================================================
def clean_date(value):

    if pd.isna(value):
        return pd.NaT

    # =============================================
    # ALREADY DATETIME
    # =============================================
    if isinstance(value, pd.Timestamp):

        if 2000 <= value.year <= 2100:
            return value

        return pd.NaT

    # =============================================
    # STRING CLEAN
    # =============================================
    value_str = str(value).strip()

    if value_str == "":
        return pd.NaT

    # =============================================
    # REMOVE TIME PART
    # =============================================
    value_str = value_str.split(" ")[0]

    # =============================================
    # REMOVE .0 FROM EXCEL FLOATS
    # EX:
    # 45628.0
    # =============================================
    if value_str.endswith(".0"):
        value_str = value_str[:-2]

    # =============================================
    # EXCEL SERIAL AS STRING
    # =============================================
    if re.match(r"^\d+$", value_str):

        try:

            serial = int(value_str)

            # =====================================
            # VALID EXCEL DATE RANGE
            # =====================================
            if 25000 <= serial <= 60000:

                parsed = (
                    pd.to_datetime("1899-12-30") +
                    pd.to_timedelta(serial, unit="D")
                )

                if 2000 <= parsed.year <= 2100:
                    return parsed

        except:
            pass

    # =============================================
    # FLOAT / INTEGER
    # =============================================
    if isinstance(value, (int, float)):

        try:

            numeric = float(value)

            if 25000 <= numeric <= 60000:

                parsed = (
                    pd.to_datetime("1899-12-30") +
                    pd.to_timedelta(numeric, unit="D")
                )

                if 2000 <= parsed.year <= 2100:
                    return parsed

        except:
            pass

    # =============================================
    # VALID STRING PATTERNS
    # =============================================
    patterns = [
        r"^\d{2}/\d{2}/\d{4}$",
        r"^\d{2}-\d{2}-\d{4}$",
        r"^\d{4}-\d{2}-\d{2}$"
    ]

    valid = any(
        re.match(p, value_str)
        for p in patterns
    )

    if not valid:
        return pd.NaT

    # =============================================
    # PARSE STRING DATE
    # =============================================
    try:

        parsed = pd.to_datetime(
            value_str,
            dayfirst=True,
            errors="coerce"
        )

        if pd.isna(parsed):
            return pd.NaT

        # =========================================
        # BLOCK 1970 / INVALID YEARS
        # =========================================
        if parsed.year < 2000 or parsed.year > 2100:
            return pd.NaT

        return parsed

    except:

        return pd.NaT


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
                        "historico",
                        "saldo"
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

    normalized_columns = {}

    for col in df.columns:
        normalized_columns[col] = clean_header(col)

    mappings = {

        "data": [
            "data",
            "date",
            "data movimentacao",
            "data de movimentacao",
            "movimentacao"
        ],

        "fornecedor": [
            "fornecedor",
            "cliente",
            "vendor",
            "supplier",
            "empresa",
            "favorecido",
            "beneficiario",
            "razao social"
        ],

        "historico": [
            "historico",
            "descricao",
            "description",
            "memo",
            "observacao",
            "tipo",
            "movimento",
            "transacao"
        ],

        "titulo/documento": [
            "titulo documento",
            "documento",
            "numero",
            "nf"
        ]
    }

    detected = {}

    for standard_name, aliases in mappings.items():

        found = None

        for original_col, normalized_col in normalized_columns.items():

            for alias in aliases:

                alias = clean_header(alias)

                if (
                    normalized_col == alias
                    or alias in normalized_col
                ):

                    found = original_col
                    break

            if found:
                break

        detected[standard_name] = found

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

    print("\n========== DETECTION ==========")
    print(detection_report)

    # =================================================
    # RENAME
    # =================================================
    rename_map = {}

    for standard_col, detected_col in detection_report.items():

        if detected_col:
            rename_map[detected_col] = standard_col

    df.rename(columns=rename_map, inplace=True)

    # =================================================
    # CLEAN HEADERS
    # =================================================
    df.columns = [
        clean_header(c)
        for c in df.columns
    ]

    print("\n========== RENAMED ==========")
    print(df.columns.tolist())

    # =================================================
    # MONEY COLUMNS DETECTION
    # =================================================
    money_map = {
        "entrada": [],
        "saida": [],
        "saldo_anterior": [],
        "saldo_atual": [],
        "valor_direto": []
    }

    for col in df.columns:

        c = clean_header(col)

        # =============================================
        # ENTRADA
        # =============================================
        if any(x in c for x in [
            "entrada",
            "credito",
            "crédito",
            "recebimento",
            "receita"
        ]):
            money_map["entrada"].append(col)

        # =============================================
        # SAIDA
        # =============================================
        if any(x in c for x in [
            "saida",
            "saída",
            "debito",
            "débito",
            "pagamento",
            "despesa"
        ]):
            money_map["saida"].append(col)

        # =============================================
        # SALDO ANTERIOR
        # =============================================
        if any(x in c for x in [
            "saldo anterior",
            "saldo inicial"
        ]):
            money_map["saldo_anterior"].append(col)

        # =============================================
        # SALDO ATUAL
        # =============================================
        if any(x in c for x in [
            "saldo atual",
            "saldo final"
        ]):
            money_map["saldo_atual"].append(col)

        # =============================================
        # VALOR DIRETO
        # =============================================
        if c in [
            "valor",
            "amount",
            "value",
            "valor total"
        ]:
            money_map["valor_direto"].append(col)

    print("\n========== MONEY MAP ==========")
    print(money_map)

    # =================================================
    # GENERATE VALOR
    # =================================================
    df["valor"] = 0.0

    # =============================================
    # DIRECT VALUE
    # =============================================
    if money_map["valor_direto"]:

        col = money_map["valor_direto"][0]

        df["valor"] = (
            df[col]
            .apply(clean_money)
            .fillna(0)
        )

    else:

        # =========================================
        # SOMA ENTRADAS
        # =========================================
        for col in money_map["entrada"]:

            vals = (
                df[col]
                .apply(clean_money)
                .fillna(0)
            )

            df["valor"] += vals

        # =========================================
        # SUBTRAI SAIDAS
        # =========================================
        for col in money_map["saida"]:

            vals = (
                df[col]
                .apply(clean_money)
                .fillna(0)
            )

            df["valor"] -= vals

        # =========================================
        # BALANCE FALLBACK
        # =========================================
        if (
            df["valor"]
            .abs()
            .sum() == 0
            and money_map["saldo_anterior"]
            and money_map["saldo_atual"]
        ):

            saldo_ant = (
                df[
                    money_map["saldo_anterior"][0]
                ]
                .apply(clean_money)
                .fillna(0)
            )

            saldo_atual = (
                df[
                    money_map["saldo_atual"][0]
                ]
                .apply(clean_money)
                .fillna(0)
            )

            df["valor"] = (
                saldo_atual - saldo_ant
            )

    print("\n========== GENERATED VALOR ==========")
    print(df["valor"].head())

    # =================================================
    # OPTIONAL FIELDS
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
    df["valor"] = (
        df["valor"]
        .apply(clean_money)
    )

    df["data"] = (
        df["data"]
        .apply(clean_date)
    )

    df["fornecedor"] = (
        df["fornecedor"]
        .fillna("")
        .astype(str)
        .apply(normalize_text)
    )

    df["historico"] = (
        df["historico"]
        .fillna("")
        .astype(str)
    )

    df["titulo/documento"] = (
        df["titulo/documento"]
        .fillna("")
        .astype(str)
    )

    # =================================================
    # HISTORICO FIX
    # =================================================
    def build_historico(row):

        historico = str(
            row.get("historico", "")
        ).strip()

        fornecedor = str(
            row.get("fornecedor", "")
        ).strip()

        documento = str(
            row.get("titulo/documento", "")
        ).strip()

        valor = row.get("valor", 0)

        invalid_hist = [
            "",
            "nan",
            "none",
            "recebimento",
            "pagamento"
        ]

        parts = []

        # =========================================
        # PRIORITIZE REAL DESCRIPTION
        # =========================================
        if (
            historico.lower()
            not in invalid_hist
        ):
            parts.append(historico)

        if fornecedor:

            if fornecedor not in parts:
                parts.append(fornecedor)

        if documento:

            parts.append(f"DOC {documento}")

        if not parts:
            parts.append("LANCAMENTO FINANCEIRO")

        final_text = " | ".join(parts)

        if valor < 0:
            return f"PAGAMENTO - {final_text}"

        return f"RECEBIMENTO - {final_text}"

    df["historico_final"] = df.apply(
        build_historico,
        axis=1
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