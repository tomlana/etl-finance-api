from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

import shutil
import os
import uuid

from etl import run_etl, load_file, detect_columns

app = FastAPI()

# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# TEMP FOLDER
# =========================
TEMP_DIR = "temp"
os.makedirs(TEMP_DIR, exist_ok=True)

# =========================
# ROOT
# =========================
@app.get("/")
def root():
    return {
        "status": "online",
        "service": "ETL Finance API"
    }

# =========================
# PREVIEW
# =========================
@app.post("/preview")
async def preview_file(input_file: UploadFile = File(...)):

    try:
        run_id = str(uuid.uuid4())

        input_path = os.path.join(
            TEMP_DIR,
            f"{run_id}_{input_file.filename}"
        )

        with open(input_path, "wb") as f:
            shutil.copyfileobj(input_file.file, f)

        df = load_file(input_path)

        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        detection = detect_columns(df)

        return {
            "status": "success",
            "run_id": run_id,
            "columns_detected": df.columns.tolist(),
            "sample_data": df.head(5).to_dict(orient="records"),
            "auto_mapping": detection
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# =========================
# PROCESS
# =========================
@app.post("/process")
async def process_files(
    input_file: UploadFile = File(...),
    fornecedores_file: UploadFile = File(...)
):

    try:
        run_id = str(uuid.uuid4())

        input_path = os.path.join(
            TEMP_DIR,
            f"{run_id}_{input_file.filename}"
        )

        forn_path = os.path.join(
            TEMP_DIR,
            f"{run_id}_{fornecedores_file.filename}"
        )

        # Save uploaded files
        with open(input_path, "wb") as f:
            shutil.copyfileobj(input_file.file, f)

        with open(forn_path, "wb") as f:
            shutil.copyfileobj(fornecedores_file.file, f)

        # Run ETL
        output_prefix = os.path.join(
            TEMP_DIR,
            f"{run_id}_result"
        )

        result = run_etl(
            input_path,
            forn_path,
            output_prefix=output_prefix
        )

        return {
            "status": "success",
            "run_id": run_id,
            "summary": {
                "total": result["total"],
                "valid": result["valid_count"],
                "rejected": result["rejected_count"]
            },
            "files": {
                "importacao":
                    f"/download/{os.path.basename(result['valid'])}",

                "rejeitados":
                    f"/download/{os.path.basename(result['rejected'])}"
            },
            "detection": result.get("detection", {})
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

# =========================
# DOWNLOAD
# =========================
@app.get("/download/{filename}")
def download_file(filename: str):

    file_path = os.path.join(TEMP_DIR, filename)

    if not os.path.exists(file_path):

        raise HTTPException(
            status_code=404,
            detail=f"File '{filename}' not found"
        )

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/octet-stream"
    )