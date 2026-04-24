from fastapi import FastAPI, UploadFile, File
import shutil
import os
from etl import run_etl

app = FastAPI()

TEMP_DIR = "temp"
os.makedirs(TEMP_DIR, exist_ok=True)

@app.post("/process")
async def process_files(
    input_file: UploadFile = File(...),
    fornecedores_file: UploadFile = File(...)
):
    input_path = os.path.join(TEMP_DIR, input_file.filename)
    forn_path = os.path.join(TEMP_DIR, fornecedores_file.filename)

    # Save files
    with open(input_path, "wb") as f:
        shutil.copyfileobj(input_file.file, f)

    with open(forn_path, "wb") as f:
        shutil.copyfileobj(fornecedores_file.file, f)

    # Run ETL
    result = run_etl(input_path, forn_path, output_prefix="temp/result")

    return {
        "status": "success",
        "summary": result
    }
