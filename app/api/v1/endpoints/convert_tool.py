from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from enum import Enum
import logging
import PyPDF2
from io import BytesIO
import os
import app.utils.csv_convert as csv
import app.utils.excel_convert as convert_to_excel
import secrets
from datetime import datetime
from fastapi.responses import StreamingResponse

router = APIRouter()

logger = logging.getLogger(__name__)


class BankType(str, Enum):
    bca = "bca"
    bni = "bni"
    mandiri = "mandiri"
    bri = "bri"


class ExportType(str, Enum):
    # json = "json"
    excel = "excel"
    csv = "csv"


def get_unique_filename(bank_type: str,export_type: str):
    timestamp = datetime.now().strftime("%m-%Y_%H%M%S")
    random_part = secrets.token_hex(3)
    if export_type == "excel":
        return f"{bank_type.upper()}_e-statement_transactions_output_{timestamp}_{random_part}.xlsx"
    elif export_type == "csv":
        return f"{bank_type.upper()}_e-statement_transactions_output_{timestamp}_{random_part}.csv"


@router.post("/convert-pdf")
async def convert_file(
    file: UploadFile = File(...),
    bank_type: BankType = Form(...),
    export_type: ExportType = Form(...),
):
    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    # Explicit validation (optional because Form(...) already requires input)
    if not bank_type:
        raise HTTPException(
            status_code=400, detail="bank_type is required and cannot be empty"
        )
    if not export_type:
        raise HTTPException(
            status_code=400, detail="export_type is required and cannot be empty"
        )

    if file.content_type != "application/pdf" and not file.filename.lower().endswith(
        ".pdf"
    ):
        raise HTTPException(
            status_code=400,
            detail="Only PDF files are allowed",
        )

    # Read PDF content into memory

    pdf_stream = BytesIO(contents)

    # Get total page count using PyPDF2
    reader = PyPDF2.PdfReader(pdf_stream)
    total_pages = len(reader.pages)

    existing_token = int(os.getenv("EXISTING_TOKEN", "0"))

    if total_pages > existing_token:
        raise HTTPException(
            status_code=400,
            detail="Your tokens are not sufficient to perform the conversion.",
        )
    pdf_stream.seek(0)

    if export_type == "excel":
        # result = await excel.excel_convert(pdf_stream, bank_type,export_type)
        if bank_type == "bca":
            try:
                output = convert_to_excel.extract_bca_transactions(pdf_stream, bank_type, export_type)
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

            filename = get_unique_filename(bank_type,export_type)
         

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    elif export_type == "csv":
        result = await csv.csv_convert(pdf_stream, bank_type)
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid export type",
        )
