from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from enum import Enum
import logging
import PyPDF2
from io import BytesIO
import os
import app.utils.bca_convert as bca_convert
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


class conversionType(str, Enum):
    manual = "manual"
    openai = "openai"    
    gemini = "gemini"


def get_unique_filename(bank_type: str,export_type: str):
    timestamp = datetime.now().strftime("%m-%Y_%H%M%S")
    random_part = secrets.token_hex(3)
    if export_type == "excel":
        return f"{bank_type.upper()}_e-statement_transactions_output_{timestamp}_{random_part}.xlsx"
    elif export_type == "csv":
        return f"{bank_type.upper()}_e-statement_transactions_output_{timestamp}_{random_part}.csv"

def get_media_type(export_type: str):
    if export_type == "excel":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif export_type == "csv":
        return "text/csv"


@router.post("/convert-pdf")
async def convert_file(
    file: UploadFile = File(...),
    bank_type: BankType = Form(...),
    export_type: ExportType = Form(...),
    conversion_type: conversionType = Form(...),
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

    # result = await excel.excel_convert(pdf_stream, bank_type,export_type)
    filename = get_unique_filename(bank_type,export_type)
    mediaType = get_media_type(export_type)

    try:
        if conversion_type == "manual":
            if bank_type == "bca":
                try:
                    output = await bca_convert.extract_bca_transactions(pdf_stream, export_type)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))
            elif bank_type == "mandiri":
                try:
                    print("mandiri")
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid bank type",
                )
            return StreamingResponse(
                output,
                media_type=mediaType,
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )
        elif conversion_type == "openai":
            print("openai")
        elif conversion_type == "gemini":
            print("gemini")
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid conversion type",
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
