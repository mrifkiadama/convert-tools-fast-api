import io
import re
import openai
import time
import os
import pdfplumber
from openpyxl import Workbook
import secrets
import google.generativeai as genai
from datetime import datetime
from fastapi.responses import StreamingResponse


def get_unique_filename(bank_type: str, export_type: str):
    timestamp = datetime.now().strftime("%m-%Y_%H%M%S")
    random_part = secrets.token_hex(3)

    if bank_type == "bca":
        if export_type == "excel":
            return f"{bank_type.upper()}_E-Statement_{timestamp}_{random_part}.xlsx"
        elif export_type == "csv":
            return f"{bank_type.upper()}_E-Statement_{timestamp}_{random_part}.csv"


async def excel_convert(pdf_stream: io.BytesIO, type_bank: str, export_type: str) -> io.BytesIO:
    text = extract_text_from_pdf(pdf_stream)
    fileTitle = get_unique_filename(type_bank, export_type)
    wb = Workbook()
    ws = wb.active
    ws.title = type_bank.upper() + "_E-Statement"

    ws.append([
        "Tanggal Transaksi",
        "Keterangan Utama",
        "Keterangan Tambahan",
        "Uang Masuk IDR",
        "Uang Keluar IDR",
        "Saldo"
    ])

    lines = text.splitlines()
    periode_match = re.search(r"PERIODE\s*:\s*(\w+)\s+(\d{4})", text, re.IGNORECASE)
    if not periode_match:
        raise ValueError("❌ Gagal menemukan PERIODE di file PDF.")
    bulan, tahun = periode_match.groups()

    bulan_map = {
            "JANUARI": "01", "FEBRUARI": "02", "MARET": "03", "APRIL": "04",
            "MEI": "05", "JUNI": "06", "JULI": "07", "AGUSTUS": "08",
            "SEPTEMBER": "09", "OKTOBER": "10", "NOVEMBER": "11", "DESEMBER": "12"
        }
    bulan_num = bulan_map.get(bulan.upper())
    if not bulan_num:
        raise ValueError(f"❌ Nama bulan tidak valid: {bulan}")
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Match date at beginning of line
        if re.match(r"^\d{2}/\d{2}", line):
            hari = line[:2]
            tanggal_full = f"{hari}/{bulan_num}/{tahun}"
            # tanggal = line[:5]
            detail_line = line[6:].strip()

            # Try to extract numbers from this line
            nums = re.findall(r"[\d.,]+", detail_line)
            crdb = "CR" if "CR" in detail_line else "DB" if "DB" in detail_line else ""

            uang_masuk = uang_keluar = saldo = ""
            if "CR" in detail_line:
                uang_masuk = nums[0] if nums else ""
                saldo = nums[1] if len(nums) > 1 else ""
            elif "DB" in detail_line:
                uang_keluar = nums[0] if nums else ""
                saldo = nums[1] if len(nums) > 1 else ""
            elif len(nums) == 2:
                uang_masuk = nums[0]
                saldo = nums[1]
            elif len(nums) == 1:
                saldo = nums[0]

            # Collect keterangan tambahan from following lines (if any)
            ket_tambahan = ""
            j = i + 1
            while j < len(lines) and not re.match(r"^\d{2}/\d{2}", lines[j]):
                if re.search(r"[A-Za-z]", lines[j]):
                    ket_tambahan += lines[j].strip() + " "
                j += 1

            ws.append([
                tanggal_full,
                f"{detail_line} {crdb}".strip(),
                ket_tambahan.strip(),
                uang_masuk.replace(",", "").replace(".", "") if uang_masuk else "",
                uang_keluar.replace(",", "").replace(".", "") if uang_keluar else "",
                saldo.replace(",", "").replace(".", "") if saldo else "",
            ])

            i = j  # Skip to next transaction
        else:
            i += 1

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    filename = fileTitle

    return StreamingResponse(
        output,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


def extract_text_from_pdf(pdf_stream, expected_columns=6, default_value=" "):
    full_text = ""

    with pdfplumber.open(pdf_stream) as pdf:
        for i, page in enumerate(pdf.pages):
            text = ""
            page_text = page.extract_text()
            if page_text:
                text += page_text

            tables = page.extract_tables()
            if tables:
                for table in tables:
                    for row in table:
                        # Normalize the row to expected column length
                        safe_row = [
                            (cell.replace("\n", " ").strip() if cell else default_value)
                            for cell in row
                        ]

                        # Fill missing columns if row is too short
                        while len(safe_row) < expected_columns:
                            safe_row.append(default_value)

                        # Trim extra columns if needed
                        safe_row = safe_row[:expected_columns]

                        text += "\n" + " ".join(safe_row)

            if text.strip():
                full_text += f"\n\n--- Page {i+1} ---\n{text}"
            else:
                print(f"⚠️  Warning: No extractable text/tables found on page {i+1}")

    # Write debug output
    with open("debug_output.txt", "w", encoding="utf-8") as f:
        f.write(full_text)

    return full_text


def truncate_input(text, limit=12000):
    return text[:limit]

def convert_to_openai(text):
    # Use OpenAI API
    system_prompt = (
        "You are a financial assistant. Extract all bank transactions from the input text and format them as a table. "
        "Columns: Tanggal Transaksi, Keterangan Utama, Keterangan Tambahan, Uang Masuk IDR, Uang Keluar IDR, Saldo. "
        "Return only CSV format without explanation."
    )
    user_prompt = f"Here is the text from the bank statement PDF:\n\n{truncate_input(text)}\n\nExtract and format as CSV table."

    start_time = time.time()
    
    openai.api_key = os.getenv("OPENAI_API_KEY")
    
    response = openai.ChatCompletion.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2,
        max_tokens=1000,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["\n\n"]
    )
    
    elapsed = time.time() - start_time
    usage = response.get("usage", {})
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    total_tokens = usage.get("total_tokens", 0)
    cost = (prompt_tokens / 1000 * 0.01) + (completion_tokens / 1000 * 0.03)
    
    print(f"🕒 GPT processing time: {elapsed:.2f} seconds")
    print(f"📊 Token usage: prompt={prompt_tokens}, completion={completion_tokens}, total={total_tokens}")
    print(f"💵 Estimated cost: ${cost:.4f}")
    
    return response['choices'][0]['message']['content']


def convert_to_gemini(text):
    # Use Gemini API
    geminiApiKey = os.getenv("GEMINI_API_KEY")
    genai.configure(api_key=geminiApiKey)
