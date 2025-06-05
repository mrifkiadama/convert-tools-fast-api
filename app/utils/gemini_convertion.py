import io
import time
import os
import pdfplumber
import google.generativeai as genai


async def process_convert(pdf_stream: io.BytesIO, export_type: str) -> io.BytesIO:
    text = extract_text_from_pdf(pdf_stream)
    result = await convert_to_gemini(text)

    return result


def extract_text_from_pdf(pdf_stream):
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
                        if any(cell for cell in row):  # skip empty rows
                            text += "\n" + ",".join(
                                [cell.strip() if cell else "" for cell in row]
                            )

            if text.strip():
                full_text += f"\n\n--- Page {i+1} ---\n{text}"
            else:
                print(f"⚠️  Warning: No extractable text/tables found on page {i+1}")

    return full_text


def truncate_input(text, limit=12000):
    return text[:limit]


def convert_to_gemini(text):
    # Use Gemini API
    geminiApiKey = os.getenv("GEMINI_API_KEY")
    genai.configure(api_key=geminiApiKey)

    start_time = time.time()

    response = genai.generate(
        model="gpt-4",
        prompt=f"Here is the text from the bank statement PDF:\n\n{truncate_input(text)}\n\nExtract and format as CSV table.",
        max_tokens=1000,
        temperature=0.2,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["\n\n"],
    )

    elapsed = time.time() - start_time
    usage = response.get("usage", {})
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    total_tokens = usage.get("total_tokens", 0)
    cost = (prompt_tokens / 1000 * 0.01) + (completion_tokens / 1000 * 0.03)

    print(f"🕒 GPT processing time: {elapsed:.2f} seconds")
    print(
        f"📊 Token usage: prompt={prompt_tokens}, completion={completion_tokens}, total={total_tokens}"
    )
    print(f"💵 Estimated cost: ${cost:.4f}")

    return response
