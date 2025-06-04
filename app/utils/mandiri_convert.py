import re
import camelot
import pdfplumber
import pandas as pd
from io import BytesIO
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter


def classify_mandiri_transaction(row):
    try:
        amount_str = str(row["Nominal"]).replace(".", "").replace(",", ".").strip()
        amount = float(amount_str.replace("+", "").replace("-", ""))
        if "+" in amount_str:
            return pd.Series([amount, ""])
        elif "-" in amount_str:
            return pd.Series(["", amount])
        else:
            return pd.Series(["", ""])
    except:
        return pd.Series(["", ""])


async def extract_mandiri_transactions(pdf_path: str, export_type: str) -> BytesIO:
    output = BytesIO()
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[0].extract_text()
     
        lines = text.split("\n")
        
        header_lines = []

        for line in lines:
            cleaned_line = re.sub(
                r"\bHALAMAN\s*:\s*\d+\s*/\s*\d+\b", "", line, flags=re.IGNORECASE
            ).strip()
            header_lines.append(cleaned_line)
            if "PERIODE" in cleaned_line.upper():
                break

        match = re.search(
            r"PERIODE\s*:\s*(\d{2}\s\w+\s\d{4})\s*-\s*(\d{2}\s\w+\s\d{4})",
            text,
            re.IGNORECASE,
        )
        year_from_periode = datetime.today().year
        if match:
            year_from_periode = datetime.strptime(match.group(1), "%d %b %Y").year

    except Exception as e:
        print("Header parsing failed:", e)
        header_lines = []
        year_from_periode = str(datetime.today().year)

    print(year_from_periode,header_lines)
    # Extract tables
    tables = camelot.read_pdf(
        filepath=pdf_path,
        pages="all",
        flavor="stream",
        strip_text="\n",
        edge_tol=500,
    )

    if not tables or tables.n == 0:
        pd.DataFrame({"Error": ["No table data found."]}).to_excel(
            output, index=False, engine="openpyxl"
        )
        output.seek(0)
        return output

    all_dfs = []
    column_keywords = {
        "TANGGAL": ["TANGGAL", "DATE"],
        "KETERANGAN": ["KETERANGAN", "REMARKS"],
        "NOMINAL": ["NOMINAL", "AMOUNT"],
        "SALDO": ["SALDO", "BALANCE"],
    }

    keyword_to_standard_col = {
        keyword: standard_col
        for standard_col, keywords in column_keywords.items()
        for keyword in keywords
    }

    for table in tables:
        df = table.df.copy()
        if df.empty:
            continue

        header_row_candidate_idx = -1
        col_idx_to_standard_name = {}

        for r_idx in range(min(df.shape[0], 5)):
            row_values = [
                str(val).upper().replace("\n", " ").strip() for val in df.iloc[r_idx]
            ]
            temp_col_map = {}
            match_count = 0
            for c_idx, cell_value in enumerate(row_values):
                for keyword, standard_col in keyword_to_standard_col.items():
                    if keyword in cell_value:
                        temp_col_map[c_idx] = standard_col
                        match_count += 1
                        break
            if match_count >= 3:
                header_row_candidate_idx = r_idx
                col_idx_to_standard_name = temp_col_map
                break

        new_df_columns = [f"Col_{i}" for i in range(df.shape[1])]
        for idx, name in col_idx_to_standard_name.items():
            new_df_columns[idx] = name
        df.columns = new_df_columns

        if header_row_candidate_idx != -1:
            df = df.iloc[header_row_candidate_idx + 1 :]

        for col in column_keywords:
            if col not in df.columns:
                df[col] = ""

        # Filter bad rows
        if "TANGGAL" in df.columns:
            df = df[
                ~df["TANGGAL"]
                .astype(str)
                .str.contains(r"SALDO AWAL|HALAMAN|BERSAMBUNG", na=False)
            ]

        df.rename(
            columns={
                "TANGGAL": "Tanggal Transaksi",
                "KETERANGAN": "Keterangan Utama",
                "NOMINAL": "Nominal",
                "SALDO": "Saldo",
            },
            inplace=True,
        )

        def format_date(val):
            try:
                date_part = val.strip().split()[0]  # e.g., '01 Jan 2025'
                dt = datetime.strptime(date_part, "%d %b %Y")
                return dt.strftime("%d/%m/%Y")
            except:
                return ""

        if "Tanggal Transaksi" in df.columns:
            df["Tanggal Transaksi"] = (
                df["Tanggal Transaksi"].astype(str).apply(format_date)
            )

        df[["Uang Masuk", "Uang Keluar"]] = df.apply(
            classify_mandiri_transaction, axis=1
        )

        all_dfs.append(df)

    if not all_dfs:
        pd.DataFrame({"Error": ["No valid data found."]}).to_excel(
            output, index=False, engine="openpyxl"
        )
        output.seek(0)
        return output

    merged_df = pd.concat(all_dfs, ignore_index=True)

    if export_type == "csv":
        try:
            csv_string = ""
            if header_lines:
                for line in header_lines:
                    csv_string += (
                        f'"{line}"' + "," * (len(merged_df.columns) - 1) + "\n"
                    )
            csv_string += merged_df.to_csv(index=False)
            output.write(csv_string.encode("utf-8"))
            output.seek(0)
        except Exception as e:
            output = BytesIO()
            pd.DataFrame({"Error": [str(e)]}).to_csv(output, index=False)
            output.seek(0)
    else:
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Transaksi"
            for i, line in enumerate(header_lines, start=1):
                ws.merge_cells(
                    start_row=i,
                    start_column=1,
                    end_row=i,
                    end_column=merged_df.shape[1],
                )
                cell = ws.cell(row=i, column=1, value=line)
                cell.font = Font(bold=True)
                cell.alignment = Alignment(horizontal="center", vertical="center")

            thin_border = Border(
                left=Side(style="thin"),
                right=Side(style="thin"),
                top=Side(style="thin"),
                bottom=Side(style="thin"),
            )

            start_row = len(header_lines) + 2
            for r_idx, row in enumerate(
                dataframe_to_rows(merged_df, index=False, header=True), start=start_row
            ):
                for c_idx, val in enumerate(row, start=1):
                    cell = ws.cell(row=r_idx, column=c_idx, value=val)
                    cell.border = thin_border
                    if r_idx == start_row:
                        cell.font = Font(bold=True)
                        cell.alignment = Alignment(
                            horizontal="center", vertical="center"
                        )
                    else:
                        col_name = merged_df.columns[c_idx - 1]
                        if col_name in [
                            "Tanggal Transaksi",
                            "Keterangan Utama",
                            "Keterangan Tambahan",
                        ]:
                            cell.alignment = Alignment(
                                horizontal="left", vertical="top"
                            )
                        elif col_name in ["Uang Masuk", "Uang Keluar", "Saldo"]:
                            cell.alignment = Alignment(
                                horizontal="right", vertical="top"
                            )
                        else:
                            cell.alignment = Alignment(
                                horizontal="left", vertical="top"
                            )

            for col in ws.columns:
                max_length = max(
                    len(str(cell.value)) if cell.value else 3 for cell in col
                )
                col_letter = get_column_letter(col[0].column)
                ws.column_dimensions[col_letter].width = max(10, max_length + 2)

            wb.save(output)
            output.seek(0)
        except Exception as e:
            output = BytesIO()
            pd.DataFrame({"Error": [str(e)]}).to_excel(
                output, index=False, engine="openpyxl"
            )
            output.seek(0)

    return output

