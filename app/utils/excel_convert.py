import pdfplumber
import io
from fastapi.responses import StreamingResponse
import camelot
import re
import pandas as pd
from io import BytesIO
from PyPDF2 import PdfReader
from datetime import datetime

def extract_bca_transactions(pdf_path: str, bank_type: str, export_type: str) -> BytesIO:

    import camelot
    import pandas as pd
    from io import BytesIO
    from PyPDF2 import PdfReader
    import re

    tables = camelot.read_pdf(
        filepath=pdf_path,
        pages="all",
        flavor="stream",
        strip_text="\n",
        edge_tol=500,
    )

    if not tables:
        print("No tables found in the PDF. Please check the PDF path and structure.")
        output = BytesIO()
        pd.DataFrame().to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
        return output

    all_dfs = []

    column_keywords = {
        'TANGGAL': ['TANGGAL', 'DATE'],
        'KETERANGAN': ['KETERANGAN', 'DESCRIPTION', 'DETAIL'],
        'CBG': ['CBG', 'BRANCH'],
        'MUTASI': ['MUTASI', 'DEBIT', 'CREDIT', 'AMOUNT'],
        'SALDO': ['SALDO', 'BALANCE']
    }

    keyword_to_standard_col = {
        keyword: standard_col
        for standard_col, keywords in column_keywords.items()
        for keyword in keywords
    }

    for i, table in enumerate(tables):
        df = table.df.copy()
        if df.shape[0] == 0:
            continue

        col_idx_to_standard_name = {}
        header_row_candidate_idx = -1

        for r_idx in range(min(df.shape[0], 5)):
            row_values = [str(val).upper().replace('\n', ' ').strip() for val in df.iloc[r_idx]]
            found_keywords_count = 0
            temp_col_map = {}
            for c_idx, cell_value in enumerate(row_values):
                for keyword, standard_col in keyword_to_standard_col.items():
                    if keyword in cell_value:
                        temp_col_map[c_idx] = standard_col
                        found_keywords_count += 1
                        break
            if found_keywords_count >= 3:
                col_idx_to_standard_name = temp_col_map
                header_row_candidate_idx = r_idx
                break

        new_df_columns = [f'Col_{j}' for j in range(df.shape[1])]
        for c_idx, standard_name in col_idx_to_standard_name.items():
            if c_idx < len(new_df_columns):
                new_df_columns[c_idx] = standard_name

        df.columns = new_df_columns

        if header_row_candidate_idx != -1:
            df = df[header_row_candidate_idx + 1:].copy()

        df = df.loc[:, ~df.columns.str.startswith('Col_') | (df.apply(lambda x: x.astype(str).str.strip() != '').any())].copy()

        for col in column_keywords.keys():
            if col not in df.columns:
                df[col] = ''

        if 'TANGGAL' in df.columns:
            df = df[~df['TANGGAL'].astype(str).str.contains(r'^(?:SALDO AWAL|HALAMAN|Bersambung)', na=False, regex=True)]

        df.replace('', pd.NA, inplace=True)
        df.dropna(subset=list(column_keywords.keys()), how='all', inplace=True)

        # ✅ Custom column remapping
        rename_map = {
            'TANGGAL': 'Tanggal Transaksi',
            'Col_1': 'Keterangan Utama',
            'KETERANGAN': 'Keterangan Tambahan',
            'CBG': 'CBG',
            'MUTASI': 'Mutasi',
            'Col_5': 'Type',
            'SALDO': 'Saldo'
        }
        df = df.rename(columns=rename_map)

        if 'Col_6' in df.columns:
            df.drop(columns=['Col_6'], inplace=True)

        all_dfs.append(df)

    if not all_dfs:
        output = BytesIO()
        pd.DataFrame().to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
        return output

    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df.replace('', pd.NA, inplace=True)
    merged_df.dropna(how='all', inplace=True)

    output = BytesIO()
    try:
        merged_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
    except Exception as e:
        print(f"Error exporting to Excel BytesIO: {e}")
        output = BytesIO()
        pd.DataFrame().to_excel(output, index=False, engine="openpyxl")
        output.seek(0)

    return output

# base export function BCA
# def extract_bca_transactions(pdf_path: str, bank_type: str, export_type: str) -> BytesIO:

#     tables = camelot.read_pdf(
#         filepath=pdf_path,
#         pages="all",
#         flavor="stream",
#         strip_text="\n",
#         edge_tol=500,
#     )
    
#     if not tables:
#         print("No tables found in the PDF. Please check the PDF path and structure.")
#         output = BytesIO()
#         pd.DataFrame().to_excel(output, index=False, engine="openpyxl")
#         output.seek(0)
#         return output

#     all_dfs = []

#     # Define a set of expected column names and their possible keywords
#     column_keywords = {
#         'TANGGAL': ['TANGGAL', 'DATE'],
#         'KETERANGAN': ['KETERANGAN', 'DESCRIPTION', 'DETAIL'],
#         'CBG': ['CBG', 'BRANCH'],
#         'MUTASI': ['MUTASI', 'DEBIT', 'CREDIT', 'AMOUNT'],
#         'SALDO': ['SALDO', 'BALANCE']
#     }

#     keyword_to_standard_col = {
#         keyword: standard_col
#         for standard_col, keywords in column_keywords.items()
#         for keyword in keywords
#     }

#     # Process each table extracted by Camelot
#     for i, table in enumerate(tables):
#         df = table.df.copy() # Get the DataFrame from the table object
#         print(f"\n--- Processing Table {i+1} (Original Camelot Output) ---")
#         print(df.to_string()) # Show original extracted table

#         if df.shape[0] == 0: # Skip empty dataframes
#             continue

#         # --- Robust Column Identification and Standardization ---
#         col_idx_to_standard_name = {}
#         header_row_candidate_idx = -1 

#         for r_idx in range(min(df.shape[0], 5)): # Check first 5 rows for header
#             row_values = [str(val).upper().replace('\n', ' ').strip() for val in df.iloc[r_idx]]

#             found_keywords_count = 0
#             temp_col_map = {} 

#             for c_idx, cell_value in enumerate(row_values):
#                 for keyword, standard_col in keyword_to_standard_col.items():
#                     if keyword in cell_value:
#                         temp_col_map[c_idx] = standard_col
#                         found_keywords_count += 1
#                         break 

#             if found_keywords_count >= 3: # At least 3 main columns found
#                 col_idx_to_standard_name = temp_col_map
#                 header_row_candidate_idx = r_idx
#                 break 

#         new_df_columns = [f'Col_{j}' for j in range(df.shape[1])] # Default generic names
#         for c_idx, standard_name in col_idx_to_standard_name.items():
#             if c_idx < len(new_df_columns):
#                 new_df_columns[c_idx] = standard_name

#         df.columns = new_df_columns

#         # Remove the identified header row and any rows above it
#         if header_row_candidate_idx != -1:
#             df = df[header_row_candidate_idx + 1:].copy()

#         # Drop any columns that are still generic 'Col_X' and don't contain data
#         df = df.loc[:, ~df.columns.str.startswith('Col_') | (df.apply(lambda x: x.astype(str).str.strip() != '').any())].copy()

#         # Ensure all expected standard columns exist, adding empty ones if missing
#         # This is important for consistent concatenation later
#         for col in column_keywords.keys():
#             if col not in df.columns:
#                 df[col] = '' 

#         # --- Minimal Row Filtering for Raw Output ---
#         # Only remove rows that are clearly non-data (like page headers/footers)
#         if 'TANGGAL' in df.columns:
#             df = df[~df['TANGGAL'].astype(str).str.contains(r'^(?:SALDO AWAL|HALAMAN|Bersambung)', na=False, regex=True)]

#         # Replace empty strings with NA and drop rows where ALL of the *standard* columns are empty
#         df.replace('', pd.NA, inplace=True)
#         df.dropna(subset=list(column_keywords.keys()), how='all', inplace=True)

#         print(f"Table {i+1} columns after standardization and initial filtering: {df.columns.tolist()}")
#         print(df.to_string(),'check df after header processing and initial filtering')

#         all_dfs.append(df)

#     if not all_dfs:
#         print("No valid dataframes to concatenate after initial processing.")
#         output = BytesIO()
#         pd.DataFrame().to_excel(output, index=False, engine="openpyxl")
#         output.seek(0)
#         return output

#     # Concatenate all processed dataframes into a single DataFrame
#     merged_df = pd.concat(all_dfs, ignore_index=True)

#     # Final cleanup of any fully empty rows after merging
#     merged_df.replace('', pd.NA, inplace=True)
#     merged_df.dropna(how='all', inplace=True)

#     print(f"\n--- Final Raw Merged Table Data ---")
#     print(merged_df.to_string())
#     print(f"Final DataFrame shape: {merged_df.shape}")

#     # Export to Excel using BytesIO
#     output = BytesIO()
#     try:
#         merged_df.to_excel(output, index=False, engine="openpyxl")
#         output.seek(0) # Rewind the buffer to the beginning
#         print(f"\nSuccessfully prepared Excel data in BytesIO.")
#     except Exception as e:
#         print(f"Error exporting to Excel BytesIO: {e}")
#         output = BytesIO()
#         pd.DataFrame().to_excel(output, index=False, engine="openpyxl")
#         output.seek(0)

#     return output
