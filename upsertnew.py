import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Tải biến môi trường từ file .env
load_dotenv()

# Lấy URL và API Key từ biến môi trường
url = os.getenv("url")
key = os.getenv("key")


def connect_to_supabase(SUPABASE_URL: str, SUPABASE_KEY: str) -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL or API key is missing.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upload_csv_to_supabase(supabase: Client, table_name: str, csv_file: str):
    try:
        data = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"File not found: {csv_file}")
        return
    except pd.errors.ParserError:
        print("Error parsing the CSV file.")
        return
 # xoá wallet trùng
    for index, row in data.iterrows():
        wallet = row['wallet']
        # Kiểm tra xem wallet đã tồn tại trong bảng hay chưa
        response = supabase.table(table_name).delete().eq('wallet', wallet).execute()

    records = data.to_dict(orient="records")
    response = supabase.table(table_name).upsert(records).execute()

    if response.data:
        print(f"Operation successful. Retrieved data: {response.data}")
    else:
        print("No data retrieved or operation failed.")

def main():
    try:
        supabase = connect_to_supabase(url, key)
        print("Connected to Supabase successfully!")

        # Path to CSV and table name
        csv_file = "E:/FILEMANAGEMENT_PC/C/codepython/botalertdatabase/1731940797_wallets.csv"
        table_name = "potential_walletss"

        upload_csv_to_supabase(supabase, table_name, csv_file)
        # clean_up_potential_wallets(supabase, "potential_wallets")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
