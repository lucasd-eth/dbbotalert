from supabase import create_client, Client
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
# Lấy URL và KEY từ file .env
url = os.getenv("url")
key = os.getenv("key")
# class SupabaseClient:

def connect_to_supabase(SUPABASE_URL: str, SUPABASE_KEY: str):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase URL or API key is missing.")
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    
def get_data(self, table_name: str, columns: list = None):
        # Nếu không truyền columns, lấy tất cả các cột
        if columns is None:
            return self.client.table(table_name).select("*").execute()
        else:
            # Nếu có truyền columns, lấy các cột cụ thể
            return self.client.table(table_name).select(",".join(columns)).execute()
    
def get_and_print_txns_per_token(self):
        try:
            # Lấy tất cả giao dịch chưa được in ra (is_print = 0)
            response = (
                self.client.table("txns_dune")
                .select("id, wallet, token_address, sol_value, update_at, is_print")
                .eq("is_print", 0)   
                .order("update_at")  # Sắp xếp theo thời gian
                .execute()
            )

            # Danh sách giao dịch chưa được in
            notprint_txns = response.data

            # Tạo một dictionary để nhóm giao dịch theo từng token_address
            grouped_notprint_txns = {}
            for txn in notprint_txns:
                token_address = txn["token_address"]
                if token_address not in grouped_notprint_txns:
                    grouped_notprint_txns[token_address] = []
                grouped_notprint_txns[token_address].append(txn)

            # Kết quả trả về
            results = {}

            # Xử lý từng token_address
            for token_address, token_txns in grouped_notprint_txns.items():
                running_total = 0
                to_print = []
                distinct_wallets = set()  # Set to track distinct wallets

                # Cộng dồn sol_value theo token_address và lưu các ví khác nhau
                for txn in token_txns:
                    running_total += txn["sol_value"]
                    distinct_wallets.add(txn["wallet"])  # Add wallet to the set
                    to_print.append(txn)

                # Nếu tổng cộng dồn >= threshold và có ít nhất 2 ví khác nhau, thì in ra và cập nhật trạng thái
                if running_total >= 50 and len(distinct_wallets) > 2:
                    # Lưu thông tin giao dịch cần in
                    results[token_address] = {
                        "transactions": to_print,
                        "total_value": round(running_total, 2),
                        "distinct_wallets": len(distinct_wallets)  # Store the count of distinct wallets
                    }

                    # In thông tin giao dịch
                    print(f"Token: {token_address} - Total Sol Value: {round(running_total, 2)}")
                    print(f"  Distinct Wallets: {len(distinct_wallets)}")
                    for txn in to_print:
                        print(f"  Wallet: {txn['wallet']}, Sol Value: {round(txn['sol_value'], 2)}")

                    # Đánh dấu giao dịch đã in
                    ids_to_update = [txn["id"] for txn in to_print]
                    update_response = (
                        self.client.table("txns_dune")
                        .update({"is_print": 1})
                        .in_("id", ids_to_update)
                        .execute()
                    )
            return results
        except Exception as e:
            print(f"Error in get_and_print_txns_per_token: {e}")
            return
        
def upload_potential_wallets_to_supabase(self, csv_file: str):
        table_name = "potential_wallets"
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
            # Kiểm tra xem nếu wallet tồn tại thì xoá trong supabase
            response = self.client.table(table_name).delete().eq('wallet', wallet).execute()

        records = data.to_dict(orient="records")
        response = self.client.table(table_name).upsert(records).execute()

        if response.data:
            print(f"Operation successful. Retrieved data: {response.data}")
        else:
            print("No data retrieved or operation failed.")

def upload_txns_dune_to_supabase(self, csv_file: str):
            table_name = "txns_dune"
            try:
                data = pd.read_csv(csv_file)
            except FileNotFoundError:
                print(f"File not found: {csv_file}")
                return
            except pd.errors.ParserError:
                print("Error parsing the CSV file.")
                return

            records = data.to_dict(orient="records")
            response = self.client.table(table_name).upsert(records).execute()

            if response.data:
                print(f"Operation successful. Retrieved data: {response.data}")
            else:
                print("No data retrieved or operation failed.")
    
    # def update_total_buy_from_txns_dune(supabase: Client):
    #     # Tên bảng
    #     source_table = "txns_dune"
    #     target_table = "total_buy"

    #     # Truy vấn dữ liệu đã nhóm từ txns_dune
    #     query = f"""
    #     SELECT 
    #         token_address,
    #         wallet,
    #         SUM(value) AS total_value
    #     FROM {source_table}
    #     GROUP BY token_address, wallet
    #     """

    #     try:
    #         # Lấy dữ liệu nhóm từ bảng txns_dune
    #         grouped_data = supabase.rpc("execute_sql", {"sql": query}).execute()
            
    #         if not grouped_data.data:
    #             print("Không tìm thấy dữ liệu nào từ bảng txns_dune.")
    #             return

    #         # Cập nhật bảng total_buy bằng cách duyệt từng nhóm
    #         for record in grouped_data.data:
    #             response = supabase.table(target_table).update({
    #                 "total_value": record["total_value"]
    #             }).match({
    #                 "token_address": record["token_address"],
    #                 "wallet": record["wallet"]
    #             }).execute()

    #             if response.status_code == 200:
    #                 print(f"Cập nhật thành công cho token_address={record['token_address']} và wallet={record['wallet']}")
    #             else:
    #                 print(f"Lỗi khi cập nhật: {response}")

    #     except Exception as e:
    #         print(f"Đã xảy ra lỗi: {e}")

def main():
        try:
            supabase = connect_to_supabase(url,key)
            # supabase.__init__
            print("Connected to Supabase successfully!")

            # Path to CSV and table name
            wallet_csv_file = "E:/FILEMANAGEMENT_PC/C/codepython/botalertdatabase/1731940797_wallets.csv"
            txn_csv_file= "E:/FILEMANAGEMENT_PC/C/codepython/botalertdatabase/1731940797.csv"

            # supabase.upload_potential_wallets_to_supabase(wallet_csv_file)
            # supabase.upload_txns_dune_to_supabase(txn_csv_file)
            supabase.get_data("potential_wallets")

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
        main()

# Lấy dữ liệu từ bảng "potential_wallets"
# wallets = supabase.get_data("potential_wallets",columns=["wallet"])
# txn = supabase.get_data("txns_dune",columns=["wallet"])
# txn_result = supabase.get_txn_by_wallet()
# txn_printed = supabase.get_and_print_txns_per_token()
#print(supabase.get_and_print_txns_per_token())
