import time
from dotenv import load_dotenv
import os
import csv, ipaddress, itertools, collections as c
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import settings


# スクリプトファイルのディレクトリを基準に .env ファイルパスを組み立て
BASE_DIR = os.path.dirname(os.path.abspath(__file__))        # scripts/ の絶対パス
PROJECT_ROOT = os.path.dirname(BASE_DIR)                     # その１階層上
input_path = os.path.join(BASE_DIR,"inputfolder/ip_address_list.csv")
output_path = os.path.join(BASE_DIR,"outputfolder/get_ip_address_list_result.csv")

dotenv_path = os.path.join(PROJECT_ROOT, '.env')             # プロジェクトルート直下の .env
load_dotenv(dotenv_path=dotenv_path)  # ここで読み込み

# ワークスペースクライアントの定義
ws = WorkspaceClient(
  host          = os.getenv("DATABRICKS_HOST"),
  client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
  client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
)

# CSVからCIDRを読み込む
def load_cidrs(csv_path: str | Path) -> list[str]:
    """CSV から CIDR を list[str] で返す。列名は 'CIDR' を想定。"""
    cidrs: list[str] = []
    with open(csv_path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)          # ヘッダーを dict key に
        for row in reader:
            cidr = row.get("CIDR")
            if cidr:                        # 空行・欠損はスキップ
                cidrs.append(cidr.strip())
    print(cidrs)
    return cidrs

#  ワークスペースに設定かつ有効化済みのIPアドレスリストを取得
all_ip_address_list = ws.ip_access_lists.list()
enable_ip_address_list = list({
    addr
    for ip in all_ip_address_list if ip.enabled
    for addr in ip.ip_addresses          # ネストをフラット化
})

# ファイル削除
for p in Path(os.path.dirname(output_path)).iterdir():
    if p.is_file():
        p.unlink()           # ファイルを削除
# ファイルへ書き込み
with open(output_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["CIDR"])        # ヘッダー
    writer.writerows([[c] for c in enable_ip_address_list])  # 1 行 1 値

# 入力ファイルからIPアドレスを読み込み
ws_set_list = {ip for ip in enable_ip_address_list}
csv_list = {ip for ip in load_cidrs(input_path)}

# ③ 完全一致判定
if ws_set_list == csv_list:
    print("✅  CSV とワークスペースの IP/CIDR 一覧は完全に一致しています。")
else:
    # 差分を提示
    only_ws  = ws_set_list  - csv_list   # CSV に無い
    only_csv = csv_list - ws_set_list    # ワークスペースに無い
    print("❌ 一致しません。")
    if only_ws:
        print("  - CSV に存在せずワークスペースだけにある CIDR:", sorted(only_ws))
    if only_csv:
        print("  - ワークスペースに存在せず CSV だけにある CIDR:", sorted(only_csv))