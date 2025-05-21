import time
from dotenv import load_dotenv
import os
import csv
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import settings


# スクリプトファイルのディレクトリを基準に .env ファイルパスを組み立て
BASE_DIR = os.path.dirname(os.path.abspath(__file__))        # scripts/ の絶対パス
PROJECT_ROOT = os.path.dirname(BASE_DIR)                     # その１階層上
input_path = os.path.join(BASE_DIR,"inputfolder/ip_address_list.csv")
dotenv_path = os.path.join(PROJECT_ROOT, '.env')             # プロジェクトルート直下の .env
load_dotenv(dotenv_path=dotenv_path)  # ここで読み込み
print(f"""
    {input_path}
    """
)
# ワークスペースクライアントの定義
ws = WorkspaceClient(
  host          = os.getenv("DATABRICKS_HOST"),
  client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
  client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
)

# IPアクセスリストの削除
def delete_all_ip_access_lists(dry_run: bool = True) -> None:
    lists = list(ws.ip_access_lists.list())   # ← generator → list に展開

    if not lists:
        print("No IP access lists found; nothing to delete.")
        return

    print(f"Found {len(lists)} IP access list(s).")

    for iplist in lists:
        print(f"  - {iplist.label} ({iplist.list_id})")

    if dry_run:
        print("\nDRY-RUN モード: 実際の削除は行っていません。")
        return

    print("\nDeleting…")
    for iplist in lists:
        try:
            ws.ip_access_lists.delete(ip_access_list_id=iplist.list_id)
            print(f"  ✔ deleted {iplist.label}")
        except Exception as e:
            # “default” リストなど削除不可のケース対策
            print(f"  ✖ {iplist.label}: {e.error_code} – {e.message}")

    print("Done.")
# IPアクセスリストの作成
def create_ip_access_list(label: str, ip_addresses: list[str], list_type) -> None:
  created = ws.ip_access_lists.create(
    label=label,
    ip_addresses=ip_addresses,
    list_type=list_type,
  )
  by_id = ws.ip_access_lists.get(ip_access_list_id=created.ip_access_list.list_id)
  print(by_id)

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


if __name__ == "__main__":
    # ▼ dry_run=True で結果だけ確認し、問題なければ False にする
    delete_all_ip_access_lists(dry_run=False)
    create_ip_access_list(label=f"dk-{time.time_ns()}", ip_addresses=load_cidrs(input_path), list_type=settings.ListType.ALLOW)