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
input_path = Path(os.path.join(BASE_DIR,"inputfolder/ip_address_list.csv"))
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

# --------------------------------------------------------------------------- #
# CSV 読み込み: label → [ip1, ip2, ...]
# --------------------------------------------------------------------------- #
def load_ip_records(csv_path: Path) -> list[tuple[str, list[str]]]:
    """
    label,ip_address (カンマ区切り) 形式を
    [(label, [ip1, ip2, ...]), ...] に変換
    """
    records: list[tuple[str, list[str]]] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label = row.get("label", "").strip()
            ips   = row.get("ip_address", "")
            if label and ips:
                ip_list = [ip.strip() for ip in ips.split(",") if ip.strip()]
                records.append((label, ip_list))
    return records

# --------------------------------------------------------------------------- #
# 既存 IP アクセスリスト削除（オプション）
# --------------------------------------------------------------------------- #
def delete_all_ip_access_lists(dry_run: bool = True) -> None:
    lists = list(ws.ip_access_lists.list())
    if not lists:
        print("No IP access lists found; nothing to delete.")
        return

    print(f"Found {len(lists)} IP access list(s).")
    for lst in lists:
        print(f"  - {lst.label} ({lst.list_id})")

    if dry_run:
        print("\nDRY-RUN: 実際の削除はしていません。")
        return

    print("\nDeleting…")
    for lst in lists:
        try:
            ws.ip_access_lists.delete(ip_access_list_id=lst.list_id)
            print(f"  ✔ deleted {lst.label}")
        except Exception as e:
            print(f"  ✖ {lst.label}: {e.error_code} – {e.message}")


# --------------------------------------------------------------------------- #
# IP アクセスリスト作成
# --------------------------------------------------------------------------- #
def create_ip_access_list(label: str, ip_addresses: list[str],
                          list_type=settings.ListType.ALLOW) -> None:
    created = ws.ip_access_lists.create(
        label        = label,
        ip_addresses = ip_addresses,
        list_type    = list_type,
    )
    info = ws.ip_access_lists.get(ip_access_list_id=created.ip_access_list.list_id)
    print(f"✔ created {info.ip_access_list.label}: {', '.join(info.ip_access_list.ip_addresses)}")


# --------------------------------------------------------------------------- #
# メイン処理
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    # ▼ 既存リストを削除したい場合は dry_run=False にする
    delete_all_ip_access_lists(dry_run=False)

    # CSV → [(label, [ips…]), ...]
    ip_records = load_ip_records(input_path)

    # レコード単位でワークスペースへ登録
    for label, ip_list in ip_records:
        # 必要なら prefix 付与
        # label = f"dk-{label}"
        create_ip_access_list(label=label, ip_addresses=ip_list)