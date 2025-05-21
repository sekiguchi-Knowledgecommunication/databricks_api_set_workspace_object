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
input_path = Path(os.path.join(BASE_DIR,"inputfolder/ip_address_list.csv"))
output_path = Path(os.path.join(BASE_DIR,"outputfolder/get_ip_address_list_result.csv"))

dotenv_path = os.path.join(PROJECT_ROOT, '.env')             # プロジェクトルート直下の .env
load_dotenv(dotenv_path=dotenv_path)  # ここで読み込み

# ワークスペースクライアントの定義
ws = WorkspaceClient(
  host          = os.getenv("DATABRICKS_HOST"),
  client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
  client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
)

# ------------------------------------------------------------------------------
# 1. CSV 側： label → [ip1, ip2, ...] へ変換
# ------------------------------------------------------------------------------
def load_ip_lists(csv_path: Path) -> dict[str, list[str]]:
    """
    CSV (label, ip_address) 形式を
    {label: [ip1, ip2, ...]} にロード
    """
    result: dict[str, list[str]] = {}
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label = row.get("label", "").strip()
            ips   = row.get("ip_address", "")
            if not label or not ips:
                continue
            # カンマ区切り → リスト化 & 前後空白除去
            result[label] = [ip.strip() for ip in ips.split(",") if ip.strip()]
    return result


csv_ip_dict = load_ip_lists(input_path)

# ------------------------------------------------------------------------------
# 2. ワークスペース側： label → [ip1, ip2, ...] へ変換
# ------------------------------------------------------------------------------
ws_ip_dict: dict[str, list[str]] = {
    al.label: al.ip_addresses
    for al in ws.ip_access_lists.list()
    if al.enabled                      # 有効なリストのみ
}

# ------------------------------------------------------------------------------
# 3. 完全一致を判定し差分を出力
# ------------------------------------------------------------------------------
def normalize(s: str) -> str:
    return s.strip().lower()

def compare_ip_maps(expect: dict[str, list[str]], actual: dict[str, list[str]]):
    all_labels = expect.keys() | actual.keys()
    diffs: list[dict] = []

    for label in sorted(all_labels):
        exp_set = {normalize(ip) for ip in expect.get(label, [])}
        act_set = {normalize(ip) for ip in actual.get(label, [])}
        if exp_set != act_set:
            diffs.append({
                "label"      : label,
                "only_csv"   : sorted(exp_set - act_set),
                "only_ws"    : sorted(act_set - exp_set)
            })
    return diffs

diff_report = compare_ip_maps(csv_ip_dict, ws_ip_dict)

if not diff_report:
    print("✅ CSV とワークスペースの IP アドレスはラベル単位で完全一致しました。")
else:
    print("❌ 一致しないラベルがありました。差分を表示します。")
    for d in diff_report:
        print(f"[{d['label']}]")
        if d["only_csv"]:
            print("  - CSV にのみ存在:", ", ".join(d['only_csv']))
        if d["only_ws"]:
            print("  - ワークスペースにのみ存在:", ", ".join(d['only_ws']))
    # 差分が NG 扱いなら例外化
    # raise SystemExit("IP リストが一致しません")

# ------------------------------------------------------------------------------
# 4. 出力フォルダをクリーンにして結果を書き出し
# ------------------------------------------------------------------------------
output_path.parent.mkdir(exist_ok=True)

# 既存ファイル削除
for p in output_path.parent.iterdir():
    if p.is_file():
        p.unlink()

# label, ip_address (カンマ連結) で再書き出し
with output_path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["label", "ip_address"])
    for label, ips in sorted(ws_ip_dict.items()):
        writer.writerow([label, ",".join(ips)])

print(f"📄 {output_path} にワークスペース側の IP 一覧を書き出しました。")