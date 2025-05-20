import time
from dotenv import load_dotenv
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import settings
from typing import Tuple


# スクリプトファイルのディレクトリを基準に .env ファイルパスを組み立て
BASE_DIR = os.path.dirname(os.path.abspath(__file__))        # scripts/ の絶対パス
PROJECT_ROOT = os.path.dirname(BASE_DIR)                     # その１階層上
dotenv_path = os.path.join(PROJECT_ROOT, '.env')             # プロジェクトルート直下の .env
load_dotenv(dotenv_path=dotenv_path)  # ここで読み込み

# ワークスペースクライアントの定義
w = WorkspaceClient(
  host          = os.getenv("DATABRICKS_HOST"),
  client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
  client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
)
KEY = "enableIpAccessLists"   

def get_ip_access_status(w: WorkspaceClient) -> bool:
    """
    Returns:
        True  –  IP アクセスリスト機能が有効
        False –  無効
    """
    conf = w.workspace_conf.get_status(keys=KEY)   # {"enableIpAccessLists": "true"|"false"}
    return conf.get(KEY, "false").lower() == "true"


def update_ip_access(w: WorkspaceClient, enable: str) -> None:
    """IP アクセスリスト機能を有効化（冪等）"""
    w.workspace_conf.set_status({KEY: enable})


def main() -> None:
    enabled = get_ip_access_status(w)

    if enabled:
        print("✅ すでに IP アクセスリスト機能は有効です。")
        print("🎉 IP アクセスリスト機能が有効なため、無効化します。")
        update_ip_access(w,"False")
        return

    print("ℹ️  IP アクセスリスト機能が無効のため、有効化します…")
    update_ip_access(w,"True")

    # 反映確認
    if get_ip_access_status(w):
        print("🎉 IP アクセスリスト機能を有効化しました。")
    else:
        raise RuntimeError("IP アクセスリスト機能の有効化に失敗しました。")


if __name__ == "__main__":
    main()