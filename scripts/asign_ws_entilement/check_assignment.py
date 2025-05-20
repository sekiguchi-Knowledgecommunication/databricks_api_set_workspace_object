
from __future__ import annotations
from dotenv import load_dotenv

import os
import re
from pathlib import Path
from typing import List, Dict, Set
from urllib.parse import urlparse

import pandas as pd
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service.iam import WorkspacePermission,ComplexValue

# ---------- 定数 -------------------------------------------------------------
# スクリプトファイルのディレクトリを基準に .env ファイルパスを組み立て
BASE_DIR = os.path.dirname(os.path.abspath(__file__))        # scripts/ の絶対パス
PROJECT_ROOT = os.path.dirname(BASE_DIR)                     # その１階層上
# クレデンシャルの取得
dotenv_path = os.path.join(PROJECT_ROOT, '.env')             # プロジェクトルート直下の .env
# ワークスペースクライアントの定義
w = WorkspaceClient(
  host          = os.getenv("DATABRICKS_HOST"),
  client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
  client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
)


# アカウントクライアントの定義
ac = AccountClient(
    host          = os.getenv("DATABRICKS_ACCOUNT_HOST"),
    account_id    = os.getenv("DATABRICKS_ACCOUNT_ID"),
    client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
    client_secret = os.getenv("DATABRICKS_ACCOUNT_SECRET")
)

group_list = ac.workspace_assignment.list(w.get_workspace_id())
for group in group_list:
    print(group.principal.display_name)