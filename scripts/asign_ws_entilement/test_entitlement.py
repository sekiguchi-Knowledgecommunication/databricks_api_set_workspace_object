from __future__ import annotations
from dotenv import load_dotenv

import os
import re
from pathlib import Path
from typing import Dict, Set, Tuple
from urllib.parse import urlparse

import pandas as pd
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service.iam import WorkspacePermission,ComplexValue

# ---------- 定数 -------------------------------------------------------------
# スクリプトファイルのディレクトリを基準に .env ファイルパスを組み立て
BASE_DIR = os.path.dirname(os.path.abspath(__file__))        # scripts/ の絶対パス
PROJECT_ROOT = os.path.dirname(BASE_DIR)                     # その１階層上
INPUT_PATH = os.path.join(BASE_DIR,"inputfolder/groups_entitlement.csv")
OUTPUT_PATH = os.path.join(BASE_DIR,"outputfolder/groups_entitlement_result.csv")

ENTITLEMENT_VALUES = {
    "workspace_access": "workspace-access",
    "sql_access": "databricks-sql-access",
    "cluster_create": "allow-cluster-create",
}

# クレデンシャルの取得
dotenv_path = os.path.join(PROJECT_ROOT, '.env')             # プロジェクトルート直下の .env
# ワークスペースクライアントの定義
ws = WorkspaceClient(
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

# ======== 共通ユーティリティ ====================================================
def load_expected(path: Path) -> Dict[str, Dict[str, bool]]:
    """
    CSV を読み取り、{ group_name: {perm_name: bool, ...} } 形式へ変換
    perm_name は workspace_admin / workspace_access / sql_access / cluster_create
    """
    df = pd.read_csv(path, encoding="utf-8")
    cols = ["group", "workspace_admin", "workspace_access", "sql_access", "cluster_create"]
    if set(cols) - set(df.columns):
        raise ValueError("CSV に必要な列が不足しています")
    result: Dict[str, Dict[str, bool]] = {}
    for _, row in df.iterrows():
        grp = str(row["group"]).strip()
        result[grp] = {
            "workspace_admin": str(row["workspace_admin"]).upper() == "ON",
            "workspace_access": str(row["workspace_access"]).upper() == "ON",
            "sql_access": str(row["sql_access"]).upper() == "ON",
            "cluster_create": str(row["cluster_create"]).upper() == "ON",
        }
    return result

def build_group_lookup() -> Dict[str, int]:
    """ワークスペース内グループ {display_name: id}"""
    return {g.display_name: int(g.id) for g in w.groups.list()}

# ======== 検証ロジック =========================================================
def validate() -> None:
    ws_id = ws.get_workspace_id()
    expected = load_expected(INPUT_PATH)
    grp_lookup = build_group_lookup()

    # ワークスペースの権限割当を一括取得（パフォーマンス向上）
    assignment_map = {
        pa.principal_id: set(pa.permissions)
        for pa in ac.workspace_assignment.list(workspace_id=ws_id)
    }

    all_ok = True
    print(f"対象ワークスペース ID: {ws_id}\n")

    for grp_name, exp in expected.items():
        if grp_name not in grp_lookup:
            print(f"⚠️  グループ '{grp_name}' がワークスペースに存在しない → 検証スキップ")
            continue

        gid = grp_lookup[grp_name]
        print(f"● グループ '{grp_name}' (id={gid})")

        # --- Workspace Admin 判定 ------------------------------------------------
        act_admin = WorkspacePermission.ADMIN in assignment_map.get(gid, set())
        if act_admin == exp["workspace_admin"]:
            print(f"   ✅ workspace_admin : {act_admin}")
        else:
            print(f"   ❌ workspace_admin : 期待={exp['workspace_admin']} / 実際={act_admin}")
            all_ok = False

        # --- Entitlements 判定 ---------------------------------------------------
        ent_set: Set[str] = {e.value for e in ac.groups.get(id=gid).entitlements or []}
        for key, ent_val in ENTITLEMENT_VALUES.items():
            act_flag = ent_val in ent_set
            if act_flag == exp[key]:
                print(f"   ✅ {key:<16}: {act_flag}")
            else:
                print(f"   ❌ {key:<16}: 期待={exp[key]} / 実際={act_flag}")
                all_ok = False

    print("\n====== 結果 ======")
    if all_ok:
        print("🎉 すべて一致しました (PASS)")
    else:
        print("⚠️  不一致があります (FAIL)")
        exit(1)

# ======== エントリーポイント ===================================================
if __name__ == "__main__":
    validate()