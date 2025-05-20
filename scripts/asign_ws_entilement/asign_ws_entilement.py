
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
CSV_PATH = os.path.join(BASE_DIR,"inputfile/groups_entitlement.csv")
ENTITLEMENT_VALUES = {
    "workspace_access": "workspace-access",
    "sql_access": "databricks-sql-access",
    "cluster_create": "allow-cluster-create",
}

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

# ---------- ユーティリティ ----------------------------------------------------
def load_csv(path: Path) -> Dict[str, Set[str]]:
    """CSV => {group_name: set(権限文字列)} へ変換"""
    df = pd.read_csv(path, encoding="utf-8")
    required_cols = [
        "group",
        "workspace_admin",
        "workspace_access",
        "sql_access",
        "cluster_create",
    ]
    if set(required_cols) - set(df.columns):
        raise ValueError("CSV に必要な列が不足しています: " + ", ".join(required_cols))

    mapping: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        grp = str(row["group"]).strip()
        perms: Set[str] = set()
        if str(row["workspace_admin"]).upper() == "ON":
            perms.add("workspace_admin")
        for col in ("workspace_access", "sql_access", "cluster_create"):
            if str(row[col]).upper() == "ON":
                perms.add(col)
        mapping[grp] = perms
    return mapping


def build_ws_group_lookup() -> Dict[str, int]:
    """ワークスペース内のグループ {display_name: id} を取得"""
    return {g.display_name: int(g.id) for g in w.groups.list()}

# ================= メイン処理 ========================================

def main() -> None:
    ws_id = w.get_workspace_id()
    print(f"対象ワークスペース ID: {ws_id}\n")

    csv_perms = load_csv(CSV_PATH)
    ws_groups = build_ws_group_lookup()

    # --- 対象グループとスキップ分を振り分け --------------------------
    targets = {g: p for g, p in csv_perms.items() if g in ws_groups}
    for missing in (set(csv_perms) - set(targets)):
        print(f"⚠️  グループ '{missing}' がワークスペースに存在しないためスキップ")

    # --- 付与処理 ------------------------------------------------------
    for grp_name, perms in targets.items():
        gid = ws_groups[grp_name]
        print(f"\n▶ グループ '{grp_name}' (id={gid}) に設定する権限: {', '.join(perms) or 'なし'}")

        # 1) Workspace Admin（ON のときのみ付与、OFF には干渉しない）
        if "workspace_admin" in perms:
            ac.workspace_assignment.update(
                workspace_id=ws_id,
                principal_id=gid,
                permissions=[WorkspacePermission.ADMIN],
            )
            print("  - Workspace 管理者権限を付与")

        # 2) エンタイトルメント（ON のものをマージ付与）
        else:
            ent_values = [ENTITLEMENT_VALUES[p] for p in perms if p in ENTITLEMENT_VALUES]
            if ent_values:
                current = ac.groups.get(id=gid).entitlements or []
                merged = {e.value for e in current}.union(ent_values)
                w.groups.update(
                    id=str(gid),
                    entitlements=[ComplexValue(value=v) for v in merged],
                )
                print("  - Entitlements 更新: " + ", ".join(ent_values))

    print("\n◆ すべての処理が完了しました")


if __name__ == "__main__":
    main()
