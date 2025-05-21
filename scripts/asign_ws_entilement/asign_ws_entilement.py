from __future__ import annotations
from dotenv import load_dotenv
import os
from pathlib import Path
from typing import Set, Dict

import pandas as pd
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service.iam import WorkspacePermission, ComplexValue

# ────────────────────────────────────────────────────────────────────────────
# 外部モジュール（検証&エクスポート）
# ────────────────────────────────────────────────────────────────────────────
from entitlement_validator import (
    export_workspace_entitlements,
    validate_entitlements,
)

# ---------- 定数 -------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
CSV_PATH   = BASE_DIR / "inputfolder" / "groups_entitlement.csv"

ENTITLEMENT_VALUES = {
    "workspace_access": "workspace-access",
    "sql_access": "databricks-sql-access",
    "cluster_create": "allow-cluster-create",
}

# ---------- クライアント初期化 ----------------------------------------------
load_dotenv(PROJECT_ROOT / ".env")

ws = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    client_id=os.getenv("DATABRICKS_CLIENT_ID"),
    client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
)
ac = AccountClient(
    host=os.getenv("DATABRICKS_ACCOUNT_HOST"),
    account_id=os.getenv("DATABRICKS_ACCOUNT_ID"),
    client_id=os.getenv("DATABRICKS_CLIENT_ID"),
    client_secret=os.getenv("DATABRICKS_ACCOUNT_SECRET"),
)

# ---------- ヘルパ ------------------------------------------------------------
def load_csv(path: Path) -> Dict[str, Set[str]]:
    """CSV → {group: {"workspace_admin", "workspace_access", ...}}"""
    df = pd.read_csv(path, encoding="utf-8")
    required = ["group", "workspace_admin", *ENTITLEMENT_VALUES.keys()]
    if set(required) - set(df.columns):
        raise ValueError("CSV に必要な列が不足しています: " + ", ".join(required))

    result: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        grp = str(row["group"]).strip()
        flags = {
            col for col in required[1:]
            if str(row[col]).upper() == "ON"
        }
        result[grp] = flags
    return result


def build_ws_group_lookup() -> Dict[str, int]:
    """{display_name: id}"""
    return {g.display_name: int(g.id) for g in ws.groups.list() if g.display_name not in ["users","admins"]}

# ────────────────────────────────────────────────────────────────────────────
# ① アップデート関数（既存ロジックを関数化）
# ────────────────────────────────────────────────────────────────────────────
def update_entitlements_from_csv(csv_path: Path) -> None:
    ws_id = ws.get_workspace_id()
    csv_perms = load_csv(csv_path)
    ws_groups = build_ws_group_lookup()

    targets = {g: p for g, p in csv_perms.items() if g in ws_groups}
    for missing in (set(csv_perms) - targets.keys()):
        print(f"⚠️  '{missing}' がワークスペースに無いのでスキップ")

    for grp_name, perms in targets.items():
        gid = ws_groups[grp_name]
        print(f"\n▶ {grp_name} へ適用: {', '.join(sorted(perms)) or '無し'}")

        # entitlement更新
        ent_values = [
                ENTITLEMENT_VALUES[p] for p in perms & ENTITLEMENT_VALUES.keys()
        ]
        current = ac.groups.get(id=gid).entitlements or []
        merged = {e.value for e in current}.union(ent_values)
        ws.groups.update(
            id=str(gid),
            entitlements=[ComplexValue(value=v) for v in merged],
        )
        print("  - Entitlements 更新:", ", ".join(ent_values))

        # Workspace Admin
        if "workspace_admin" in perms:
            ac.workspace_assignment.update(
                workspace_id=ws_id,
                principal_id=gid,
                permissions=[WorkspacePermission.ADMIN],
            )
            print("  - Workspace ADMIN 付与")
                # その他エンタイトルメント
# ────────────────────────────────────────────────────────────────────────────
# ② ③ エクスポート → 検証 → 終了
# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. CSV に基づきアップデート
    update_entitlements_from_csv(CSV_PATH)

    # 2. アップデート後の状態を CSV に保存
    export_workspace_entitlements(ac,ws)  # default path: outputfolder/workspace_entitlements.csv

    # 3. 入力 CSV とワークスペースを検証（差分あれば SystemExit）
    validate_entitlements(CSV_PATH, ws)

    print("\n✅ すべて完了しました")
