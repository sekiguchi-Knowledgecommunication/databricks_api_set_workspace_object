
from dotenv import load_dotenv
import os
from pathlib import Path
from typing import Dict, List
import pandas as pd

from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service.iam import WorkspacePermission
from databricks.sdk.service.iam import ComplexValue

# スクリプトファイルのディレクトリを基準に .env ファイルパスを組み立て
BASE_DIR = os.path.dirname(os.path.abspath(__file__))        # scripts/ の絶対パス
PROJECT_ROOT = os.path.dirname(BASE_DIR)                     # その１階層上
csv_path = os.path.join(BASE_DIR,"inputfile/groups_entitlement.csv")
dotenv_path = os.path.join(PROJECT_ROOT, '.env')             # プロジェクトルート直下の .env

print(f"""
    {csv_path}
    """)
# ワークスペースクライアントの定義
w = WorkspaceClient(
  host          = os.getenv("DATABRICKS_HOST"),
  client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
  client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
)
a = AccountClient(
    host          = os.getenv("DATABRICKS_ACCOUNT_HOST"),
    account_id    = os.getenv("DATABRICKS_ACCOUNT_ID"),
    client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
    client_secret = os.getenv("DATABRICKS_ACCOUNT_SECRET")
)
CSV_PATH = "./groups_entilement.csv"             # ← 適宜変更（UTF-8 想定）
workspace_id = w.get_workspace_id()
# print(workspace_id)

# def load_target_groups(csv_path: Path) -> List[str]:
#     """CSV から group_physical_name 列を読み取り、ユニークなリストを返す。"""
#     df = pd.read_csv(csv_path, encoding="utf-8")
#     required_cols = {"group_logical_name", "group_physical_name"}
#     # 必須列が存在するかをチェック
#     if required_cols - set(df.columns):
#         raise ValueError(f"CSV に必須列 {required_cols} がありません。")
#     # NaN を除外し物理名のみ返却
#     return df["group_physical_name"].dropna().unique().tolist()

# def assign_groups_entilement(csv_path: Path) -> None:
#     """
#     CSV の物理グループ名に一致するアカウントグループをワークスペースへ USER 権限で追加する。
#     既に追加済み、または存在しないグループはスキップする。
#     """

#     targets = load_target_groups(csv_path)
#     lookup = build_group_lookup(a)
#     already = current_workspace_group_ids(a)

#     print(f"★ CSV に定義された対象グループ数: {len(targets)}")
#     added, skipped = 0, 0

#     for physical_name in targets:
#         gid = lookup.get(physical_name)

#         # グループがアカウントに存在しない場合
#         if gid is None:
#             print(f"⚠️  グループ '{physical_name}' がアカウントに存在しないためスキップ")
#             skipped += 1
#             continue

#         # 既にワークスペースに割り当て済みの場合
#         if gid in already:
#             print(f"ℹ️  グループ '{physical_name}' は既にワークスペースに割り当て済み - スキップ")
#             skipped += 1
#             continue

#         # WORKSPACE へ USER 権限で割り当て
        
#         a.workspace_assignment.update(
#             workspace_id=workspace_id,
#             principal_id=gid,
#             permissions=[WorkspacePermission.USER],
#         )
#         print(f"✅  グループ '{physical_name}' をワークスペース {workspace_id} に追加しました")
#         added += 1

#     print(f"\n◆ 処理結果: 追加 {added} / スキップ {skipped}")

# ---------- エントリーポイント ----------------------------------------------

if __name__ == "__main__":
    status= a.groups.update(
        id="32514441361772",
        entitlements=[ 
            ComplexValue(
                value='workspace-access'
            ),
            ComplexValue(
                value='databricks-sql-access'
            )
        ]
    )
    print(status)

#   a.workspace_assignment.update(
#         workspace_id=WORKSPACE_ID,
#         principal_id=int(grp.id),
#         permissions=[iam.WorkspacePermission.ADMIN],  # ← ここを USER にすれば一般権限
#     )
