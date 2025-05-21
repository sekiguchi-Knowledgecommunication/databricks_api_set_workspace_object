"""
entitlement_validator.py
========================
Compare and export Databricks *group entitlements* against a control CSV.

Public helpers
--------------
export_workspace_entitlements(ws_client, output_path="outputfolder/workspace_entitlements.csv")
    Dump current workspace entitlements to a CSV compatible with the loader
    script (columns: group, workspace_access, sql_access, cluster_create).

validate_entitlements(csv_path, ws_client, *, strict=True)
    Compare the CSV definition and the actual workspace entitlements. Prints a
    diff. If *strict* is True (default) and any mismatch exists, raises
    SystemExit so CI / batch jobs can fail fast.

Assumptions
-----------
* The workspace admin permission (WorkspacePermission.ADMIN) is managed
  separately in the caller script. This module validates **entitlements only**
  (workspace-access, databricks-sql-access, allow-cluster-create).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
from databricks.sdk import WorkspaceClient,AccountClient
from databricks.sdk.service.iam import WorkspacePermission, ComplexValue
# ---------------------------------------------------------------------------
# Constants – mirror main script definitions so this module is standalone
# ---------------------------------------------------------------------------
ENTITLEMENT_VALUES: Dict[str, str] = {
    "workspace_access": "workspace-access",
    "sql_access": "databricks-sql-access",
    "cluster_create": "allow-cluster-create",
}
# reverse lookup for convenience: value -> key
_KEY_BY_VALUE = {v: k for k, v in ENTITLEMENT_VALUES.items()}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    return name.strip().lower()


def _load_csv(csv_path: Path | str) -> Dict[str, Set[str]]:
    """Convert CSV to {group: set(alias)} using ON/OFF flags."""
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path, encoding="utf-8")

    required_cols = ["group", *ENTITLEMENT_VALUES.keys()]
    if set(required_cols) - set(df.columns):
        raise ValueError(f"CSV に必要な列が不足しています: {', '.join(required_cols)}")

    mapping: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        grp = _normalize(str(row["group"]))
        ent: Set[str] = {
            col for col in ENTITLEMENT_VALUES.keys() if str(row[col]).upper() == "ON"
        }
        mapping[grp] = ent
    return mapping


def _workspace_entitlements(ws: WorkspaceClient) -> Dict[str, Set[str]]:
    """Return {normalized group name: set(alias)} for the workspace."""
    result: Dict[str, Set[str]] = {}
    ws_list = [g for g in ws.groups.list() if g.display_name not in ["users","admins"]]
    for g in ws_list:
        alias_set: Set[str] = set()
        for e in (g.entitlements or []):
            alias = _KEY_BY_VALUE.get(e.value)
            if alias:
                alias_set.add(alias)
        result[_normalize(g.display_name)] = alias_set
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def export_workspace_entitlements(
    ac: AccountClient,
    ws: WorkspaceClient,
    output_path: Path | str = Path(__file__).resolve().parent / "outputfolder" / "result_groups_entitlements.csv",
) -> None:
    """Export workspace entitlements to a CSV compatible with loader script."""

    output_path = Path(output_path)
    output_path.parent.mkdir(exist_ok=True, parents=True)

    rows: List[List[str]] = []
    ws_list = [g for g in ws.groups.list() if g.display_name not in ["users","admins"]]
    for g in ws_list:
        ent_aliases = {
            _KEY_BY_VALUE.get(e.value) for e in (g.entitlements or [])
        }
        row = [g.display_name]
        admin_flag = "OFF"
        for group in ac.workspace_assignment.list(ws.get_workspace_id()):
            if g.id == group.principal.principal_id:
                if g.permisssions == [WorkspacePermission.ADMIN]:
                    admin_flag = "ON"
        row.append(admin_flag)
        for col in ENTITLEMENT_VALUES.keys():
            row.append("ON" if col in ent_aliases else "OFF")
        rows.append(row)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["group", "workspace_admin",*ENTITLEMENT_VALUES.keys()])
        writer.writerows(rows)

    print(f"📄 ワークスペースのエンタイトルメント {len(rows)} 件を {output_path} にエクスポートしました。")


def validate_entitlements(
    csv_path: Path | str,
    ws: WorkspaceClient,
    *,
    strict: bool = True,
) -> bool:
    """Compare CSV vs workspace entitlements.

    Returns True if identical, else False. If *strict* is True, mismatches raise
    SystemExit to halt callers.
    """
    csv_map = _load_csv(csv_path)
    ws_map = _workspace_entitlements(ws)

    all_groups = csv_map.keys() | ws_map.keys()
    diffs: List[str] = []

    for grp in sorted(all_groups):
        csv_set = csv_map.get(grp, set())
        ws_set = ws_map.get(grp, set())
        if csv_set != ws_set:
            diffs.append(grp)
            print(f"❌ グループ '{grp}' が一致しません")
            missing = csv_set - ws_set
            extra = ws_set - csv_set
            if missing:
                print("  - CSV にのみ ON:", ", ".join(sorted(missing)))
            if extra:
                print("  - ワークスペースにのみ ON:", ", ".join(sorted(extra)))

    if not diffs:
        print("✅ CSV とワークスペースのエンタイトルメントは完全一致しています。")
        return True

    if strict:
        raise SystemExit("検証失敗: 一致しないエンタイトルメントが存在します。")
    return False