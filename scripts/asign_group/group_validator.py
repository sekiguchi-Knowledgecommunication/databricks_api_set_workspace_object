"""
group_validator.py
==================
Utility helpers for Databricks Workspace group operations.

Functions
---------
validate_groups(csv_path, ws_client, *, strict=True)
    Compare the group_physical_name column in a CSV with the display names of
    groups already present in the workspace. Prints a diff. If ``strict`` is
    True (default) and any diff exists, the function raises ``SystemExit`` so
    that calling scripts can fail-fast in CI / production jobs.

export_workspace_groups(ws_client, output_path="outputfolder/workspace_groups.csv")
    Export **all** current workspace group display names to a single‑column CSV
    file (header ``group_physical_name``). The parent directory is created if
    missing and existing files are silently overwritten.

Both helpers normalise strings using :func:`_normalize` so that trivial
whitespace or case differences do not trigger mismatches.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Set

import pandas as pd
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# internal helpers
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    """Return *name* stripped and lower‑cased for insensitive comparison."""
    return name.strip().lower()


def _load_csv_groups(csv_path: Path | str) -> List[str]:
    """Read *csv_path* and return a **normalised** list of group names.

    The CSV must contain a column named ``group_physical_name``.
    """
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path, encoding="utf-8")

    if "group_physical_name" not in df.columns:
        raise ValueError("CSV に 'group_physical_name' 列がありません。")

    return [_normalize(n) for n in df["group_physical_name"].dropna().tolist()]


def _workspace_group_names(ws: WorkspaceClient) -> List[str]:
    """Return a **normalised** list of group display names in *ws*."""
    return [_normalize(g.display_name) for g in ws.groups.list() if not g.display_name in ["users","admins"]]

# ---------------------------------------------------------------------------
# public helpers
# ---------------------------------------------------------------------------

def validate_groups(
    csv_path: Path | str,
    ws: WorkspaceClient,
    *,
    strict: bool = True,
) -> bool:
    """Validate that the CSV and workspace contain **exactly** the same groups.

    Parameters
    ----------
    csv_path
        Path to a CSV containing a ``group_physical_name`` column.
    ws
        An authenticated :class:`databricks.sdk.WorkspaceClient` instance.
    strict
        If *True*, raise :class:`SystemExit` when any difference is found.

    Returns
    -------
    bool
        ``True`` if the sets match, else ``False`` (unless *strict* raises).
    """

    csv_set: Set[str] = set(_load_csv_groups(csv_path))
    ws_set: Set[str] = set(_workspace_group_names(ws))

    if csv_set == ws_set:
        print("✅ CSV とワークスペースのグループ名は完全一致しています。")
        return True

    # calculate differences
    only_csv = sorted(csv_set - ws_set)
    only_ws = sorted(ws_set - csv_set)

    print("❌ グループ名が一致しません。差分を表示します。")
    if only_csv:
        print("  - CSV にのみ存在:", ", ".join(only_csv))
    if only_ws:
        print("  - ワークスペースにのみ存在:", ", ".join(only_ws))

    if strict:
        raise SystemExit("検証失敗: グループ名が一致しません。")
    return False


def export_workspace_groups(
    ws: WorkspaceClient,
    output_path: Path | str = Path(__file__).resolve().parent / "outputfolder" / "result_workspace_groups.csv",
) -> None:
    """Export the current workspace's group names to *output_path*.

    The CSV will have a single column ``group_physical_name``. Any existing
    file is **overwritten**. Parent directories are created automatically.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    # デフォルトグループを除く
    group_names = sorted(g.display_name for g in ws.groups.list() if not g.display_name in ["users","admins"])

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["group_physical_name"])
        writer.writerows([[n] for n in group_names])

    print(
        f"📄 ワークスペースのグループ {len(group_names)} 件を {output_path} にエクスポートしました。"
    )

