import time
from dotenv import load_dotenv
import os

from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service import settings
import csv
from pathlib import Path


load_dotenv()
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

groups = a.groups.list()
for group in groups:
    if group.id ==  "32514441361772":
        print(group)
