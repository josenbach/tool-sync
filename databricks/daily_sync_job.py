# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Tool Sync (TipQA -> Ion)
# MAGIC Clones the tool-sync repo from GitHub and runs the daily synchronization.
# MAGIC Secrets are loaded from the `jae-python-credentials` scope.

# COMMAND ----------

import subprocess, os, sys, time

REPO_URL = "https://github.com/josenbach/tool-sync.git"
WORK_DIR = f"/tmp/tool-sync-{int(time.time())}"

print(f"Cloning repo to {WORK_DIR}...")
subprocess.check_call(
    ["git", "clone", "--depth", "1", REPO_URL, WORK_DIR],
    stdout=sys.stdout, stderr=sys.stderr,
)

# Install databricks-sql-connector using the cluster library path so it
# doesn't conflict with the runtime's own 'databricks' namespace.
subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "-t", f"{WORK_DIR}/.lib",
     "databricks-sql-connector", "python-dotenv"],
    stdout=sys.stdout, stderr=sys.stderr,
)

# COMMAND ----------

# Load secrets and set environment variables
os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get("jae-python-credentials", "databricks-token")
os.environ["V2CLIENT"] = dbutils.secrets.get("jae-python-credentials", "ion-v2-client")
os.environ["V2SECRET"] = dbutils.secrets.get("jae-python-credentials", "ion-v2-secret")
os.environ["ENVIRONMENT"] = "v2_production"

# COMMAND ----------

# Set up paths: put our local lib dir first so databricks-sql-connector
# takes precedence over the runtime's 'databricks' package
lib_dir = f"{WORK_DIR}/.lib"
sys.path.insert(0, lib_dir)
sys.path.insert(0, WORK_DIR)
os.chdir(WORK_DIR)

# Evict the runtime's cached 'databricks' namespace so our
# databricks-sql-connector's 'databricks.sql' can be found
for mod_name in list(sys.modules.keys()):
    if mod_name.startswith(("databricks", "utilities", "daily_tool_sync")):
        del sys.modules[mod_name]

# Import from .lib and pin the module references — importing daily_tool_sync
# triggers transitive imports that can cause the runtime to re-cache its own
# 'databricks' package, overwriting ours.
from databricks import sql as _sql_check
print(f"databricks.sql loaded from: {_sql_check.__file__}")

_pinned = {k: v for k, v in sys.modules.items() if k.startswith("databricks")}

import json, traceback
try:
    from daily_tool_sync import main

    # Restore our databricks-sql-connector modules in case they were overwritten
    sys.modules.update(_pinned)

    stats = main() or {}

    summary = {
        "status": "SUCCESS",
        "total_tools": stats.get("total_tools", 0),
        "created": stats.get("created", 0),
        "updated": stats.get("updated", 0),
        "converted": stats.get("converted", 0),
        "marked_unavailable": stats.get("marked_unavailable", 0),
        "marked_available": stats.get("marked_available", 0),
        "update_then_mark_unavailable": stats.get("update_then_mark_unavailable", 0),
        "skipped": stats.get("skipped", 0),
        "errors": stats.get("errors", 0),
    }
    if stats.get("error_details"):
        summary["error_details"] = [
            {
                "serial_number": e.get("serial_number"),
                "action": e.get("action"),
                "reason": e.get("reason"),
                "error": e.get("error"),
            }
            for e in stats["error_details"][:50]
        ]
    dbutils.notebook.exit(json.dumps(summary))
except SystemExit as e:
    if e.code != 0:
        traceback.print_exc()
        raise Exception(f"daily_tool_sync exited with code {e.code}")
except Exception:
    traceback.print_exc()
    raise
