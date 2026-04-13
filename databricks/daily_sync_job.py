# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Tool Sync (TipQA → Ion)
# MAGIC Clones the tool-sync repo from GitHub and runs the daily synchronization.
# MAGIC Secrets are loaded from the `jae-python-credentials` scope.

# COMMAND ----------

import subprocess, os, sys

REPO_URL = "https://github.com/josenbach/tool-sync.git"
WORK_DIR = "/tmp/tool-sync"

# Clone or pull latest
if os.path.exists(WORK_DIR):
    subprocess.check_call(["git", "-C", WORK_DIR, "pull", "--ff-only"], stdout=sys.stdout, stderr=sys.stderr)
else:
    subprocess.check_call(["git", "clone", REPO_URL, WORK_DIR], stdout=sys.stdout, stderr=sys.stderr)

# Install dependencies (only what's needed beyond Databricks runtime defaults)
subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q",
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

os.chdir(WORK_DIR)
sys.path.insert(0, WORK_DIR)

# Force reimport of any cached modules from a previous run
for mod_name in list(sys.modules.keys()):
    if mod_name.startswith("utilities") or mod_name == "daily_tool_sync":
        del sys.modules[mod_name]

from daily_tool_sync import main

main()
