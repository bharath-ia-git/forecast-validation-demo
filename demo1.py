from pathlib import Path
from datetime import datetime
import sys

# -------------------- EDIT THESE --------------------
GBQ_PROJECT_ID = "balsambrands-data-scan-2022025"
GBQ_QUERY = """
SELECT *
FROM `balsambrands-20022025.balsam_ingestion_dev.fdm_temp1`
"""
# ----------------------------------------------------

def log(msg): print(f"[INFO] {msg}")

# --- Deps ---
try:
    import pandas as pd, numpy as np
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pandas", "numpy"])
    import pandas as pd, numpy as np

# GBQ
try:
    import pandas_gbq
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pandas-gbq", "google-cloud-bigquery"])
    import pandas_gbq

# Writers
try:
    import xlsxwriter
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "XlsxWriter"])
    import xlsxwriter

# Ensure openpyxl exists
try:
    import openpyxl  # noqa
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "openpyxl"])
    import openpyxl  # noqa

# --- IST date for filename ---
try:
    from zoneinfo import ZoneInfo
    ist = ZoneInfo("Asia/Kolkata")
except Exception:
    ist = None

now_ist = datetime.now(ist) if ist else datetime.now()
today_str = now_ist.strftime("%d %b %Y")
OUT_PATH = Path(f"./Forecast_Validation_demo - {today_str}.xlsx")

# --- Pull GBQ data ---
log("Running GBQ query… (browser login will pop once)")
df = pandas_gbq.read_gbq(
    GBQ_QUERY,
    project_id=GBQ_PROJECT_ID,
    dialect="standard",
    location="US"
)


log(f"GBQ rows fetched: {len(df):,}; cols: {len(df.columns)}")

# --- Write Excel ---
with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Forecast_Validation", index=False)

log(f"Report written to: {OUT_PATH}")
