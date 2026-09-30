"""
Central Project Settings & Global Configuration
================================================
Midterm & Final Data Pipeline Project (Phase 1 & Phase 2)
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 1. مسارات المجلدات في المشروع (Project Paths)
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
SCREENSHOTS_DIR = PROJECT_ROOT / "screenshots"

REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 2. ملفات الإدخال (INPUT & SAMPLE FILES)
# ============================================================
_env_input = os.getenv("INPUT_FILE", "orders_sample.csv")
if _env_input.startswith("data/") or _env_input.startswith("data\\"):
    INPUT_FILE = PROJECT_ROOT / _env_input
else:
    INPUT_FILE = DATA_DIR / _env_input

SAMPLE_FILE = DATA_DIR / "orders_sample.csv"
if not SAMPLE_FILE.exists():
    SAMPLE_FILE = DATA_DIR / "orders_small_sample.csv"

# ============================================================
# 3. إعدادات التوجيه الذكي والدفعات (Router & Batch Settings)
# ============================================================
SMALL_FILE_THRESHOLD_MB = int(os.getenv("SMALL_FILE_THRESHOLD_MB", 200))
DEFAULT_SAMPLE_ROWS = int(os.getenv("DEFAULT_SAMPLE_ROWS", 100_000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10_000))

# ============================================================
# 4. إعدادات قاعدة البيانات MongoDB (Database Settings)
# ملاحظة: قاعدة pipeline_30m_production محفوظة للمشروع النصفي،
# وقاعدة hybrid_pipeline_db مخصصة لاختبارات الـ API السريعة والمناقشة.
# ============================================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "hybrid_pipeline_db")

# ============================================================
# 5. أسماء المجموعات المعيارية - المرحلة الأولى (Phase 1 Collections)
# ============================================================
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# ============================================================
# 6. أسماء مجموعات المرحلة الثانية (Phase 2: MVs & Jobs Collections)
# ============================================================
MV_DAILY_SALES = "daily_sales_summary"
MV_TOP_PRODUCTS = "top_products_summary"
MV_WATERMARKS = "mv_watermarks"
JOBS_LOG_COLLECTION = "job_execution_logs"