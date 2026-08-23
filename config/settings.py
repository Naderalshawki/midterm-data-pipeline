"""
Central Project Settings & Global Configuration
================================================
Midterm Data Pipeline Project (Sections 6.2 - 6.5)
"""
from pathlib import Path

# ============================================================
# 1. مسارات المشروع الأساسية (Project Paths)
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
SCREENSHOTS_DIR = REPORTS_DIR / "screenshots"

# ============================================================
# 2. ملفات الإدخال (Input / Output Files)
# ============================================================

# [الملف الأساسي النشط حالياً للـ 30 مليون عبر Spark]:
INPUT_FILE = DATA_DIR / "orders_huge_mixed_quality.csv"

# [ملف العينة الأصلي المحدد سابقاً]:
SAMPLE_FILE = DATA_DIR / "orders_sample.csv"

# [ملفات العينات الإضافية - كتعليق للرجوع لها وقت الحاجة]:
# SMALL_SAMPLE_FILE = DATA_DIR / "orders_small_sample.csv"

# ============================================================
# 3. إعدادات التوجيه التلقائي للملفات (File Router Settings)
# ============================================================
# Files <= 200MB -> Python Batch Streaming
# Files > 200MB  -> PySpark Distributed Ingestion
SMALL_FILE_THRESHOLD_MB = 200

# ============================================================
# 4. إعدادات التحميل بالدفعات (Batch Ingestion Configuration)
# ============================================================
DEFAULT_SAMPLE_ROWS = 100_000
BATCH_SIZE = 10_000

# ============================================================
# 5. إعدادات قاعدة بيانات MongoDB (Target Database Configuration)
# ============================================================
MONGO_URI = "mongodb://localhost:27017"

# [قاعدة البيانات الجديدة النشطة حالياً لتشغيل الـ 30 مليون]:
MONGO_DATABASE = "pipeline_30m_production"

# [قواعد البيانات السابقة محفوظة بالكامل كتعليق ولن تتأثر]:
# MONGO_DATABASE = "new_pipeline"
# MONGO_DATABASE = "midterm_data_pipeline"

# ============================================================
# 6. أسماء المجموعات المعيارية (Collections Schema)
# ============================================================
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"