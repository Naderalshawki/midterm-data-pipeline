"""
Hybrid ELT Data Pipeline - Single Master Entrypoint
Direct one-click execution supporting automatic routing & database execution.
Fully compliant with official midterm requirements (Sections 6.2 to 6.11).
"""
import os
import sys
from pathlib import Path

# ضبط مسار المشروع تلقائياً ليعمل زر Run من أي مكان
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from config.settings import INPUT_FILE, SAMPLE_FILE
from src.file_router import route_file
from src.batch_loader import run_batch_pipeline
from src.spark_loader import run_spark_pipeline
from src.elt_pipeline import run_elt_pipeline


def execute_pipeline(input_file_path: str):
    input_file = Path(input_file_path)
    if not input_file.exists():
        print(f"\n[-] خطأ: لم يتم العثور على الملف في المسار: {input_file}")
        return

    print("\n" + "=" * 80)
    print("STARTING HYBRID DATA PIPELINE (END-TO-END EXECUTION)")
    print("=" * 80)

    # 1. المرحلة الأولى: توجيه واختيار المحرك تلقائياً وتحميل البيانات الخام (Sections 6.2 - 6.5)
    engine = route_file(str(input_file))
    
    if engine == "python_batch":
        run_id = run_batch_pipeline(str(input_file))
    else:
        run_id = run_spark_pipeline(str(input_file))

    # 2. المرحلة الثانية: التنظيف، العزل، والـ Idempotent Upsert (Sections 6.6 - 6.11)
    print("\n" + "=" * 80)
    print("TRIGGERING STAGE 2: ELT CLEANING, VALIDATION & IDEMPOTENT UPSERT")
    print("=" * 80)
    run_elt_pipeline(target_run_id=run_id)

    print("\n" + "=" * 80)
    print("ENTIRE PIPELINE EXECUTION FINISHED SUCCESSFULLY!")
    print("=" * 80)


def main():
    # الأولوية: 1. وسيط الطرفية إن وُجد | 2. ملف الـ 30 مليون المعرف في settings.py | 3. ملف العينة
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
    elif INPUT_FILE.exists():
        target_file = str(INPUT_FILE)
    elif SAMPLE_FILE.exists():
        target_file = str(SAMPLE_FILE)
    else:
        target_file = "data/orders_small_sample.csv"

    execute_pipeline(target_file)


if __name__ == "__main__":
    main()