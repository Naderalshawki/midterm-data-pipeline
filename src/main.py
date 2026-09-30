"""
Hybrid ELT Data Pipeline & Phase 2 Analytics - Ultimate One-Click Master Entrypoint
===================================================================================
One-click execution running:
1. Phase 1 (Midterm 18G): File Router -> Raw Ingestion -> 8 Quality Rules -> Quarantine & Upsert
2. Phase 2 (Final 7G)   : 3 Indexes & Explain -> 5 Queries -> 5 Aggregations -> 2 Incremental MVs -> 2 Jobs
3. Unified Web Server   : Auto-starts FastAPI Server & prints/opens http://localhost:8000 & /docs
"""
import os
import sys
import threading
import webbrowser
from pathlib import Path

# ضبط مسار المشروع تلقائياً ليعمل زر Run في VS Code أو الطرفية من أي مكان
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from config.settings import SAMPLE_FILE, INPUT_FILE
from src.file_router import route_file
from src.batch_loader import run_batch_pipeline
from src.spark_loader import run_spark_pipeline
from src.elt_pipeline import run_elt_pipeline
from src.queries_and_indexes import (
    create_indexes_and_benchmark_explain,
    list_available_queries,
    execute_named_query,
)
from src.aggregations import (
    list_available_aggregations,
    run_aggregation_report,
)
from src.materialized_views import refresh_all_materialized_views
from src.scheduler import run_job_by_name


def execute_pipeline(input_file_path: str, run_phase2: bool = True):
    """تشغيل خط البيانات الكامل (المرحلة الأولى + المرحلة الثانية) بضغطة زر واحدة"""
    input_file = Path(input_file_path)
    if not input_file.is_absolute():
        input_file = PROJECT_ROOT / input_file

    if not input_file.exists():
        print(f"\n[-] خطأ: لم يتم العثور على الملف في المسار: {input_file}")
        return None

    print("\n" + "=" * 85)
    print("🚀 STARTING FULL HYBRID DATA PIPELINE (PHASE 1 MIDTERM + PHASE 2 FINAL)")
    print("=" * 85)

    # =========================================================================
    # PHASE 1 - STAGE 1: Smart File Routing & Raw Ingestion (Sections 6.2 - 6.5)
    # =========================================================================
    engine = route_file(str(input_file))
    if engine == "python_batch":
        run_id = run_batch_pipeline(str(input_file))
    else:
        run_id = run_spark_pipeline(str(input_file))

    # =========================================================================
    # PHASE 1 - STAGE 2: 8 Cleaning Rules, Quarantine & Idempotent Upsert
    # =========================================================================
    print("\n" + "=" * 85)
    print("🧹 STAGE 2: ELT CLEANING, VALIDATION, QUARANTINE & IDEMPOTENT UPSERT")
    print("=" * 85)
    run_elt_pipeline(target_run_id=run_id)

    if not run_phase2:
        return run_id

    # =========================================================================
    # PHASE 2 - REQUIREMENT 1: 3 Indexes, Explain Benchmark & 5 Queries
    # =========================================================================
    print("\n" + "=" * 85)
    print("⚡ STAGE 3 (PHASE 2): BUILDING 3 INDEXES, RUNNING EXPLAIN & 5 QUERIES")
    print("=" * 85)
    explain_report = create_indexes_and_benchmark_explain()
    for comp in explain_report["explain_comparisons"]:
        print(f" [Explain] {comp['query_name']}: {comp['impact_summary']}")

    for q in list_available_queries():
        q_res = execute_named_query(q["name"], limit=5)
        print(f" [Query OK] {q['name']} -> Returned {q_res['count_returned']} records")

    # =========================================================================
    # PHASE 2 - REQUIREMENT 2: 5 MongoDB Aggregation Reports
    # =========================================================================
    print("\n" + "=" * 85)
    print("📊 STAGE 4 (PHASE 2): EXECUTING 5 ANALYTICAL AGGREGATION REPORTS")
    print("=" * 85)
    for agg in list_available_aggregations():
        agg_res = run_aggregation_report(agg["name"], limit=5)
        print(f" [Aggregation OK] {agg['name']} -> Returned {agg_res['count_returned']} summary rows")

    # =========================================================================
    # PHASE 2 - REQUIREMENT 3: 2 Materialized Views with Incremental Refresh
    # =========================================================================
    print("\n" + "=" * 85)
    print("🔄 STAGE 5 (PHASE 2): INCREMENTAL REFRESH OF 2 MATERIALIZED VIEWS")
    print("=" * 85)
    mv_res = refresh_all_materialized_views(force_full=False)
    for view_name, view_data in mv_res["views"].items():
        m = view_data["metrics"]
        print(
            f" [Materialized View OK] {view_name} | Mode: {m['refresh_mode']} "
            f"| Delta Processed: {m['delta_records_processed']} | Total Rows: {m['total_view_documents']}"
        )

    # =========================================================================
    # PHASE 2 - REQUIREMENT 4: 2 Scheduled Jobs Execution & Audit Logging
    # =========================================================================
    print("\n" + "=" * 85)
    print("⏱️ STAGE 6 (PHASE 2): RUNNING 2 SCHEDULED JOBS & SAVING AUDIT LOGS")
    print("=" * 85)
    job1 = run_job_by_name("refresh_materialized_views_job")
    print(f" [Job 1 OK] {job1['job_name']} -> Status: {job1['status']} ({job1['duration_seconds']}s)")

    job2 = run_job_by_name("generate_periodic_report_job")
    print(f" [Job 2 OK] {job2['job_name']} -> Status: {job2['status']} ({job2['duration_seconds']}s)")

    print("\n" + "=" * 85)
    print("✅ ALL PHASE 1 & PHASE 2 PIPELINE STAGES COMPLETED SUCCESSFULLY!")
    print("=" * 85)

    return run_id


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    no_server = "--no-server" in sys.argv

    if args:
        target_file = args[0]
    elif SAMPLE_FILE.exists():
        target_file = str(SAMPLE_FILE)
    elif INPUT_FILE.exists():
        target_file = str(INPUT_FILE)
    else:
        target_file = "data/orders_sample.csv"

    # 1. تشغيل المراحل الـ 6 كاملة (النصفي + النهائي)
    execute_pipeline(target_file, run_phase2=True)

    if no_server:
        return

    # 2. تشغيل سيرفر FastAPI الموحد وطباعة الروابط المباشرة وفتح المتصفح تلقائياً
    port = int(os.getenv("API_PORT", 8000))
    dashboard_url = f"http://localhost:{port}/"
    swagger_url = f"http://localhost:{port}/docs"
    health_url = f"http://localhost:{port}/health"

    print("\n" + "╔" + "═" * 83 + "╗")
    print("║ 🌐 UNIFIED FASTAPI SERVER & WEB COMMAND STUDIO IS STARTING NOW...                 ║")
    print("╠" + "═" * 83 + "╣")
    print(f"║ 🚀 Web Command Studio (Dashboard) : {dashboard_url:<46}║")
    print(f"║ 📘 Official Swagger UI (/docs)    : {swagger_url:<46}║")
    print(f"║ ❤️ System Health Check (/health)  : {health_url:<46}║")
    print("║ 💡 اضغط Ctrl + Click على أي رابط أعلاه (أو سيفتح المتصفح تلقائياً الآن)           ║")
    print("║ 🛑 لإيقاف السيرفر في أي وقت اضغط : Ctrl + C                                       ║")
    print("╚" + "═" * 83 + "╝\n")

    # فتح المتصفح تلقائياً بعد ثانية ونصف من بدء السيرفر
    threading.Timer(1.5, lambda: webbrowser.open(dashboard_url)).start()

    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    main()