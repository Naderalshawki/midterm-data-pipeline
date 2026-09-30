"""
Phase 2 - Requirement 4: Scheduled Jobs & Execution Audit Logger
================================================================
Implements 2 Scheduled Jobs using APScheduler:
1. refresh_materialized_views_job : Incrementally refreshes Materialized Views
2. generate_periodic_report_job   : Generates periodic aggregation summary report

Records execution start_time, end_time, duration, status (SUCCESS/FAILED), and result.
"""
import json
from datetime import datetime, timezone
from pymongo import MongoClient, DESCENDING
from apscheduler.schedulers.background import BackgroundScheduler
from config import settings
from src.materialized_views import refresh_all_materialized_views
from src.aggregations import run_aggregation_report

_scheduler = None


def _log_job_execution(job_name: str, trigger_type: str, start_dt: datetime, end_dt: datetime, status: str, details: dict):
    """تسجيل نتيجة تنفيذ المهمة في MongoDB وفي ملف JSON للتوثيق"""
    duration_sec = round((end_dt - start_dt).total_seconds(), 3)
    log_entry = {
        "job_name": job_name,
        "trigger_type": trigger_type,
        "start_time": start_dt.isoformat(),
        "end_time": end_dt.isoformat(),
        "duration_seconds": duration_sec,
        "status": status,
        "details": details
    }

    client = MongoClient(settings.MONGO_URI)
    try:
        db = client[settings.MONGO_DATABASE]
        db[settings.JOBS_LOG_COLLECTION].insert_one(dict(log_entry))
    finally:
        client.close()

    # حفظ آخر السجلات في ملف reports/jobs_execution_history.json
    history_file = settings.REPORTS_DIR / "jobs_execution_history.json"
    history = []
    if history_file.exists():
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = []

    history.insert(0, log_entry)
    history = history[:50]
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    return log_entry


def job_refresh_materialized_views(trigger_type: str = "scheduled") -> dict:
    """المهمة المجدولة الأولى: تحديث العروض المادية تزايدياً"""
    start_dt = datetime.now(timezone.utc)
    try:
        res = refresh_all_materialized_views(force_full=False)
        end_dt = datetime.now(timezone.utc)
        summary = {
            "daily_sales_metrics": res["views"][settings.MV_DAILY_SALES]["metrics"],
            "top_products_metrics": res["views"][settings.MV_TOP_PRODUCTS]["metrics"]
        }
        return _log_job_execution(
            job_name="refresh_materialized_views_job",
            trigger_type=trigger_type,
            start_dt=start_dt,
            end_dt=end_dt,
            status="SUCCESS",
            details=summary
        )
    except Exception as exc:
        end_dt = datetime.now(timezone.utc)
        return _log_job_execution(
            job_name="refresh_materialized_views_job",
            trigger_type=trigger_type,
            start_dt=start_dt,
            end_dt=end_dt,
            status="FAILED",
            details={"error": str(exc)}
        )


def job_generate_periodic_report(trigger_type: str = "scheduled") -> dict:
    """المهمة المجدولة الثانية: إنشاء تقرير دوري للتجميعات وحفظه"""
    start_dt = datetime.now(timezone.utc)
    try:
        city_sales = run_aggregation_report("sales_by_city", limit=5)
        status_dist = run_aggregation_report("orders_by_status", limit=10)
        top_prods = run_aggregation_report("top_products", limit=5)

        report_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "top_cities_by_sales": city_sales["results"],
            "orders_by_status": status_dist["results"],
            "top_selling_products": top_prods["results"]
        }

        report_file = settings.REPORTS_DIR / "periodic_analytics_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report_payload, f, ensure_ascii=False, indent=2)

        end_dt = datetime.now(timezone.utc)
        return _log_job_execution(
            job_name="generate_periodic_report_job",
            trigger_type=trigger_type,
            start_dt=start_dt,
            end_dt=end_dt,
            status="SUCCESS",
            details={
                "report_file": str(report_file),
                "cities_analyzed": len(city_sales["results"]),
                "statuses_analyzed": len(status_dist["results"]),
                "top_products_count": len(top_prods["results"])
            }
        )
    except Exception as exc:
        end_dt = datetime.now(timezone.utc)
        return _log_job_execution(
            job_name="generate_periodic_report_job",
            trigger_type=trigger_type,
            start_dt=start_dt,
            end_dt=end_dt,
            status="FAILED",
            details={"error": str(exc)}
        )


JOB_REGISTRY = {
    "refresh_materialized_views_job": {
        "name": "refresh_materialized_views_job",
        "title": "1. مهمة التحديث التزايدي للعروض المادية (Incremental MV Refresh)",
        "schedule": "Every 15 minutes (cron/interval)",
        "interval_minutes": 15,
        "handler": job_refresh_materialized_views
    },
    "generate_periodic_report_job": {
        "name": "generate_periodic_report_job",
        "title": "2. مهمة توليد التقرير التحليلي الدوري (Periodic Analytics Report)",
        "schedule": "Every 30 minutes (cron/interval)",
        "interval_minutes": 30,
        "handler": job_generate_periodic_report
    }
}


def start_background_scheduler():
    """تشغيل المجدول الزمني في الخلفية عند بدء تشغيل الـ API"""
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    _scheduler = BackgroundScheduler(timezone="UTC")
    for job_id, meta in JOB_REGISTRY.items():
        _scheduler.add_job(
            meta["handler"],
            trigger="interval",
            minutes=meta["interval_minutes"],
            id=job_id,
            replace_existing=True,
            kwargs={"trigger_type": "scheduled"}
        )
    _scheduler.start()
    return _scheduler


def stop_background_scheduler():
    """إيقاف المجدول الزمني بأمان"""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)


def list_scheduled_jobs() -> dict:
    """عرض قائمة المهام المجدولة وحالتها وآخر سجلات التنفيذ"""
    client = MongoClient(settings.MONGO_URI)
    try:
        db = client[settings.MONGO_DATABASE]
        recent_logs = list(
            db[settings.JOBS_LOG_COLLECTION]
            .find({}, {"_id": 0})
            .sort("start_time", DESCENDING)
            .limit(15)
        )
    finally:
        client.close()

    jobs_info = []
    for job_id, meta in JOB_REGISTRY.items():
        last_run = next((log for log in recent_logs if log["job_name"] == job_id), None)
        jobs_info.append({
            "name": job_id,
            "title": meta["title"],
            "schedule": meta["schedule"],
            "scheduler_active": bool(_scheduler and _scheduler.running),
            "last_execution": last_run
        })

    return {
        "scheduler_running": bool(_scheduler and _scheduler.running),
        "jobs": jobs_info,
        "recent_execution_logs": recent_logs
    }


def run_job_by_name(job_name: str) -> dict:
    """تشغيل مهمة مجدولة يدوياً فوراً أثناء المناقشة أو عبر الـ API"""
    if job_name not in JOB_REGISTRY:
        raise ValueError(f"Unknown job '{job_name}'. Available jobs: {list(JOB_REGISTRY.keys())}")

    handler = JOB_REGISTRY[job_name]["handler"]
    return handler(trigger_type="manual")


if __name__ == "__main__":
    print("Executing both scheduled jobs manually for verification...")
    r1 = run_job_by_name("refresh_materialized_views_job")
    print("Job 1 Result:", json.dumps(r1, ensure_ascii=False, indent=2))
    r2 = run_job_by_name("generate_periodic_report_job")
    print("Job 2 Result:", json.dumps(r2, ensure_ascii=False, indent=2))