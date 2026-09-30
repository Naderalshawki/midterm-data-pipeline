"""
Automated Verification Suite for Phase 2 Final Project Requirements
Tests all 10 FastAPI endpoints:
- GET  /health
- POST /ingest
- POST /indexes
- GET  /queries & GET /queries/{name}
- GET  /aggregations & GET /aggregations/{name}
- POST /refresh-mv (Incremental Refresh verification)
- GET  /jobs & POST /jobs/{name}/run
"""
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi.testclient import TestClient
from src.api import app

client = TestClient(app)


def test_1_health_and_ingest_if_empty():
    print("\n[1/6] Testing GET /health & POST /ingest ...")
    response = client.get("/health")
    assert response.status_code == 200, f"Health failed: {response.text}"
    data = response.json()
    assert data["status"] == "healthy"
    print(" -> Health OK:", data["collections_counts"])

    # إذا كانت القاعدة فارغة نقوم بتشغيل POST /ingest تلقائياً على ملف العينة
    if data["collections_counts"].get("orders_validated", 0) == 0:
        print(" -> Database is empty. Triggering POST /ingest with sample data...")
        res_ingest = client.post("/ingest", json={})
        assert res_ingest.status_code == 200, f"Ingest failed: {res_ingest.text}"
        print(" -> Ingest OK:", res_ingest.json()["metrics"])


def test_2_indexes_and_explain_endpoint():
    print("\n[2/6] Testing POST /indexes (Creating 3 Indexes & Running Explain Before/After)...")
    res = client.post("/indexes")
    assert res.status_code == 200, f"Indexes failed: {res.text}"
    data = res.json()
    assert len(data["indexes_created"]) >= 3
    assert len(data["explain_comparisons"]) >= 3
    for comp in data["explain_comparisons"]:
        assert "before_index" in comp
        assert "after_index" in comp
        print(f" -> Explain [{comp['query_name']}]: {comp['impact_summary']}")


def test_3_queries_endpoints():
    print("\n[3/6] Testing GET /queries & GET /queries/{name} ...")
    res_list = client.get("/queries")
    assert res_list.status_code == 200
    queries = res_list.json()["queries"]
    assert len(queries) >= 5

    for q in queries:
        q_name = q["name"]
        res_q = client.get(f"/queries/{q_name}?limit=5")
        assert res_q.status_code == 200
        payload = res_q.json()
        assert payload["query_name"] == q_name
        assert "results" in payload
        print(f" -> Query '{q_name}' returned {payload['count_returned']} rows.")


def test_4_aggregations_endpoints():
    print("\n[4/6] Testing GET /aggregations & GET /aggregations/{name} ...")
    res_list = client.get("/aggregations")
    assert res_list.status_code == 200
    aggs = res_list.json()["aggregations"]
    assert len(aggs) >= 5

    for agg in aggs:
        agg_name = agg["name"]
        res_agg = client.get(f"/aggregations/{agg_name}?limit=5")
        assert res_agg.status_code == 200
        payload = res_agg.json()
        assert payload["report_name"] == agg_name
        assert "results" in payload
        print(f" -> Aggregation '{agg_name}' returned {payload['count_returned']} rows.")


def test_5_materialized_views_incremental_refresh():
    print("\n[5/6] Testing POST /refresh-mv (Materialized Views & Incremental Refresh)...")
    res1 = client.post("/refresh-mv", json={"force_full": False})
    assert res1.status_code == 200
    data1 = res1.json()
    assert "daily_sales_summary" in data1["views"]
    assert "top_products_summary" in data1["views"]
    print(" -> First Refresh Mode:", data1["views"]["daily_sales_summary"]["metrics"]["refresh_mode"])

    # التشغيل الثاني مباشرة بدون بيانات جديدة لإثبات التحديث التزايدي (0 سجل جديد)
    res2 = client.post("/refresh-mv", json={"force_full": False})
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["views"]["daily_sales_summary"]["metrics"]["delta_records_processed"] == 0
    assert data2["views"]["top_products_summary"]["metrics"]["delta_records_processed"] == 0
    print(" -> Second Refresh Mode (Incremental Verified):", data2["views"]["daily_sales_summary"]["metrics"]["refresh_mode"])


def test_6_scheduled_jobs_endpoints():
    print("\n[6/6] Testing GET /jobs & POST /jobs/{name}/run ...")
    res_jobs = client.get("/jobs")
    assert res_jobs.status_code == 200
    jobs_data = res_jobs.json()
    assert len(jobs_data["jobs"]) >= 2

    for job in jobs_data["jobs"]:
        j_name = job["name"]
        res_run = client.post(f"/jobs/{j_name}/run")
        assert res_run.status_code == 200
        run_payload = res_run.json()
        assert run_payload["job_name"] == j_name
        assert run_payload["status"] == "SUCCESS"
        assert "start_time" in run_payload
        assert "end_time" in run_payload
        print(f" -> Job '{j_name}' executed manually: {run_payload['status']} in {run_payload['duration_seconds']}s")