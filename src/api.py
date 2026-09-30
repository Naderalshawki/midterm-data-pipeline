"""
Phase 2 - Requirement 5: Unified FastAPI Execution & Testing Interface
+ Enterprise Web Analytics & Command Studio (/) & Official Swagger UI (/docs)
=============================================================================
Provides all 10 required JSON endpoints:
- GET  /health
- POST /ingest
- POST /indexes
- GET  /queries
- GET  /queries/{name}
- GET  /aggregations
- GET  /aggregations/{name}
- POST /refresh-mv
- GET  /jobs
- POST /jobs/{name}/run
"""
import os
import sys
import shutil
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient, DESCENDING

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from src.file_router import route_file
from src.batch_loader import run_batch_pipeline
from src.spark_loader import run_spark_pipeline
from src.elt_pipeline import run_elt_pipeline
from src.queries_and_indexes import (
    list_available_queries,
    execute_named_query,
    create_indexes_and_benchmark_explain,
)
from src.aggregations import (
    list_available_aggregations,
    run_aggregation_report,
)
from src.materialized_views import refresh_all_materialized_views
from src.scheduler import (
    start_background_scheduler,
    stop_background_scheduler,
    list_scheduled_jobs,
    run_job_by_name,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_background_scheduler()
    yield
    stop_background_scheduler()


app = FastAPI(
    title="Big Data Hybrid ELT Pipeline - Unified Phase 2 API",
    description="واجهة التشغيل والاختبار الموحدة للمشروع النصفي والنهائي (PySpark + Python Batch + MongoDB)",
    version="2.0.0",
    lifespan=lifespan,
)


class IngestRequest(BaseModel):
    file_path: Optional[str] = Field(
        default=None,
        description="مسار ملف الـ CSV المراد إدخاله ومعالجته. في حال تركه فارغاً يتم اختيار ملف العينة أو الملف الافتراضي تلقائياً."
    )


class RefreshMVRequest(BaseModel):
    force_full: bool = Field(
        default=False,
        description="إذا كان False يتم التحديث التزايدي (Incremental)، وإذا كان True يتم إعادة البناء الكامل."
    )


def _execute_ingestion_gate(target_path: Path) -> dict:
    """بوابة الإدخال الموحدة للمشروع النصفي (File Router -> Raw -> ELT -> Validated/Quarantine)"""
    if not target_path or not target_path.exists():
        raise HTTPException(status_code=404, detail=f"Input file not found at: {target_path}")

    engine = route_file(str(target_path))
    if engine == "python_batch":
        run_id = run_batch_pipeline(str(target_path))
    else:
        run_id = run_spark_pipeline(str(target_path))

    elt_summary = run_elt_pipeline(target_run_id=run_id)

    client = MongoClient(settings.MONGO_URI)
    try:
        db = client[settings.MONGO_DATABASE]
        raw_count = db[settings.RAW_COLLECTION].count_documents({"run_id": run_id})
        val_count = db[settings.VALIDATED_COLLECTION].estimated_document_count()
        quar_count = db[settings.QUARANTINE_COLLECTION].count_documents({"run_id": run_id})
    finally:
        client.close()

    return {
        "status": "success",
        "input_file": str(target_path),
        "engine_selected": engine,
        "run_id": run_id,
        "metrics": {
            "raw_loaded_for_run": raw_count,
            "quarantined_for_run": quar_count,
            "total_validated_in_db": val_count,
            "elt_return": elt_summary,
        },
    }


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Big Data Hybrid ELT & Analytics Studio | Eng. Nader Alshawki</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;800;900&family=Fira+Code:wght@400;500;600;700&display=swap');
  :root {
    --bg-dark: #030712;
    --panel-bg: rgba(15, 23, 42, 0.82);
    --border-col: rgba(51, 65, 85, 0.75);
    --cyan: #38bdf8;
    --emerald: #10b981;
    --amber: #f59e0b;
    --rose: #f43f5e;
    --violet: #a855f7;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Cairo', sans-serif;
    background:
      radial-gradient(circle at 10% 10%, rgba(56, 189, 248, 0.12), transparent 35%),
      radial-gradient(circle at 90% 20%, rgba(168, 85, 247, 0.12), transparent 35%),
      radial-gradient(circle at 50% 90%, rgba(16, 185, 129, 0.08), transparent 40%),
      var(--bg-dark);
    color: #f8fafc;
    min-height: 100vh;
    padding: 20px 26px;
  }

  /* Top Hero Header */
  .hero {
    display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px;
    background: linear-gradient(135deg, rgba(15, 23, 42, 0.95), rgba(30, 41, 59, 0.85));
    border: 1px solid var(--border-col);
    border-bottom: 2px solid var(--cyan);
    padding: 18px 26px; border-radius: 16px; margin-bottom: 18px;
    box-shadow: 0 15px 35px rgba(0, 0, 0, 0.55);
  }
  .hero-title { display: flex; align-items: center; gap: 14px; }
  .pulse-dot {
    width: 14px; height: 14px; border-radius: 50%; background: var(--emerald);
    box-shadow: 0 0 14px var(--emerald); animation: pulse 2s infinite;
  }
  @keyframes pulse {
    0%, 100% { transform: scale(1); opacity: 1; }
    50% { transform: scale(1.25); opacity: 0.65; }
  }
  .hero h1 { font-size: 22px; font-weight: 900; color: #fff; letter-spacing: 0.3px; }
  .hero h1 span { color: var(--cyan); }
  .hero-sub { font-size: 13px; color: #94a3b8; margin-top: 3px; }
  .hero-actions { display: flex; gap: 10px; flex-wrap: wrap; }
  .hero-btn {
    text-decoration: none; padding: 9px 16px; border-radius: 10px; font-weight: 800;
    font-size: 13px; cursor: pointer; border: none; font-family: 'Cairo', sans-serif;
    display: inline-flex; align-items: center; gap: 6px; transition: all 0.2s;
  }
  .hero-btn:hover { transform: translateY(-2px); filter: brightness(1.1); }
  .btn-master { background: linear-gradient(135deg, #2563eb, #7c3aed); color: #fff; box-shadow: 0 4px 15px rgba(124, 58, 237, 0.4); }
  .btn-swagger { background: var(--emerald); color: #022c22; }
  .btn-refresh { background: #1e293b; color: var(--cyan); border: 1px solid var(--cyan); }

  /* KPI Telemetry Grid */
  .kpi-grid {
    display: grid; grid-template-columns: repeat(6, 1fr); gap: 14px; margin-bottom: 20px;
  }
  @media (max-width: 1200px) { .kpi-grid { grid-template-columns: repeat(3, 1fr); } }
  .kpi-card {
    background: var(--panel-bg); border: 1px solid var(--border-col);
    border-top: 3px solid var(--cyan); border-radius: 12px; padding: 14px 16px;
    position: relative; overflow: hidden;
  }
  .kpi-card .label { font-size: 12px; color: #94a3b8; font-weight: 700; }
  .kpi-card .val {
    font-size: 23px; font-weight: 900; color: #fff;
    font-family: 'Fira Code', monospace; margin-top: 4px;
  }
  .kpi-card .sub { font-size: 11px; color: #64748b; margin-top: 4px; font-family: 'Fira Code', monospace; }

  /* Main Layout */
  .studio-grid { display: grid; grid-template-columns: 430px 1fr; gap: 20px; align-items: start; }
  @media (max-width: 1100px) { .studio-grid { grid-template-columns: 1fr; } }

  /* Left Control Sidebar */
  .sidebar {
    background: var(--panel-bg); border: 1px solid var(--border-col);
    border-radius: 16px; padding: 18px; display: flex; flex-direction: column; gap: 14px;
  }
  .sec-box {
    background: rgba(2, 6, 23, 0.65); border: 1px solid #1e293b;
    border-radius: 12px; padding: 12px; display: flex; flex-direction: column; gap: 8px;
  }
  .sec-title {
    font-size: 14px; font-weight: 800; color: var(--cyan);
    display: flex; justify-content: space-between; align-items: center;
    padding-bottom: 6px; border-bottom: 1px solid #1e293b; margin-bottom: 2px;
  }
  .sec-badge {
    font-size: 11px; background: rgba(56, 189, 248, 0.15); color: var(--cyan);
    padding: 2px 8px; border-radius: 99px; font-family: 'Fira Code', monospace;
  }
  .input-row { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
  .input-full { width: 100%; }
  input.ctrl-input {
    width: 100%; padding: 7px 10px; border-radius: 7px; border: 1px solid #334155;
    background: #0f172a; color: #f8fafc; font-size: 12px; font-family: 'Cairo', 'Fira Code', sans-serif;
  }
  input.ctrl-input:focus { outline: none; border-color: var(--cyan); }
  button.cmd-btn {
    width: 100%; padding: 9px 12px; border: 1px solid #334155; border-radius: 8px;
    background: linear-gradient(90deg, #0f172a, #1e293b); color: #e2e8f0;
    font-family: 'Cairo', sans-serif; font-weight: 700; font-size: 12.5px;
    cursor: pointer; text-align: right; transition: all 0.18s;
    display: flex; justify-content: space-between; align-items: center;
  }
  button.cmd-btn:hover {
    background: linear-gradient(90deg, #1d4ed8, #2563eb);
    border-color: #60a5fa; color: #fff; transform: translateX(-3px);
  }
  .route-tag {
    font-family: 'Fira Code', monospace; font-size: 10.5px; padding: 2px 7px;
    border-radius: 5px; background: #020617; color: var(--cyan); border: 1px solid #1e293b;
  }

  /* Right Interactive Output Studio */
  .workspace {
    background: var(--panel-bg); border: 1px solid var(--border-col);
    border-radius: 16px; padding: 20px; display: flex; flex-direction: column; min-height: 760px;
  }
  .ws-header {
    display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;
    padding-bottom: 14px; border-bottom: 1px solid #1e293b; margin-bottom: 16px;
  }
  .ws-endpoint {
    font-family: 'Fira Code', monospace; font-size: 15px; font-weight: 700; color: var(--cyan);
  }
  .ws-tabs { display: flex; gap: 8px; align-items: center; }
  .tab-btn {
    padding: 6px 14px; border-radius: 8px; border: 1px solid #334155;
    background: #0f172a; color: #94a3b8; font-family: 'Cairo', sans-serif;
    font-size: 12.5px; font-weight: 700; cursor: pointer;
  }
  .tab-btn.active { background: var(--cyan); color: #020617; border-color: var(--cyan); }
  .status-pill {
    font-family: 'Fira Code', monospace; font-size: 12px; font-weight: 700;
    padding: 4px 10px; border-radius: 6px; background: rgba(16, 185, 129, 0.15); color: var(--emerald);
  }

  /* Visual Data Table & Bars */
  #visualContainer { display: block; overflow-x: auto; }
  #jsonContainer { display: none; }
  .summary-banner {
    background: rgba(56, 189, 248, 0.08); border: 1px solid rgba(56, 189, 248, 0.3);
    border-radius: 10px; padding: 12px 16px; margin-bottom: 16px;
    display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;
  }
  table.data-table {
    width: 100%; border-collapse: collapse; font-size: 13px; text-align: right;
    background: rgba(2, 6, 23, 0.55); border-radius: 10px; overflow: hidden;
  }
  table.data-table th {
    background: #0f172a; color: var(--cyan); font-weight: 800;
    padding: 11px 14px; border-bottom: 1px solid #334155; font-family: 'Fira Code', 'Cairo', sans-serif;
  }
  table.data-table td {
    padding: 10px 14px; border-bottom: 1px solid #1e293b; color: #e2e8f0;
    font-family: 'Fira Code', 'Cairo', sans-serif; vertical-align: middle;
  }
  table.data-table tr:hover td { background: rgba(30, 41, 59, 0.55); }
  .bar-wrap {
    width: 120px; height: 7px; background: #1e293b; border-radius: 99px; overflow: hidden; margin-top: 4px;
  }
  .bar-fill { height: 100%; background: linear-gradient(90deg, var(--cyan), var(--emerald)); }
  .badge-collscan { background: rgba(244, 63, 94, 0.2); color: #fda4af; padding: 3px 8px; border-radius: 6px; font-weight: 700; }
  .badge-ixscan { background: rgba(16, 185, 129, 0.2); color: #6ee7b7; padding: 3px 8px; border-radius: 6px; font-weight: 700; }

  pre#jsonOutput {
    background: #020617; border: 1px solid #1e293b; border-radius: 12px;
    padding: 16px; font-family: 'Fira Code', monospace; direction: ltr; text-align: left;
    font-size: 12.5px; color: #a7f3d0; overflow: auto; max-height: 680px; line-height: 1.6;
  }
</style>
</head>
<body>

  <!-- Top Hero Bar -->
  <div class="hero">
    <div class="hero-title">
      <div class="pulse-dot"></div>
      <div>
        <h1>🚀 منصة هندسة البيانات الضخمة والتحليلات الفورية <span>(Phase 1 & Phase 2 Studio)</span></h1>
        <div class="hero-sub">تصميم وتطوير المهندس: نادر الشوكي | PySpark 3.5 + Python Streaming + MongoDB + FastAPI + APScheduler</div>
      </div>
    </div>
    <div class="hero-actions">
      <button class="hero-btn btn-master" onclick="runMasterAllInOne()">⚡ تشغيل شامل لكل متطلبات المرحلة الثانية بضغطة زر</button>
      <a href="/docs" target="_blank" class="hero-btn btn-swagger">📘 فتح Swagger UI الرسمي (/docs)</a>
      <button class="hero-btn btn-refresh" onclick="callApi('GET', '/health')">🔄 تحديث العدادات الحية</button>
    </div>
  </div>

  <!-- 6 Live Telemetry KPI Cards -->
  <div class="kpi-grid">
    <div class="kpi-card" style="border-top-color: var(--cyan);">
      <div class="label">1. البيانات الخام (orders_raw)</div>
      <div class="val" id="st_raw">...</div>
      <div class="sub">Stage 1 Verbatim Ingestion</div>
    </div>
    <div class="kpi-card" style="border-top-color: var(--emerald);">
      <div class="label">2. السجلات المعتمدة (orders_validated)</div>
      <div class="val" id="st_val">...</div>
      <div class="sub">Unique Idempotent Upsert</div>
    </div>
    <div class="kpi-card" style="border-top-color: var(--rose);">
      <div class="label">3. المعزولة لآخر دفعة (Quarantine)</div>
      <div class="val" id="st_quar">...</div>
      <div class="sub">Isolated Anomalies w/ Reason</div>
    </div>
    <div class="kpi-card" style="border-top-color: var(--amber);">
      <div class="label">4. العرض المادي: المبيعات اليومية</div>
      <div class="val" id="st_mv1">...</div>
      <div class="sub">MV: daily_sales_summary</div>
    </div>
    <div class="kpi-card" style="border-top-color: var(--violet);">
      <div class="label">5. العرض المادي: أفضل المنتجات</div>
      <div class="val" id="st_mv2">...</div>
      <div class="sub">MV: top_products_summary</div>
    </div>
    <div class="kpi-card" style="border-top-color: #22c55e;">
      <div class="label">6. حالة الاتساق والـ Watermark</div>
      <div class="val" style="color:#4ade80; font-size:17px;" id="st_db">ONLINE 100%</div>
      <div class="sub" id="st_run">Balanced (Section 6.11)</div>
    </div>
  </div>

  <!-- Main Studio Workspace -->
  <div class="studio-grid">

    <!-- Sidebar Controls -->
    <div class="sidebar">

      <!-- Section 0: Ingestion Gate -->
      <div class="sec-box">
        <div class="sec-title">
          <span>0. بوابة رفع وإدخال البيانات (Midterm Gate)</span>
          <span class="sec-badge">POST /ingest</span>
        </div>
        <input type="text" id="customFilePath" class="ctrl-input" value="data/orders_sample.csv" placeholder="مسار ملف CSV (مثل: data/orders_sample.csv)">
        <button class="cmd-btn" onclick="runIngestByPath()">
          <span>▶️ تشغيل الـ Pipeline على المسار المحدد</span>
          <span class="route-tag">POST /ingest</span>
        </button>
        <input type="file" id="csvFileUpload" class="ctrl-input" accept=".csv">
        <button class="cmd-btn" style="background:linear-gradient(90deg,#064e3b,#047857); border-color:#10b981;" onclick="uploadAndIngestCsv()">
          <span>📤 رفع ملف CSV جديد من جهازك وتشغيله فوراً</span>
          <span class="route-tag">UPLOAD CSV</span>
        </button>
      </div>

      <!-- Section 1: Queries, Indexes & Explain -->
      <div class="sec-box">
        <div class="sec-title">
          <span>1. الاستعلامات الـ 5 والفهارس و Explain</span>
          <span class="sec-badge">1.5 Grade</span>
        </div>
        <button class="cmd-btn" style="border-color:var(--amber);" onclick="callApi('POST', '/indexes')">
          <span>⚡ إنشاء الفهارس الـ 3 ومقارنة Explain قبل/بعد</span>
          <span class="route-tag">POST /indexes</span>
        </button>
        <button class="cmd-btn" onclick="callApi('GET', '/queries')">
          <span>📋 استعراض تعريفات الاستعلامات الـ 5 والفهارس</span>
          <span class="route-tag">GET /queries</span>
        </button>

        <!-- Dynamic Filter Inputs for Queries -->
        <div class="input-row" style="margin-top:4px;">
          <input type="text" id="q_city" class="ctrl-input" placeholder="المدينة (اختياري، مثل: صنعاء)">
          <input type="text" id="q_status" class="ctrl-input" placeholder="الحالة (مثل: confirmed)">
        </div>
        <div class="input-row">
          <input type="text" id="q_cust" class="ctrl-input" placeholder="رقم العميل customer_id">
          <input type="text" id="q_phone" class="ctrl-input" placeholder="الهاتف +967...">
        </div>
        <div class="input-row">
          <input type="number" id="q_min" class="ctrl-input" placeholder="أقل مبلغ min_amount">
          <input type="number" id="q_max" class="ctrl-input" placeholder="أعلى مبلغ max_amount">
        </div>

        <button class="cmd-btn" onclick="runDynamicQuery('city_status_recent_orders')">
          <span>1. طلبات مدينة محددة حسب الحالة والأحدث</span>
          <span class="route-tag">Query 1</span>
        </button>
        <button class="cmd-btn" onclick="runDynamicQuery('customer_order_history')">
          <span>2. السجل التاريخي لطلبات عميل محدد</span>
          <span class="route-tag">Query 2</span>
        </button>
        <button class="cmd-btn" onclick="runDynamicQuery('high_value_orders_range')">
          <span>3. الطلبات ضمن شريحة مالية محددة</span>
          <span class="route-tag">Query 3</span>
        </button>
        <button class="cmd-btn" onclick="runDynamicQuery('lookup_by_customer_phone')">
          <span>4. البحث السريع عبر رقم الهاتف الموحد</span>
          <span class="route-tag">Query 4</span>
        </button>
        <button class="cmd-btn" onclick="runDynamicQuery('orders_by_product_item')">
          <span>5. الطلبات المتضمنة لمنتج محدد (SKU)</span>
          <span class="route-tag">Query 5</span>
        </button>
      </div>

      <!-- Section 2: 5 Aggregation Reports -->
      <div class="sec-box">
        <div class="sec-title">
          <span>2. التقارير التجميعية الخمسة (Aggregations)</span>
          <span class="sec-badge">1.5 Grade</span>
        </div>
        <button class="cmd-btn" onclick="callApi('GET', '/aggregations')">
          <span>📊 عرض قائمة التقارير التجميعية الخمسة</span>
          <span class="route-tag">GET /aggregations</span>
        </button>
        <button class="cmd-btn" onclick="callApi('GET', '/aggregations/sales_by_city?limit=15')">
          <span>1. تقرير المبيعات والطلبات حسب المدينة</span>
          <span class="route-tag">sales_by_city</span>
        </button>
        <button class="cmd-btn" onclick="callApi('GET', '/aggregations/top_products?limit=15')">
          <span>2. تقرير أفضل المنتجات مبيعاً وإيراداً</span>
          <span class="route-tag">top_products</span>
        </button>
        <button class="cmd-btn" onclick="callApi('GET', '/aggregations/top_customers?limit=15')">
          <span>3. تقرير أعلى العملاء إنفاقاً وطلباً</span>
          <span class="route-tag">top_customers</span>
        </button>
        <button class="cmd-btn" onclick="callApi('GET', '/aggregations/sales_by_period?limit=15')">
          <span>4. تقرير المبيعات اليومية حسب الفترة</span>
          <span class="route-tag">sales_by_period</span>
        </button>
        <button class="cmd-btn" onclick="callApi('GET', '/aggregations/orders_by_status')">
          <span>5. تقرير توزيع الطلبات حسب الحالة</span>
          <span class="route-tag">orders_by_status</span>
        </button>
      </div>

      <!-- Section 3 & 4: Materialized Views & Scheduled Jobs -->
      <div class="sec-box">
        <div class="sec-title">
          <span>3 & 4. العروض المادية والمهام المجدولة</span>
          <span class="sec-badge">2.5 Grades</span>
        </div>
        <button class="cmd-btn" style="border-color:var(--emerald);" onclick="callApi('POST', '/refresh-mv', {force_full: false})">
          <span>🔄 تحديث تزايدي للـ Materialized Views (Incremental)</span>
          <span class="route-tag">POST /refresh-mv</span>
        </button>
        <button class="cmd-btn" onclick="callApi('GET', '/jobs')">
          <span>⏱️ عرض المهام المجدولة وسجل التنفيذ (Audit Logs)</span>
          <span class="route-tag">GET /jobs</span>
        </button>
        <button class="cmd-btn" onclick="callApi('POST', '/jobs/refresh_materialized_views_job/run')">
          <span>▶️ تشغيل يدوي فوري: مهمة تحديث الـ MVs</span>
          <span class="route-tag">Run Job 1</span>
        </button>
        <button class="cmd-btn" onclick="callApi('POST', '/jobs/generate_periodic_report_job/run')">
          <span>▶️ تشغيل يدوي فوري: مهمة التقرير الدوري</span>
          <span class="route-tag">Run Job 2</span>
        </button>
      </div>

    </div>

    <!-- Right Interactive Results Studio -->
    <div class="workspace">
      <div class="ws-header">
        <div>
          <div class="ws-endpoint" id="endpointTitle">GET /health (System Telemetry Ready)</div>
          <div style="font-size:12px; color:#94a3b8; margin-top:2px;" id="endpointSub">اختر أي عملية من القائمة الجانبية لعرض الجداول التحليلية ومخرجات JSON الفورية</div>
        </div>
        <div class="ws-tabs">
          <button class="tab-btn active" id="tabVisualBtn" onclick="switchView('visual')">📊 العرض الجدولي والبصري (Visual Studio)</button>
          <button class="tab-btn" id="tabJsonBtn" onclick="switchView('json')">{ } مخرجات JSON الخام (Raw JSON)</button>
          <span class="status-pill" id="statusBadge">READY</span>
        </div>
      </div>

      <!-- Visual Table & Benchmark Container -->
      <div id="visualContainer"></div>

      <!-- Raw JSON Container -->
      <div id="jsonContainer">
        <pre id="jsonOutput">// النتائج التفصيلية بصيغة JSON ستظهر هنا...</pre>
      </div>
    </div>

  </div>

<script>
function switchView(mode) {
  document.getElementById('visualContainer').style.display = (mode === 'visual') ? 'block' : 'none';
  document.getElementById('jsonContainer').style.display = (mode === 'json') ? 'block' : 'none';
  document.getElementById('tabVisualBtn').classList.toggle('active', mode === 'visual');
  document.getElementById('tabJsonBtn').classList.toggle('active', mode === 'json');
}

async function loadHealth() {
  try {
    const res = await fetch('/health');
    const data = await res.json();
    const c = data.collections_counts || {};
    document.getElementById('st_raw').innerText = (c.orders_raw ?? 0).toLocaleString();
    document.getElementById('st_val').innerText = (c.orders_validated ?? 0).toLocaleString();
    document.getElementById('st_quar').innerText = (c.latest_run_quarantine ?? c.orders_quarantine ?? 0).toLocaleString();
    document.getElementById('st_mv1').innerText = (c.daily_sales_summary ?? 0).toLocaleString();
    document.getElementById('st_mv2').innerText = (c.top_products_summary ?? 0).toLocaleString();
    document.getElementById('st_db').innerText = 'DB: ' + (data.database || 'ONLINE');
  } catch (e) {}
}

function runDynamicQuery(qName) {
  const params = new URLSearchParams();
  const city = document.getElementById('q_city').value.trim();
  const status = document.getElementById('q_status').value.trim();
  const cust = document.getElementById('q_cust').value.trim();
  const phone = document.getElementById('q_phone').value.trim();
  const minAmt = document.getElementById('q_min').value.trim();
  const maxAmt = document.getElementById('q_max').value.trim();

  if (city) params.append('city', city);
  if (status) params.append('status', status);
  if (cust) params.append('customer_id', cust);
  if (phone) params.append('customer_phone', phone);
  if (minAmt) params.append('min_amount', minAmt);
  if (maxAmt) params.append('max_amount', maxAmt);
  params.append('limit', '15');

  callApi('GET', `/queries/${qName}?` + params.toString());
}

function runIngestByPath() {
  const pathVal = document.getElementById('customFilePath').value.trim();
  callApi('POST', '/ingest', { file_path: pathVal || null });
}

async function uploadAndIngestCsv() {
  const fileInput = document.getElementById('csvFileUpload');
  if (!fileInput.files || fileInput.files.length === 0) {
    alert('الرجاء اختيار ملف CSV أولاً من جهازك!');
    return;
  }
  const file = fileInput.files[0];
  const formData = new FormData();
  formData.append('file', file);

  document.getElementById('endpointTitle').innerText = 'POST /ingest/upload (' + file.name + ')';
  document.getElementById('statusBadge').innerText = 'UPLOADING & RUNNING...';
  document.getElementById('visualContainer').innerHTML = '<div class="summary-banner">⏳ جاري رفع ملف CSV وتشغيل خط البيانات الهجين (Stage 1 + Stage 2)...</div>';

  const t0 = performance.now();
  try {
    const res = await fetch('/ingest/upload', { method: 'POST', body: formData });
    const data = await res.json();
    const elapsed = Math.round(performance.now() - t0);
    document.getElementById('statusBadge').innerText = `HTTP ${res.status} (${elapsed} ms)`;
    document.getElementById('jsonOutput').innerText = JSON.stringify(data, null, 2);
    renderVisualOutput('/ingest/upload', data);
    loadHealth();
  } catch (err) {
    document.getElementById('statusBadge').innerText = 'ERROR';
    document.getElementById('jsonOutput').innerText = String(err);
  }
}

async function runMasterAllInOne() {
  document.getElementById('endpointTitle').innerText = 'MASTER SUITE EXECUTION (Indexes + Explain + Incremental MVs + Scheduled Jobs)';
  document.getElementById('statusBadge').innerText = 'RUNNING SUITE...';
  document.getElementById('visualContainer').innerHTML = '<div class="summary-banner">⚡ جاري تشغيل الفهارس و Explain وتحديث الـ Materialized Views وتشغيل المهام المجدولة...</div>';
  const t0 = performance.now();
  try {
    const rIdx = await (await fetch('/indexes', { method: 'POST' })).json();
    const rMv  = await (await fetch('/refresh-mv', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({force_full:false}) })).json();
    const rJ1  = await (await fetch('/jobs/refresh_materialized_views_job/run', { method: 'POST' })).json();
    const rJ2  = await (await fetch('/jobs/generate_periodic_report_job/run', { method: 'POST' })).json();
    const combined = { indexes_and_explain: rIdx, materialized_views_refresh: rMv, scheduled_jobs_triggered: [rJ1, rJ2] };
    const elapsed = Math.round(performance.now() - t0);
    document.getElementById('statusBadge').innerText = `SUITE OK (${elapsed} ms)`;
    document.getElementById('jsonOutput').innerText = JSON.stringify(combined, null, 2);
    renderVisualOutput('/indexes', rIdx);
    loadHealth();
  } catch (e) {
    document.getElementById('statusBadge').innerText = 'ERROR';
  }
}

function buildHtmlTableFromArray(rows) {
  if (!rows || rows.length === 0) {
    return '<div class="summary-banner">لا توجد سجلات مطابقة لهذا الفلتر حالياً.</div>';
  }
  const cols = Object.keys(rows[0]);
  // Find max numeric value for visual bar scaling
  let maxNum = 0;
  rows.forEach(r => cols.forEach(k => {
    if (typeof r[k] === 'number' && r[k] > maxNum) maxNum = r[k];
  }));

  let html = '<table class="data-table"><thead><tr>';
  cols.forEach(c => { html += `<th>${c}</th>`; });
  html += '</tr></thead><tbody>';

  rows.forEach(r => {
    html += '<tr>';
    cols.forEach(c => {
      let val = r[c];
      if (typeof val === 'number') {
        const pct = maxNum > 0 ? Math.min(100, Math.round((val / maxNum) * 100)) : 0;
        const showBar = (c.includes('revenue') || c.includes('spent') || c.includes('orders') || c.includes('amount') || c.includes('quantity') || c.includes('count'));
        html += `<td><strong>${val.toLocaleString()}</strong>` +
                (showBar ? `<div class="bar-wrap"><div class="bar-fill" style="width:${pct}%"></div></div>` : '') +
                `</td>`;
      } else if (Array.isArray(val)) {
        html += `<td><code style="font-size:11px;color:#93c5fd;">${JSON.stringify(val)}</code></td>`;
      } else if (typeof val === 'object' && val !== null) {
        html += `<td><code style="font-size:11px;color:#cbd5e1;">${JSON.stringify(val)}</code></td>`;
      } else {
        html += `<td>${val ?? ''}</td>`;
      }
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  return html;
}

function renderVisualOutput(url, data) {
  const container = document.getElementById('visualContainer');

  // 1. Special Renderer for POST /indexes (Explain Before vs After)
  if (data && data.explain_comparisons) {
    let html = `<div class="summary-banner">
      <span>✅ <strong>تم إنشاء 3 فهارس بنجاح</strong> (بينها Compound Indexes) ومقارنة الأداء عبر <code>explain("executionStats")</code></span>
      <span>القاعدة: <code>${data.database}</code></span>
    </div>`;
    html += `<table class="data-table"><thead><tr>
      <th>الاستعلام (Query)</th>
      <th>الفهرس الخادم (Index)</th>
      <th>قبل الفهرس (Stage)</th>
      <th>المفحوصة (Before Docs)</th>
      <th>الزمن (Before)</th>
      <th>بعد الفهرس (Stage)</th>
      <th>المفحوصة (After Docs)</th>
      <th>الزمن (After)</th>
    </tr></thead><tbody>`;
    data.explain_comparisons.forEach(comp => {
      const b = comp.before_index;
      const a = comp.after_index;
      html += `<tr>
        <td><strong>${comp.query_title}</strong><br><small style="color:#94a3b8">${comp.query_name}</small></td>
        <td><code style="color:#38bdf8">${comp.served_by_index}</code></td>
        <td><span class="badge-collscan">${b.scan_stage}</span></td>
        <td>${b.totalDocsExamined.toLocaleString()}</td>
        <td>${b.executionTimeMillis} ms</td>
        <td><span class="badge-ixscan">${a.scan_stage}</span></td>
        <td><strong>${a.totalDocsExamined.toLocaleString()}</strong></td>
        <td><strong style="color:#4ade80">${a.executionTimeMillis} ms</strong></td>
      </tr>`;
    });
    html += `</tbody></table>`;
    container.innerHTML = html;
    return;
  }

  // 2. Renderer for Queries & Aggregations (data.results)
  if (data && Array.isArray(data.results)) {
    const title = data.title || data.query_name || data.report_name || 'نتائج العملية';
    const desc = data.description || '';
    let html = `<div class="summary-banner">
      <div><strong>${title}</strong><div style="font-size:12px;color:#94a3b8">${desc}</div></div>
      <div>عدد السجلات المعادة: <strong>${data.count_returned}</strong></div>
    </div>`;
    html += buildHtmlTableFromArray(data.results);
    container.innerHTML = html;
    return;
  }

  // 3. Renderer for Materialized Views Refresh (POST /refresh-mv)
  if (data && data.views) {
    const d1 = data.views.daily_sales_summary;
    const d2 = data.views.top_products_summary;
    let html = `<div class="summary-banner">
      <span>🔄 <strong>التحديث التزايدي للعروض المادية (Incremental Materialized Views)</strong></span>
      <span>وضع التحديث: <code>${d1.metrics.refresh_mode}</code> | السجلات الجديدة المعالجة (Delta): <strong>${d1.metrics.delta_records_processed}</strong></span>
    </div>`;
    html += `<h4 style="margin:10px 0;color:#38bdf8;">1. عينة العرض المادي الأول: daily_sales_summary (إجمالي الأيام: ${d1.metrics.total_view_documents})</h4>`;
    html += buildHtmlTableFromArray(d1.sample_top_rows);
    html += `<h4 style="margin:18px 0 10px;color:#a855f7;">2. عينة العرض المادي الثاني: top_products_summary (إجمالي المنتجات: ${d2.metrics.total_view_documents})</h4>`;
    html += buildHtmlTableFromArray(d2.sample_top_rows);
    container.innerHTML = html;
    return;
  }

  // 4. Renderer for Lists (queries, aggregations, jobs)
  if (data && Array.isArray(data.queries)) {
    container.innerHTML = buildHtmlTableFromArray(data.queries);
    return;
  }
  if (data && Array.isArray(data.aggregations)) {
    container.innerHTML = buildHtmlTableFromArray(data.aggregations);
    return;
  }
  if (data && Array.isArray(data.jobs)) {
    let html = `<div class="summary-banner">
      <span>⏱️ <strong>حالة المجدول الزمني (APScheduler):</strong> ${data.scheduler_running ? 'يعمل في الخلفية (ACTIVE)' : 'متوقف'}</span>
      <span>عدد سجلات التنفيذ الموثقة: <strong>${(data.recent_execution_logs || []).length}</strong></span>
    </div>`;
    html += `<h4 style="margin:10px 0;color:#38bdf8;">سجل تنفيذ المهام المجدولة (Audit Execution Logs)</h4>`;
    html += buildHtmlTableFromArray(data.recent_execution_logs || []);
    container.innerHTML = html;
    return;
  }

  // Fallback generic key-value table
  if (data && typeof data === 'object') {
    container.innerHTML = buildHtmlTableFromArray([data]);
  }
}

async function callApi(method, url, bodyObj = null) {
  document.getElementById('endpointTitle').innerText = method + ' ' + url;
  document.getElementById('statusBadge').innerText = 'RUNNING...';
  const t0 = performance.now();
  try {
    const opts = { method, headers: { 'Content-Type': 'application/json' } };
    if (bodyObj) opts.body = JSON.stringify(bodyObj);
    const res = await fetch(url, opts);
    const data = await res.json();
    const elapsed = Math.round(performance.now() - t0);
    document.getElementById('statusBadge').innerText = `HTTP ${res.status} (${elapsed} ms)`;
    document.getElementById('jsonOutput').innerText = JSON.stringify(data, null, 2);
    renderVisualOutput(url, data);
    loadHealth();
  } catch (err) {
    document.getElementById('statusBadge').innerText = 'ERROR';
    document.getElementById('jsonOutput').innerText = String(err);
  }
}

// Initial load: fetch health and display sales_by_city chart table immediately
window.addEventListener('DOMContentLoaded', () => {
  loadHealth();
  callApi('GET', '/aggregations/sales_by_city?limit=10');
});
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def web_command_center():
    """منصة القيادة والتحليلات البصرية التفاعلية"""
    return DASHBOARD_HTML


# ============================================================
# 1. GET /health
# ============================================================
@app.get("/health", tags=["System Health"])
def health_check():
    """فحص حالة النظام والاتصال بقاعدة بيانات MongoDB وإحصائيات المجموعات"""
    client = MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
        db = client[settings.MONGO_DATABASE]
        latest_raw = db[settings.RAW_COLLECTION].find_one({}, {"run_id": 1}, sort=[("_id", DESCENDING)])
        latest_run_id = latest_raw.get("run_id") if latest_raw else None
        latest_quar = (
            db[settings.QUARANTINE_COLLECTION].count_documents({"run_id": latest_run_id})
            if latest_run_id else db[settings.QUARANTINE_COLLECTION].estimated_document_count()
        )
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": settings.MONGO_DATABASE,
            "latest_run_id": latest_run_id,
            "collections_counts": {
                settings.RAW_COLLECTION: db[settings.RAW_COLLECTION].estimated_document_count(),
                settings.VALIDATED_COLLECTION: db[settings.VALIDATED_COLLECTION].estimated_document_count(),
                settings.QUARANTINE_COLLECTION: db[settings.QUARANTINE_COLLECTION].estimated_document_count(),
                "latest_run_quarantine": latest_quar,
                settings.MV_DAILY_SALES: db[settings.MV_DAILY_SALES].estimated_document_count(),
                settings.MV_TOP_PRODUCTS: db[settings.MV_TOP_PRODUCTS].estimated_document_count(),
            },
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {exc}")
    finally:
        client.close()


# ============================================================
# 2. POST /ingest
# ============================================================
@app.post("/ingest", tags=["Stage 1 & 2 Ingestion (Midterm Pipeline)"])
def ingest_data(payload: Optional[IngestRequest] = None):
    """يستخدم نفس بوابة الإدخال والـ Pipeline المنفذة في المشروع النصفي"""
    target_path = None
    if payload and payload.file_path:
        candidate = Path(payload.file_path)
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        target_path = candidate
    elif settings.SAMPLE_FILE.exists():
        target_path = settings.SAMPLE_FILE
    elif settings.INPUT_FILE.exists():
        target_path = settings.INPUT_FILE
    else:
        target_path = PROJECT_ROOT / "data" / "orders_small_sample.csv"

    try:
        return _execute_ingestion_gate(target_path)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline ingestion failed: {exc}")


@app.post("/ingest/upload", tags=["Stage 1 & 2 Ingestion (Midterm Pipeline)"])
def upload_and_ingest_csv(file: UploadFile = File(...)):
    """رفع ملف CSV جديد مباشرة من المتصفح أو Swagger وتشغيل بوابة الإدخال والـ Pipeline عليه فوراً"""
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = Path(file.filename or "uploaded_orders.csv").name
    saved_path = settings.DATA_DIR / f"uploaded_{safe_name}"
    try:
        with open(saved_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return _execute_ingestion_gate(saved_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CSV upload & ingestion failed: {exc}")
    finally:
        file.file.close()


# ============================================================
# 3. POST /indexes
# ============================================================
@app.post("/indexes", tags=["1. Queries, Indexes & Explain"])
def build_indexes_and_explain():
    """إنشاء الفهارس الثلاثة (بينها Compound Index) وتشغيل مقارنة explain('executionStats') قبل وبعد"""
    try:
        return create_indexes_and_benchmark_explain()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Index creation/explain failed: {exc}")


# ============================================================
# 4. GET /queries
# ============================================================
@app.get("/queries", tags=["1. Queries, Indexes & Explain"])
def get_all_queries():
    """عرض قائمة الاستعلامات الخمسة المتاحة في المشروع"""
    return {
        "count": 5,
        "queries": list_available_queries(),
    }


# ============================================================
# 5. GET /queries/{name}
# ============================================================
@app.get("/queries/{name}", tags=["1. Queries, Indexes & Explain"])
def run_query(
    name: str,
    city: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    customer_id: Optional[str] = Query(default=None),
    customer_phone: Optional[str] = Query(default=None),
    min_amount: Optional[float] = Query(default=None),
    max_amount: Optional[float] = Query(default=None),
    item_name: Optional[str] = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
):
    """تشغيل استعلام محدد بالاسم مع إمكانية تمرير معاملات بحث اختيارية"""
    params = {
        "city": city,
        "status": status,
        "customer_id": customer_id,
        "customer_phone": customer_phone,
        "min_amount": min_amount,
        "max_amount": max_amount,
        "item_name": item_name,
    }
    try:
        return execute_named_query(name, params=params, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {exc}")


# ============================================================
# 6. GET /aggregations
# ============================================================
@app.get("/aggregations", tags=["2. Aggregation Reports"])
def get_all_aggregations():
    """عرض قائمة تقارير الـ Aggregation الخمسة المتاحة"""
    reports = list_available_aggregations()
    return {
        "count": len(reports),
        "aggregations": reports,
    }


# ============================================================
# 7. GET /aggregations/{name}
# ============================================================
@app.get("/aggregations/{name}", tags=["2. Aggregation Reports"])
def get_aggregation_by_name(
    name: str,
    limit: int = Query(default=20, ge=1, le=200),
):
    """تشغيل تقرير Aggregation محدد بالاسم وإرجاع نتائجه الفعلية"""
    try:
        return run_aggregation_report(name, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Aggregation report failed: {exc}")


# ============================================================
# 8. POST /refresh-mv
# ============================================================
@app.post("/refresh-mv", tags=["3. Materialized Views"])
def refresh_materialized_views_endpoint(payload: Optional[RefreshMVRequest] = None):
    """تحديث العروض المادية (daily_sales_summary و top_products_summary) تزايدياً"""
    force_full = payload.force_full if payload else False
    try:
        return refresh_all_materialized_views(force_full=force_full)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Materialized Views refresh failed: {exc}")


# ============================================================
# 9. GET /jobs
# ============================================================
@app.get("/jobs", tags=["4. Scheduled Jobs"])
def get_scheduled_jobs():
    """عرض المهام المجدولة وحالتها وسجل التنفيذ"""
    try:
        return list_scheduled_jobs()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {exc}")


# ============================================================
# 10. POST /jobs/{name}/run
# ============================================================
@app.post("/jobs/{name}/run", tags=["4. Scheduled Jobs"])
def trigger_job_manually(name: str):
    """تشغيل مهمة مجدولة يدوياً بشكل فوري وتسجيل وقت البداية والنهاية والحالة"""
    try:
        return run_job_by_name(name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Manual job execution failed: {exc}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run("src.api:app", host="0.0.0.0", port=port, reload=False)