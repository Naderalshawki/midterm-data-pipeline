# 🚀 High-Performance Hybrid ELT Data Pipeline & Unified Analytics Engine (Phase 1 & Phase 2)
> **Enterprise-Grade Distributed Big Data Ingestion (30M+ Orders), 8-Rule Quality Validation, Anomaly Quarantine, Idempotent Upsert, Indexed Operational Queries, Incremental Materialized Views, Scheduled Jobs, and Unified FastAPI Command Center.**

**Lead Data Engineer & Architect:** **Eng. Nader Alshawki** ([@Naderalshawki](https://github.com/Naderalshawki))  
**Course:** Big Data Engineering (Practical) — **Total Scope:** 25 Grades (Phase 1 Midterm: 18 Grades + Phase 2 Final Additions: 7 Grades)

---

## 📌 1. Project Overview & End-to-End Architecture 🏗️

This repository implements an end-to-end **Hybrid Big Data ELT & Real-Time Analytics Platform** using **PySpark**, **Pure Python Batch Streaming**, **MongoDB**, **APScheduler**, and **FastAPI**:
- **Phase 1 (Midterm Core - 18 Grades):** Processes massive datasets (up to 30,000,000+ records / ~12.65 GB) via an intelligent **File Router** (`<= 200 MB` $\rightarrow$ Python Batch Streaming; `> 200 MB` $\rightarrow$ PySpark Distributed Mode), enforcing 100% raw fidelity in `orders_raw`, applying **8 standardization rules** with full **Audit Trail (`corrections`)**, isolating unrecoverable anomalies in `orders_quarantine`, and performing **Idempotent Upserts** into `orders_validated`.
- **Phase 2 (Final Project Additions - 7 Grades):** Extends the core pipeline with **5 practical operational queries**, **3 custom indexes (including Compound Indexes)** verified with before/after `explain("executionStats")`, **5 independent MongoDB Aggregation Reports**, **2 Incremental Materialized Views** (`daily_sales_summary` & `top_products_summary`), **2 Scheduled Background Jobs** with execution audit logging, an interactive **CLI Launcher**, a **Web Command Center Dashboard (`/`)**, and a unified **FastAPI Swagger interface (`/docs`)**.

```text
+-----------------------------------------------------------------------------------+
|                                  RAW CSV INPUT                                    |
|        (orders_huge_mixed_quality.csv ~ 12.65 GB / 30M rows OR Sample CSV)        |
+-----------------------------------------+-----------------------------------------+
                                          |
                          [ Smart File Router Engine (6.2) ]
              +---------------------------+---------------------------+
              | (Size <= 200 MB)                                      | (Size > 200 MB)
              v                                                       v
  [ Python Batch Streaming ]                             [ PySpark Distributed Mode ]
              +---------------------------+---------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                     STAGE 1: RAW INGESTION & METADATA ENRICHMENT                  |
|  - Explicit StringType Schema Enforcement (100% Raw Fidelity)                     |
|  - Audit Metadata: (run_id, source_file, source_row_number, ingested_at)          |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
                              [( MongoDB: orders_raw )]
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|               STAGE 2: QUALITY VALIDATION, CLASSIFICATION & UPSERT (ELT)          |
|  - 8 Quality Rules: Arabic Digits, Word-Numbers, Currency (YER), Phone (+967),    |
|    Email Syntax Repair, ISO-8601 Dates, Status Mapping, Items JSON & SKU Repair   |
+---------------------+---------------------------------------+---------------------+
                      |                                       |
        (Passed Valid / Repaired)                   (Corrupted / Unrecoverable)
                      |                                       |
                      v                                       v
+-------------------------------------------+   +-----------------------------------+
|        COLLECTION: orders_validated       |   |    COLLECTION: orders_quarantine  |
|  - Valid Records + Corrected (Audit Trail)|   |  - Missing Mandatory Keys         |
|  - Idempotent Upsert (order_id Unique)    |   |  - Impossible Dates / Negatives   |
|  - 3 Custom Indexes (Compound & Single)   |   |  - Non-Parsable Items JSON        |
+---------------------+---------------------+   +-----------------------------------+
                      |
                      v
+-----------------------------------------------------------------------------------+
|            STAGE 3 (PHASE 2): ANALYTICS, INCREMENTAL MVs, JOBS & FASTAPI          |
|  - 5 Operational Queries + Before/After explain("executionStats") Benchmark       |
|  - 5 Independent Aggregation Reports (City, Products, Customers, Period, Status)  |
|  - 2 Incremental Materialized Views (daily_sales_summary, top_products_summary)   |
|  - 2 APScheduler Jobs (Auto & Manual Run + Execution Audit Logs)                  |
|  - Unified FastAPI Server (10 JSON Endpoints + Swagger /docs + Web Dashboard /)   |
+-----------------------------------------------------------------------------------+
```

---

## ✅ 2. Full Compliance Checklist (Midterm 18G + Final 7G = 25/25)

### Phase 1: Midterm Requirements Checklist (18 Grades)
- [x] **Small Sample Automatic Routing:** Selects `Python Batch` automatically when file size $\le 200\text{ MB}$.
- [x] **Large Dataset Automatic Routing:** Selects `PySpark Distributed` automatically when file size $> 200\text{ MB}$.
- [x] **100% Raw Ingestion First:** All records land first in `orders_raw` with full metadata (`run_id`, `source_file`, `ingested_at`).
- [x] **8 Data Quality & Cleaning Rules Implemented:**
  * **Rule 1 (Arabic-Indic Numerals):** Converts `٥٠٠٠` $\rightarrow$ `5000`.
  * **Rule 2 (Arabic Word-Numbers & Currency):** Parses `خمسة آلاف ريال` $\rightarrow$ `5000.0` & `YER`.
  * **Rule 3 (Thousands Separators & Decimals):** Cleans `15,000.50` $\rightarrow$ `15000.50`.
  * **Rule 4 (Currency Normalization):** Standardizes `ريال يمني`, `ر.ي`, `YR` $\rightarrow$ `YER`.
  * **Rule 5 (Yemeni Phone E.164 Format):** Standardizes local variants `771234567` $\rightarrow$ `+967771234567`.
  * **Rule 6 (Email Syntax Repair):** Fixes `user@@mail..com` $\rightarrow$ `user@mail.com`.
  * **Rule 7 (ISO-8601 Date Standardization):** Normalizes mixed dates $\rightarrow$ `YYYY-MM-DDTHH:MM:SSZ`.
  * **Rule 8 (Order & Payment Status Mapping):** Standardizes Arabic/English statuses (`مؤكد` $\rightarrow$ `confirmed`).
- [x] **Audit Trail on Corrected Records:** Every repaired field is logged inside the `corrections` array (`field`, `original_value`, `corrected_value`, `rule_code`).
- [x] **Quarantine Isolation with Reasons:** Unrecoverable records are isolated in `orders_quarantine` with explicit `error_code` and `quarantine_reason`.
- [x] **Section 6.11 Mathematical Consistency Equation Verified:** `Raw == Valid + Corrected + Quarantine` (100% balanced for every `run_id`).
- [x] **Performance & Metrics Logged:** Execution time, throughput (`rows/s`), and counts saved automatically to `reports/results.json`.
- [x] **Idempotency & Upsert Verified:** Re-running the same dataset or updating existing orders produces **0 duplicates** in `orders_validated`.

### Phase 2: Final Project Additions Checklist (7 Grades)
- [x] **1. Queries, Indexes & Explain (1.5 Grades):** 5 practical dynamic queries, 3 indexes (including 2 Compound Indexes), and before/after `explain("executionStats")` comparison for 3 queries saved to `reports/index_explain_report.json`.
- [x] **2. Aggregations (1.5 Grades):** 5 independent MongoDB Aggregation Reports returning live analytical results.
- [x] **3. Materialized Views (1.5 Grades):** 2 Materialized Views (`daily_sales_summary` & `top_products_summary`) with true **Incremental Refresh** via `mv_watermarks` and atomic `$inc` delta merges (no full rebuild).
- [x] **4. Scheduled Jobs (1.0 Grade):** 2 scheduled jobs managed by `APScheduler` supporting both periodic execution and immediate manual triggering, logging `start_time`, `end_time`, `duration_seconds`, and `status` (`SUCCESS`/`FAILED`).
- [x] **5. Unified FastAPI Interface (0.75 Grade):** All 10 required JSON endpoints implemented, Swagger UI exposed at `/docs`, Web Command Center at `/`, and `POST /ingest` wired directly to the Phase 1 ingestion gate.
- [x] **6. GitHub, `.env.example`, `requirements.txt` & Dynamic Execution (0.75 Grade):** Zero hardcoded test outcomes; dynamically adapts to any evaluation dataset.

---

## 📁 3. Project Directory Structure 📂

```plaintext
midterm-data-pipeline/
├── .env.example                      # Clean environment variable template (no sensitive secrets)
├── config/
│   └── settings.py                   # Dynamic settings loader (.env + Phase 1 & Phase 2 collections)
├── data/
│   ├── orders_huge_mixed_quality.csv # Target dataset (30M records ~12.65 GB)
│   └── orders_sample.csv             # Development & evaluation sample file (100k rows)
├── reports/
│   ├── results.json                  # Stage 1 & 2 ELT execution metrics & consistency report
│   ├── index_explain_report.json     # Before vs. After explain("executionStats") benchmark
│   ├── periodic_analytics_report.json# Output generated by periodic scheduled report job
│   └── jobs_execution_history.json   # Execution audit trail for scheduled & manual jobs
├── screenshots/                      # Verification screenshots (Swagger, Dashboard, CLI, Compass)
├── src/
│   ├── main.py                       # Master End-to-End ELT Pipeline entrypoint (Phase 1)
│   ├── file_router.py                # Automatic workload router (Python Batch vs. PySpark)
│   ├── batch_loader.py               # High-speed Python Streaming Batch loader (<= 200MB)
│   ├── spark_loader.py               # Distributed PySpark Raw Ingestion loader (> 200MB)
│   ├── quality_rules.py              # 8 Data Quality & Normalization rules engine
│   ├── elt_pipeline.py               # Stage 2 validation, classification, quarantine & Upsert
│   ├── queries_and_indexes.py        # [Phase 2] 5 Operational Queries + 3 Indexes + Explain
│   ├── aggregations.py               # [Phase 2] 5 Independent MongoDB Aggregation Pipelines
│   ├── materialized_views.py         # [Phase 2] 2 Materialized Views with Incremental Refresh
│   ├── scheduler.py                  # [Phase 2] 2 Scheduled Jobs + Execution Audit Logger
│   ├── api.py                        # [Phase 2] Unified FastAPI Server (/docs & / Dashboard)
│   └── cli_launcher.py               # Interactive Rich Terminal Command Center (Phase 1 + 2)
├── tests/
│   ├── test_cleaning_rules.py        # Unit tests for all 8 data cleaning rules
│   ├── test_classification.py        # Tests for Valid, Corrected, and Quarantine routing
│   ├── test_idempotency.py           # Idempotent Upsert & zero-duplicate verification
│   └── test_phase2_api.py            # Automated test suite for all 10 Phase 2 API endpoints
├── requirements.txt                  # Complete Python dependencies for Phase 1 & Phase 2
└── README.md                         # Comprehensive installation, operation & architecture guide
```

---

## ⚙️ 4. Installation & Environment Setup 💻

### Prerequisites
- **Python:** `3.10` – `3.12`
- **MongoDB Server:** `6.0+` running on `mongodb://localhost:27017`
- **Java (OpenJDK 17/21):** Required only when running PySpark mode on files $> 200\text{ MB}$

### Step-by-Step Setup (Windows PowerShell)

```powershell
# 1. Navigate to the project directory
cd C:\lec\midterm-data-pipeline

# 2. Create virtual environment (if not already created)
python -m venv .venv

# 3. Activate the virtual environment
.\.venv\Scripts\Activate.ps1

# 4. Install all required dependencies
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 5. (Optional) Copy environment template to .env
Copy-Item .env.example .env
```

---

## 🌐 5. Running the Unified FastAPI Server & Web Dashboard (`src/api.py`)

Start the unified API server using either command below:

```powershell
# Method 1: Direct module execution
.\.venv\Scripts\python.exe -m src.api

# Method 2: Using Uvicorn directly
.\.venv\Scripts\python.exe -m uvicorn src.api:app --host 0.0.0.0 --port 8000
```

Once running, open your browser at:
- **Swagger UI (Official Automated Evaluation Interface):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Interactive Web Command Center Dashboard:** [http://localhost:8000/](http://localhost:8000/)
- **System Health & Live Counts:** [http://localhost:8000/health](http://localhost:8000/health)

### Unified API Endpoints Reference (10 Required Routes)

| Method & Endpoint | Description & Functionality | Sample cURL / Request |
| :--- | :--- | :--- |
| **`GET /health`** | Verifies MongoDB connectivity and returns document counts across all Phase 1 & Phase 2 collections. | `curl http://localhost:8000/health` |
| **`POST /ingest`** | Triggers the exact Phase 1 Midterm ingestion gate (`file_router` $\rightarrow$ `orders_raw` $\rightarrow$ `elt_pipeline`). Accepts optional `{"file_path": "..."}`. | `curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d "{}"` |
| **`POST /indexes`** | Creates the 3 custom indexes (including Compound Indexes) and executes before/after `explain("executionStats")` on 3 queries. | `curl -X POST http://localhost:8000/indexes` |
| **`GET /queries`** | Lists all 5 operational queries, their dynamic filters, and serving indexes. | `curl http://localhost:8000/queries` |
| **`GET /queries/{name}`** | Executes a specific operational query by name with optional query parameters (`city`, `status`, `customer_id`, `min_amount`, `limit`). | `curl "http://localhost:8000/queries/city_status_recent_orders?limit=10"` |
| **`GET /aggregations`** | Lists all 5 analytical aggregation reports available in the system. | `curl http://localhost:8000/aggregations` |
| **`GET /aggregations/{name}`** | Runs a specific aggregation report independently and returns live JSON results. | `curl "http://localhost:8000/aggregations/sales_by_city?limit=10"` |
| **`POST /refresh-mv`** | Performs an **Incremental Refresh** of `daily_sales_summary` and `top_products_summary` using watermarks. | `curl -X POST http://localhost:8000/refresh-mv -H "Content-Type: application/json" -d "{\"force_full\": false}"` |
| **`GET /jobs`** | Lists all scheduled background jobs, their cron/interval schedules, and recent execution audit logs. | `curl http://localhost:8000/jobs` |
| **`POST /jobs/{name}/run`** | Immediately triggers a scheduled job manually for testing/defense and logs start/end timestamps & status. | `curl -X POST http://localhost:8000/jobs/refresh_materialized_views_job/run` |

---

## 🖥️ 6. Running via CLI (Individual Modules & Interactive Command Center)

Every component can also be executed independently from the terminal:

```powershell
# 1. Launch Interactive Rich Terminal Command Center (Menu for Phase 1 + Phase 2)
.\.venv\Scripts\python.exe -m src.cli_launcher

# 2. Run End-to-End Phase 1 ELT Pipeline (Default or Custom CSV Path)
.\.venv\Scripts\python.exe -m src.main
.\.venv\Scripts\python.exe -m src.main data/orders_sample.csv

# 3. Create 3 Indexes & Generate Before/After explain("executionStats") Report
.\.venv\Scripts\python.exe -m src.queries_and_indexes

# 4. Run All 5 Aggregation Reports (or a single report independently)
.\.venv\Scripts\python.exe -m src.aggregations --report all --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report sales_by_city --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report top_products --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report top_customers --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report sales_by_period --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report orders_by_status --limit 10

# 5. Run Incremental Materialized Views Refresh
.\.venv\Scripts\python.exe -m src.materialized_views

# 6. Execute Scheduled Jobs Manually & Log Audit Trail
.\.venv\Scripts\python.exe -m src.scheduler
```

---

## 🔍 7. Phase 2 Deep Dive: Queries, Indexes, Explain, Aggregations, MVs & Jobs

### 7.1 The 5 Practical Operational Queries (`src/queries_and_indexes.py`)
1. **`city_status_recent_orders`**: Retrieves the most recent orders filtered by `city` and `status`, sorted by `order_date` descending.
2. **`customer_order_history`**: Retrieves the complete chronological order history for a specific `customer_id`.
3. **`high_value_orders_range`**: Filters orders within a specific financial bracket (`total_amount` between `$gte` and `$lte`), sorted descending.
4. **`lookup_by_customer_phone`**: Performs rapid order lookup by standardized Yemeni phone number (`+9677XXXXXXXX`).
5. **`orders_by_product_item`**: Searches inside the nested `items` array (`items.item_name`) to find orders containing a specific SKU.

### 7.2 The 3 Indexes & `explain("executionStats")` Benchmark (Before vs. After)

| Index Name | Keys & Index Type | Target Query | Before Index (`COLLSCAN`) | After Index (`IXSCAN`) | Why Chosen & Measured Impact |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **`idx_city_status_order_date`** | `(city: 1, status: 1, order_date: -1)`<br>**Compound Index** | `city_status_recent_orders` | Stage: `COLLSCAN`<br>Docs Examined: **95,136**<br>Time: **168 ms** | Stage: `IXSCAN`<br>Docs Examined: **25**<br>Time: **12 ms** | Follows the **ESR (Equality, Sort, Range)** rule. Eliminates full collection scan and in-memory sort, reducing examined docs by **99.97%** and latency by **92.8%**. |
| **`idx_customer_id_order_date`** | `(customer_id: 1, order_date: -1)`<br>**Compound Index** | `customer_order_history` | Stage: `COLLSCAN`<br>Docs Examined: **95,136**<br>Time: **145 ms** | Stage: `IXSCAN`<br>Docs Examined: **1**<br>Time: **12 ms** | Enables direct B-Tree seek for a customer's chronological orders. Reduces examined documents from **95,136 to 1** (**99.99% reduction**). |
| **`idx_total_amount`** | `(total_amount: -1)`<br>**Single Field Index** | `high_value_orders_range` | Stage: `COLLSCAN`<br>Docs Examined: **95,136**<br>Time: **196 ms** | Stage: `IXSCAN`<br>Docs Examined: **25**<br>Time: **10 ms** | Accelerates range queries (`$gte`/`$lte`) and descending sort on `total_amount`, cutting query time from **196 ms to 10 ms** (**94.9% faster**). |

### 7.3 The 5 Aggregation Reports (`src/aggregations.py`)
1. **`sales_by_city`**: Computes `total_orders`, `total_revenue`, `avg_order_value`, and `total_delivery_cost` grouped by city.
2. **`top_products`**: Unwinds the `$items` array (`$unwind`) to calculate `total_quantity_sold`, `total_product_revenue`, and `order_count` per product.
3. **`top_customers`**: Ranks top customers by `total_spent`, `orders_count`, and `last_order_date`.
4. **`sales_by_period`**: Aggregates daily order volume (`total_orders`), `daily_revenue`, and `avg_order_amount` by date (`YYYY-MM-DD`).
5. **`orders_by_status`**: Analyzes order count distribution (`orders_count`), `status_revenue`, and `avg_amount` across order statuses.

### 7.4 Materialized Views & Incremental Refresh (`src/materialized_views.py`)
- **`daily_sales_summary`**: Materialized collection storing pre-aggregated daily revenue, order counts, and delivery costs.
- **`top_products_summary`**: Materialized collection storing cumulative product quantities sold, revenue, and order frequency.
- **How Incremental Refresh Works:**
  1. Each view stores its latest processed `last_updated_at` timestamp and `last_object_id` inside the `mv_watermarks` collection.
  2. On refresh (`POST /refresh-mv`), the engine queries **only newly inserted or updated documents** in `orders_validated` where `last_updated_at > watermark`.
  3. The delta aggregation results are merged atomically into the target view using `UpdateOne(..., {"$inc": {...}, "$set": {...}}, upsert=True)` without dropping or rebuilding historical data.
  4. If no new records exist since the last watermark, it completes in `< 30 ms` with `refresh_mode: "incremental_noop"` and `delta_records_processed: 0`.

### 7.5 Scheduled Jobs & Execution Audit Logging (`src/scheduler.py`)
1. **`refresh_materialized_views_job`** (Scheduled every **15 minutes**): Automatically runs incremental refresh on both Materialized Views.
2. **`generate_periodic_report_job`** (Scheduled every **30 minutes**): Executes key analytical aggregations and exports a consolidated JSON snapshot to `reports/periodic_analytics_report.json`.
- **Audit Logging:** Every scheduled or manual run (`POST /jobs/{name}/run`) logs `job_name`, `trigger_type`, `start_time`, `end_time`, `duration_seconds`, `status` (`SUCCESS`/`FAILED`), and `details` to both MongoDB (`job_execution_logs`) and `reports/jobs_execution_history.json`.

---

## 🧪 8. Automated Testing Suite (PyTest - Phase 1 & Phase 2) 🩺

```powershell
# Run Phase 2 API, Queries, Indexes/Explain, Aggregations, Incremental MVs & Jobs Tests
.\.venv\Scripts\python.exe -m pytest tests/test_phase2_api.py -v -s

# Run Complete Test Suite (Phase 1 Cleaning/Classification/Idempotency + Phase 2 API)
.\.venv\Scripts\python.exe -m pytest tests/ -v
```

```plaintext
tests/test_phase2_api.py::test_1_health_and_ingest_if_empty PASSED
tests/test_phase2_api.py::test_2_indexes_and_explain_endpoint PASSED
tests/test_phase2_api.py::test_3_queries_endpoints PASSED
tests/test_phase2_api.py::test_4_aggregations_endpoints PASSED
tests/test_phase2_api.py::test_5_materialized_views_incremental_refresh PASSED
tests/test_phase2_api.py::test_6_scheduled_jobs_endpoints PASSED
========================= 6 passed in 9.46s =========================
```

---

## 📈 9. Phase 1 Production Benchmark (30 Million Orders) & Sample Metrics 📊

| Stage Description | Metric / Attribute | 30M Production Dataset | 100K Sample Dataset | Status |
| :--- | :--- | :---: | :---: | :---: |
| **Stage 1: Raw Ingestion** | Engine Selected by Router | `PySpark 3.5 Distributed` | `Python Batch Streaming` | ✅ |
| | Input File Size | `12,652.4 MB (~12.65 GB)` | `41.77 MB` | ✅ |
| | Total Raw Ingested (`orders_raw`) | `30,000,000 records` | `100,000 records` | ✅ |
| **Stage 2: ELT & Upsert** | Valid (Unmodified) | `25,772,434 records` | `85,926 records` | ✅ |
| | Corrected (With Audit Trail) | `2,969,128 records` | `9,866 records` | ✅ |
| | Quarantined (`orders_quarantine`) | `1,258,438 records` | `4,208 records` | ✅ |
| | **Section 6.11 Consistency Check** | **`30,000,000 == 30,000,000`** | **`100,000 == 100,000`** | **PASSED (100%)** |
| | Unique Validated (`orders_validated`) | `28,538,963 records` | `95,136 records` | ✅ |
| | Re-Run Duplicate Records Created | `0 Duplicates (Idempotent)` | `0 Duplicates (Idempotent)` | ✅ |

### Quarantine Anomaly Breakdown (30M Dataset)

| Error Case Identifier | Impacted Count | Handling Strategy |
| :--- | :--- | :--- |
| `MISSING_CUSTOMER_ID` | 419,474 | Isolated to `orders_quarantine` with reason |
| `INVALID_IMPOSSIBLE_DATE` | 210,524 | Isolated to `orders_quarantine` with reason |
| `EMPTY_ITEMS` | 209,934 | Isolated to `orders_quarantine` with reason |
| `CORRUPTED_ITEMS_JSON` | 209,432 | Isolated to `orders_quarantine` with reason |
| `MISSING_ORDER_ID` | 209,392 | Isolated to `orders_quarantine` with reason |
| `AMBIGUOUS_NEGATIVE_VALUE` | 209,114 | Isolated to `orders_quarantine` with reason |

---

## 🛡️ 10. Sample Validated Document with Audit Trail (`orders_validated`) 📑

```json
{
  "order_id": "ORD-984210",
  "order_date": "2025-01-31T14:22:00Z",
  "status": "confirmed",
  "customer_id": "CUST-4412",
  "customer_phone": "+967771234567",
  "customer_email": "user@domain.com",
  "city": "صنعاء",
  "district": "السبعين",
  "delivery_type": "express",
  "delivery_cost": 2000.0,
  "payment_method": "cash_on_delivery",
  "payment_status": "paid",
  "payment_amount": 15000.0,
  "currency": "YER",
  "total_amount": 17000.0,
  "items": [
    {
      "item_name": "SKU-9921",
      "quantity": 1,
      "unit_price": 15000.0,
      "subtotal": 15000.0
    }
  ],
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "customer_phone",
      "original_value": "771234567",
      "corrected_value": "+967771234567",
      "rule_code": "PHONE_NORMALIZATION_967"
    }
  ],
  "last_updated_at": "2026-09-30T21:00:00Z"
}
```

---

## 👥 Author & Academic Integrity 🎓

- 👨‍💻 **Lead Data Engineer & Architect:** **Eng. Nader Alshawki** ([@Naderalshawki](https://github.com/Naderalshawki))
- 📚 **Course:** Big Data Engineering — Practical (Midterm Phase 1 + Final Phase 2)
- 📅 **Academic Year:** 2026
- 🏛️ **Institution:** Faculty of Computer Science & Information Technology