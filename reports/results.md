# 📊 Comprehensive Pipeline Execution & Audit Report (Phase 1 & Phase 2)

- **Generated At (UTC):** `2026-10-03T22:14:37.063523+00:00`
- **Database:** `hybrid_pipeline_db`
- **Latest Run ID:** `184c2b59-1390-4152-af90-e3ec63cc0c20`

---

## 1. Phase 1: Raw Ingestion, Quality Validation & Upsert Metrics
| Metric | Value |
| :--- | :--- |
| **Raw Input Records** | `20000` |
| **Valid (Unmodified) Records** | `12000` |
| **Corrected Records (With Audit Trail)** | `5000` |
| **Quarantined Records (Isolated)** | `3000` |
| **Consistency Check (Section 6.11)** | `PASSED (Raw == Valid + Corrected + Quarantine)` |
| **Idempotent Upsert - Inserted** | `17000` |
| **Idempotent Upsert - Updated** | `0` |
| **Elapsed Time (s)** | `22.897` |
| **Throughput (rows/s)** | `873.49` |

---

## 2. Phase 2: Materialized Views & Incremental Refresh Status
| Materialized View | Refresh Mode | Delta Records Processed | Total View Documents |
| :--- | :---: | :---: | :---: |
| **`daily_sales_summary`** | `initial_seed` | `17000` | `181` |
| **`top_products_summary`** | `initial_seed` | `17000` | `6` |

---

## 3. Generated Reports Inventory (`reports/`)
1. `reports/results.json` — Phase 1 ELT & Quality Validation Audit Log
2. `reports/results.md` — Formatted Markdown Executive Summary
3. `reports/index_explain_report.json` — 3 Indexes & Before/After `explain("executionStats")` Benchmark
4. `reports/aggregations_report.json` — 5 MongoDB Analytical Aggregation Reports
5. `reports/materialized_views_report.json` — Incremental Materialized Views & Watermarks State
6. `reports/periodic_analytics_report.json` — Scheduled Periodic Analytics Output
7. `reports/jobs_execution_history.json` — Scheduled & Manual Jobs Audit Trail
