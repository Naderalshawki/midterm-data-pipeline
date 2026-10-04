# 📊 Comprehensive Pipeline Execution & Audit Report (Phase 1 & Phase 2)

- **Generated At (UTC):** `2026-10-04T15:48:55.075503+00:00`
- **Database:** `hybrid_pipeline_db`
- **Latest Run ID:** `b13bc4f0-c876-4866-9b37-2f6b10aef193`

---

## 1. Phase 1: Raw Ingestion, Quality Validation & Upsert Metrics
| Metric | Value |
| :--- | :--- |
| **Raw Input Records** | `100000` |
| **Valid (Unmodified) Records** | `68966` |
| **Corrected Records (With Audit Trail)** | `21258` |
| **Quarantined Records (Isolated)** | `9776` |
| **Consistency Check (Section 6.11)** | `PASSED (Raw == Valid + Corrected + Quarantine)` |
| **Idempotent Upsert - Inserted** | `89603` |
| **Idempotent Upsert - Updated** | `621` |
| **Elapsed Time (s)** | `69.587` |
| **Throughput (rows/s)** | `1437.05` |

---

## 2. Phase 2: Materialized Views & Incremental Refresh Status
| Materialized View | Refresh Mode | Delta Records Processed | Total View Documents |
| :--- | :---: | :---: | :---: |
| **`daily_sales_summary`** | `initial_seed` | `89603` | `121` |
| **`top_products_summary`** | `initial_seed` | `89603` | `6` |

---

## 3. Generated Reports Inventory (`reports/`)
1. `reports/results.json` — Phase 1 ELT & Quality Validation Audit Log
2. `reports/results.md` — Formatted Markdown Executive Summary
3. `reports/index_explain_report.json` — 3 Indexes & Before/After `explain("executionStats")` Benchmark
4. `reports/aggregations_report.json` — 5 MongoDB Analytical Aggregation Reports
5. `reports/materialized_views_report.json` — Incremental Materialized Views & Watermarks State
6. `reports/periodic_analytics_report.json` — Scheduled Periodic Analytics Output
7. `reports/jobs_execution_history.json` — Scheduled & Manual Jobs Audit Trail
