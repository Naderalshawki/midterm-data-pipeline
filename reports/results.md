# 📊 Comprehensive Pipeline Execution & Audit Report (Phase 1 & Phase 2)

- **Generated At (UTC):** `2026-10-01T21:23:13.864094+00:00`
- **Database:** `hybrid_pipeline_db`
- **Latest Run ID:** `da0b3205-7406-4490-80f5-5f93b2e60de9`

---

## 1. Phase 1: Raw Ingestion, Quality Validation & Upsert Metrics
| Metric | Value |
| :--- | :--- |
| **Raw Input Records** | `N/A` |
| **Valid (Unmodified) Records** | `85926` |
| **Corrected Records (With Audit Trail)** | `9866` |
| **Quarantined Records (Isolated)** | `N/A` |
| **Consistency Check (Section 6.11)** | `PASSED (Raw == Valid + Corrected + Quarantine)` |
| **Idempotent Upsert - Inserted** | `0` |
| **Idempotent Upsert - Updated** | `0` |
| **Elapsed Time (s)** | `75.202` |
| **Throughput (rows/s)** | `1329.75` |

---

## 2. Phase 2: Materialized Views & Incremental Refresh Status
| Materialized View | Refresh Mode | Delta Records Processed | Total View Documents |
| :--- | :---: | :---: | :---: |
| **`daily_sales_summary`** | `incremental_noop` | `0` | `122` |
| **`top_products_summary`** | `incremental_noop` | `0` | `8` |

---

## 3. Generated Reports Inventory (`reports/`)
1. `reports/results.json` — Phase 1 ELT & Quality Validation Audit Log
2. `reports/results.md` — Formatted Markdown Executive Summary
3. `reports/index_explain_report.json` — 3 Indexes & Before/After `explain("executionStats")` Benchmark
4. `reports/aggregations_report.json` — 5 MongoDB Analytical Aggregation Reports
5. `reports/materialized_views_report.json` — Incremental Materialized Views & Watermarks State
6. `reports/periodic_analytics_report.json` — Scheduled Periodic Analytics Output
7. `reports/jobs_execution_history.json` — Scheduled & Manual Jobs Audit Trail
