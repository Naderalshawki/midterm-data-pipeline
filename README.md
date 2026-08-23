# 🚀 High-Performance Distributed ELT Data Pipeline (30 Million Orders)
> **Automated Big Data Ingestion, Quality Validation, Anomaly Quarantine, and Idempotent Upsert Pipeline for E-Commerce Operations in Yemen.**

---

## 📌 1. Project Overview & Architecture 🏗️

This project implements an enterprise-grade, end-to-end distributed ELT pipeline built to process, clean, validate, and store massive datasets ($30,000,000+$ records) using **PySpark**, **Pure Python**, and **MongoDB**. 

The system operates on an ELT philosophy:
1. **Raw Stage (E & L):** Stream and store dirty data at scale without information loss into MongoDB collection `orders_raw`.
2. **Transform & Validate Stage (T):** Read raw records, apply strict schema validation and 8 Yemeni-market data quality rules, repair recoverables, isolate anomalies, and execute idempotent in-place updates.

+-------------------------------------------------------------------------------+
|                             RAW CSV INPUT                                     |
|             (orders_huge_mixed_quality.csv ~ 12.65 GB / 30M rows)             |
+---------------------------------------+---------------------------------------+
|
[ File Router Engine ]
(Size > 200MB -> PySpark Distributed Mode)
|
v
+-------------------------------------------------------------------------------+
|                 STAGE 1: DISTRIBUTED RAW INGESTION (PySpark)                  |
|  - Schema Enforcement (All StringType StructType)                             |
|  - Metadata Enrichment (run_id, source_file, row_number, ingested_at)         |
|  - Direct Mongo Spark Connector Parallel Bulk Ingestion                       |
+---------------------------------------+---------------------------------------+
|
v
[( MongoDB: orders_raw )]
|
v
+-------------------------------------------------------------------------------+
|              STAGE 2: QUALITY VALIDATION & CLASSIFICATION (ELT)               |
|  - Arabic-Indic Digits Normalization & Word-Number Parsing                    |
|  - Yemeni Phone Standardization (+967) & Email Cleaning                       |
|  - Date ISO Formatting & Currency Fallback (YER)                              |
|  - Malformed Items JSON Extraction & SKU Repair                               |
+-------------------+-----------------------------------+-----------------------+
|                                   |
(Passed Valid / Repaired)              (Corrupted / Unrecoverable)
|                                   |
v                                   v
+---------------------------------------+   +-----------------------------------+
|      COLLECTION: orders_validated     |   |   COLLECTION: orders_quarantine   |
|  - Valid Records (Unmodified)         |   |  - Missing Mandatory Keys         |
|  - Corrected Records (Audit Trail)    |   |  - Impossible Negative Values     |
|  - Idempotent Upsert (order_id unique)|   |  - Malformed Non-Parsable JSON    |
+---------------------------------------+   +-----------------------------------+


---

## ⚙️ 2. Core Features & Compliance Checklist ✅

* ⚡ **Zero-Pandas Distributed Ingestion:** Handled 100% through PySpark DataFrame API with explicit schema definition.
* 🛡️ **Preserved Raw Fidelity:** Captures source payloads in verbatim string structure for full auditability.
* 🧪 **8 Data Quality Rules Engine:**
  * **Rule 1 (Arabic-Indic Numerals):** `٥٠٠٠` $\rightarrow$ `5000`.
  * **Rule 2 (Currency Standardization):** Standardizes strings to `YER`.
  * **Rule 3 (Numeric Cleaners):** Removes thousand separators `,` and parses decimals.
  * **Rule 4 (Arabic Text Numbers):** Parses words like `خمسة آلاف` to `5000.0`.
  * **Rule 5 (Yemeni Phone Format):** Cleans local and international variants to `+9677XXXXXXXX`.
  * **Rule 6 (Email Cleaning):** Repairs consecutive `@` and `.` characters (`user@@mail..com` $\rightarrow$ `user@mail.com`).
  * **Rule 7 (Date ISO 8601):** Validates and standardizes mixed dates to `YYYY-MM-DDTHH:MM:SSZ`.
  * **Rule 8 (Status Mapping):** Standardizes Arabic/English statuses (`مؤكد`, `paid` $\rightarrow$ `confirmed`).
* 🔒 **Quarantine Isolation Mechanism:** Isolate corrupt records (`MISSING_ORDER_ID`, `CORRUPTED_ITEMS_JSON`, etc.) without pipeline failure.
* 🔁 **Idempotent Upsert Engine:** Guarantees zero duplicate records upon re-execution via `$set` and unique index checks.
* 📊 **100% Mathematical Consistency Equation Verified:**
  $$\text{Raw Loaded} = \text{Valid} + \text{Corrected} + \text{Quarantine}$$

---

## 📁 3. Project Directory Structure 📂

```plaintext
midterm-data-pipeline/
├── config/
│   └── settings.py              # Central DB URIs, paths, batch sizes & thresholds
├── data/
│   ├── orders_huge_mixed_quality.csv   # Target dataset (30M records ~12.65 GB)
│   └── orders_sample.csv               # Development & test sample file
├── reports/
│   ├── screenshots/             # Proof screenshots (Spark UI, Compass, Tests)
│   ├── results.json             # Cumulative execution performance logs
│   └── results.md               # Formatted audit report
├── src/
│   ├── file_router.py           # Auto-routes workloads by file size
│   ├── spark_loader.py          # Distributed Raw Ingestion engine
│   ├── quality_rules.py         # 8 Quality validation and parsing rules
│   ├── elt_pipeline.py          # Stage 2 validation, quarantine & upsert worker
│   └── main.py                  # End-to-end master pipeline orchestrator
├── tests/
│   ├── test_cleaning_rules.py   # Unit tests for all 8 normalization rules
│   ├── test_classification.py   # Unit tests for Valid/Corrected/Quarantine
│   └── test_idempotency.py      # Production-grade Upsert & Idempotency test
├── requirements.txt             # Python project dependencies
└── README.md                    # System documentation
🚀 4. Installation & Environment Setup 💻
Prerequisites
Python: 3.10 to 3.12

Java: OpenJDK 17 or 21 (Configured in JAVA_HOME)

MongoDB: Server 6.0+ (Running on mongodb://localhost:27017)

1. Virtual Environment & Dependencies
PowerShell
# Clone or navigate to the repository
cd midterm-data-pipeline

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.venv\Scripts\activate

# Upgrade pip & install dependencies
pip install --upgrade pip
pip install -r requirements.txt
2. Configuration Settings (config/settings.py)
Ensure your settings point to the production database:

Python
MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "pipeline_30m_production"
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"
🏃 5. How to Run the Pipeline 🔄
Run End-to-End Orchestrator (Full 30M Workflow)
PowerShell
python -m src.main
Run Distributed PySpark Raw Ingestion Only
PowerShell
python -m src.spark_loader --input data/orders_huge_mixed_quality.csv
Run Stage 2 ELT Validation & Upsert Engine
PowerShell
python -m src.elt_pipeline --run-id <YOUR_INGESTION_RUN_ID>
🧪 6. Automated Testing Suite (PyTest) 🩺
Run all unit tests and idempotency verification suites across the pipeline:

PowerShell
python -m pytest tests/ -v -s
Test Verification Results
Plaintext
============================= test session starts =============================
platform win32 -- Python 3.12.4, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\lec\midterm-data-pipeline
collected 8 items

tests/test_classification.py::test_classification_valid_record PASSED     [ 12%]
tests/test_classification.py::test_classification_corrected_record PASSED [ 25%]
tests/test_classification.py::test_classification_quarantined_record PASSED [ 37%]
tests/test_cleaning_rules.py::test_rule1_arabic_digits PASSED             [ 50%]
tests/test_cleaning_rules.py::test_rule2_word_numbers_and_currency PASSED   [ 62%]
tests/test_cleaning_rules.py::test_rule3_thousands_separators PASSED      [ 75%]
tests/test_cleaning_rules.py::test_rule5_to_8_clean_order_pipeline PASSED  [ 87%]
tests/test_idempotency.py::test_idempotency_upsert PASSED                  [100%]

======================================================================
TESTING IDEMPOTENCY & UPSERT CAPABILITIES (Section 6.10)
======================================================================
[1] Target Database                     : pipeline_30m_production
    Total Validated Records Currently   : 28,538,963
[2] Selected Order for In-Place Update  : طلب-100000
    Original City: 'تعز' -> Test City: 'صنعاء - معدل للتجربة'
[3] Total Validated Records After Upsert: 28,538,963
    Updated City in Database            : 'صنعاء - معدل للتجربة'

======================================================================
IDEMPOTENCY TEST PASSED: 0 DUPLICATES CREATED, 1 RECORD UPDATED IN-PLACE!
======================================================================
========================= 8 passed in 187.04s (0:03:07) =========================
📈 7. Production Benchmark & Performance Results (30 Million Orders) 📊
Execution Summary Breakdown
Stage Description	Metric / Attribute	Recorded Value	Status
Stage 1: Raw Ingestion	Engine Used	PySpark 3.5.1 Distributed	✅
Input File Size	12,652.4 MB (~12.65 GB)	✅
Total Raw Ingested	30,000,000 records	✅
Target Raw Collection	orders_raw	✅
Stage 2: Validation & ELT	Engine Used	Pure Python Batch Streaming	✅
Batch Chunk Size	50,000 docs / batch	✅
Valid (Unmodified)	25,772,434 records	✅
Corrected (Repaired)	2,969,128 records	✅
Quarantined (Isolated)	1,258,438 records	✅
Consistency Equation	30,000,000 == 30,000,000	PASSED (100%)
Inserted (New Docs)	28,538,963 records	✅
Updated In-Place (Upsert)	149,737 records	✅
Net Validated Count	28,538,963 records	✅
Quarantine Anomaly Breakdown
Error Case Identifier	Impacted Count	Handling Strategy
MISSING_CUSTOMER_ID	419,474	Isolated to orders_quarantine
INVALID_IMPOSSIBLE_DATE	210,524	Isolated to orders_quarantine
EMPTY_ITEMS	209,934	Isolated to orders_quarantine
CORRUPTED_ITEMS_JSON	209,432	Isolated to orders_quarantine
MISSING_ORDER_ID	209,392	Isolated to orders_quarantine
AMBIGUOUS_NEGATIVE_VALUE	209,114	Isolated to orders_quarantine
🛡️ 8. Audit Trail & Data Schema 📑
Validated Record Schema (orders_validated)
JSON
{
  "_id": "6a8a2595ec27b56bf7f2f80d",
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
  "last_updated_at": "2026-08-23T14:40:00Z"
}
👥 Contributors & Academic Integrity 🎓
Developed for the Midterm Data Engineering Big Data Processing Course.

Implemented in full accordance with course specifications and validation guidelines.