"""
Distributed Raw Ingestion Component via PySpark
================================================
Fully compliant with official midterm requirements:
- Section 6.4: PySpark DataFrame API, Explicit StructType Schema, String-Raw fields, Parallel MongoDB Connector, No Pandas.
- Section 6.5: Complete Raw Layer ingestion with run_id, source_file, source_row_number, ingested_at, engine_used, raw_record.
- Section 6.10: Idempotency enforcement (cleans prior runs of the same file).
- Section 6.12: Execution metrics recording (partitions, throughput, elapsed time).
"""
import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

# إجبار البيئة على استخدام محرك PySpark النظيف داخل .venv لمنع تعارض JARs الخارجية
import pyspark
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
if "PYTHONPATH" in os.environ:
    del os.environ["PYTHONPATH"]

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    monotonically_increasing_id,
    struct,
)
from pyspark.sql.types import StringType, StructField, StructType

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
    REPORTS_DIR,
)


def get_raw_schema() -> StructType:
    """Explicit Schema Definition (Section 6.4) - Preserves raw strings without loss."""
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True),
    ])


def save_spark_report(metrics_data: dict) -> Path:
    """Cumulative metrics update to reports/results.json (Section 6.12)."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_file = REPORTS_DIR / "results.json"

    history = []
    if report_file.exists():
        try:
            with open(report_file, "r", encoding="utf-8") as f:
                content = json.load(f)
                history = content if isinstance(content, list) else [content]
        except Exception:
            history = []

    history = [item for item in history if item.get("run_id") != metrics_data.get("run_id")]
    history.append(metrics_data)

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=4, ensure_ascii=False)

    return report_file


def run_spark_pipeline(input_file: str) -> str:
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # 1. منع التكرار وإلزام الـ Idempotency (Section 6.10)
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    raw_col = db[RAW_COLLECTION]
    raw_col.create_index([("run_id", 1)])
    raw_col.create_index([("source_file", 1)])

    deleted = raw_col.delete_many({"source_file": input_path.name})
    if deleted.deleted_count > 0:
        print(f"[*] Cleaned {deleted.deleted_count:,} duplicate raw records from previous runs of '{input_path.name}'.")
    client.close()

    run_id = str(uuid.uuid4())
    start_time = time.perf_counter()

    print("=" * 75)
    print("STAGE 1: DISTRIBUTED RAW INGESTION (PYSPARK + MONGO SPARK CONNECTOR)")
    print("=" * 75)
    print(f"Input File : {input_path.name}")
    print(f"Run ID     : {run_id}")

    # 2. بناء SparkSession النظيف
    spark = (
        SparkSession.builder
        .appName("PySpark_Raw_Ingestion_Pipeline")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.database", MONGO_DATABASE)
        .config("spark.mongodb.write.collection", RAW_COLLECTION)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        # قراءة الملف بالـ Explicit Schema الرسمية دون inferSchema (Section 6.4)
        explicit_schema = get_raw_schema()
        csv_df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("escape", "\"")
            .option("multiLine", "true")
            .schema(explicit_schema)
            .load(str(input_path))
        )

        input_partitions = csv_df.rdd.getNumPartitions()
        print(f"Input Partitions : {input_partitions}")

        all_cols = explicit_schema.names

        # هيكلة البيانات الخام وحقول التتبع الوصفية وفق Section 6.5
        raw_df = (
            csv_df
            .withColumn("run_id", lit(run_id))
            .withColumn("source_file", lit(input_path.name))
            .withColumn("source_row_number", monotonically_increasing_id() + 1)
            .withColumn("ingested_at", current_timestamp())
            .withColumn("engine_used", lit("pyspark"))
            .withColumn("raw_record", struct(*[col_name for col_name in all_cols]))
            .select(
                "run_id",
                "source_file",
                "source_row_number",
                "ingested_at",
                "engine_used",
                "raw_record",
            )
        )

        # الكتابة المتوازية المباشرة إلى MongoDB
        (
            raw_df.write
            .format("mongodb")
            .mode("append")
            .save()
        )

        total_loaded = raw_df.count()
        elapsed = time.perf_counter() - start_time
        throughput = total_loaded / elapsed if elapsed > 0 else 0

        # حفظ المقاييس وفق Section 6.12
        metrics = {
            "run_id": run_id,
            "engine_used": "pyspark",
            "stage": "raw_ingestion_only",
            "file_name": input_path.name,
            "file_size_mb": round(input_path.stat().st_size / (1024 * 1024), 2),
            "raw_loaded": total_loaded,
            "input_partitions": input_partitions,
            "elapsed_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(throughput, 2),
        }

        report_file = save_spark_report(metrics)

        print("\n" + "=" * 75)
        print("STAGE 1 COMPLETED (100% PYSPARK DISTRIBUTED LOAD)")
        print("=" * 75)
        print(f"Total Raw Rows Ingested : {total_loaded:,}")
        print(f"Input Partitions Count  : {input_partitions}")
        print(f"Elapsed Time            : {elapsed:.3f} s")
        print(f"Throughput              : {throughput:,.2f} rows/s")
        print(f"Metrics Saved To        : {report_file}")
        print("=" * 75)

        return run_id

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Raw Ingestion via PySpark")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    args = parser.parse_args()

    run_spark_pipeline(args.input)