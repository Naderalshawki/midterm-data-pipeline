"""
ELT Quality Cleansing, Validation & Idempotent Upsert Pipeline (Stage 2)
========================================================================
Midterm Data Pipeline Project
Fully compliant with official midterm requirements (Sections 6.6 to 6.12).
"""
import argparse
import json
import time
from copy import deepcopy
from pathlib import Path
from typing import Dict

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    REPORTS_DIR,
    VALIDATED_COLLECTION,
)
from src.quality_rules import clean_order


def save_metrics_report(metrics_summary: dict) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_file = REPORTS_DIR / "results.json"

    history = []
    if report_file.exists():
        try:
            with open(report_file, "r", encoding="utf-8") as f:
                content = json.load(f)
                if isinstance(content, list):
                    history = content
                elif isinstance(content, dict):
                    history = [content]
        except Exception:
            history = []

    history = [item for item in history if item.get("run_id") != metrics_summary.get("run_id")]
    history.append(metrics_summary)

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=4, ensure_ascii=False)

    return report_file


def _flush_validated_ops(val_col, val_ops: list) -> tuple[int, int, int]:
    """تنفيذ عمليات الـ Upsert وإرجاع (inserted, updated, unchanged) بدقة"""
    if not val_ops:
        return 0, 0, 0
    try:
        res = val_col.bulk_write(val_ops, ordered=True)
        ins = res.upserted_count
        upd = res.modified_count
        unc = res.matched_count - res.modified_count
        return ins, upd, unc
    except BulkWriteError as bwe:
        details = bwe.details or {}
        ins = details.get("nUpserted", 0)
        upd = details.get("nModified", 0)
        unc = details.get("nMatched", 0) - upd
        return ins, upd, unc


def run_elt_pipeline(target_run_id: str = None, chunk_size: int = 50000):
    start_time = time.perf_counter()

    print("=" * 75)
    print("STAGE 2: QUALITY VALIDATION, CLASSIFICATION & IDEMPOTENT UPSERT")
    print("=" * 75)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DATABASE]

    raw_col = db[RAW_COLLECTION]
    val_col = db[VALIDATED_COLLECTION]
    quar_col = db[QUARANTINE_COLLECTION]

    val_col.create_index([("order_id", 1)], unique=True)
    val_col.create_index([("run_id", 1)])
    quar_col.create_index([("run_id", 1)])

    if not target_run_id:
        latest_raw = raw_col.find_one(sort=[("_id", -1)])
        if not latest_raw:
            raise RuntimeError("No raw data found in orders_raw collection.")
        target_run_id = latest_raw["run_id"]

    quar_col.delete_many({"run_id": target_run_id})

    raw_count_before = raw_col.count_documents({"run_id": target_run_id})
    validated_count_before = val_col.count_documents({})
    effective_batch = min(chunk_size, raw_count_before) if raw_count_before > 0 else chunk_size

    print(f"Target Run ID              : {target_run_id}")
    print(f"Raw Records to Process     : {raw_count_before:,}")
    print(f"Validated Records Before   : {validated_count_before:,}")
    print(f"Batch Processing Size      : {effective_batch:,} (Max Cap: {chunk_size:,})")
    print("-" * 75)

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    error_case_counts: Dict[str, int] = {}

    processed_rows = 0
    cursor = raw_col.find({"run_id": target_run_id}, no_cursor_timeout=True).batch_size(chunk_size)

    val_ops = []
    quar_batch = []

    try:
        for doc in cursor:
            processed_rows += 1
            raw_rec = doc.get("raw_record", {})
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            cleaned_record, corrections, quarantine_reasons = clean_order(deepcopy(raw_rec))

            if quarantine_reasons:
                quarantine_count += 1
                for code in quarantine_reasons:
                    error_case_counts[code] = error_case_counts.get(code, 0) + 1

                quar_batch.append({
                    "run_id": target_run_id,
                    "source_file": doc.get("source_file", "unknown"),
                    "source_row_number": doc.get("source_row_number"),
                    "order_id": raw_rec.get("order_id"),
                    "error_code": quarantine_reasons[0] if len(quarantine_reasons) == 1 else "MULTIPLE_CONFLICTING_ERRORS",
                    "error_details": quarantine_reasons,
                    "raw_record": raw_rec,
                    "quarantined_at": now_iso,
                })
            else:
                is_corrected = len(corrections) > 0
                if is_corrected:
                    corrected_count += 1
                else:
                    valid_count += 1

                order_id = str(cleaned_record.get("order_id"))

                val_payload = {
                    "order_id": order_id,
                    "customer_id": cleaned_record.get("customer_id"),
                    "customer_name": cleaned_record.get("customer_name"),
                    "customer_phone": cleaned_record.get("customer_phone"),
                    "customer_email": cleaned_record.get("customer_email"),
                    "city": cleaned_record.get("city"),
                    "district": cleaned_record.get("district"),
                    "order_date": cleaned_record.get("order_date"),
                    "status": cleaned_record.get("status"),
                    "delivery_type": cleaned_record.get("delivery_type"),
                    "delivery_cost": cleaned_record.get("delivery_cost"),
                    "payment_method": cleaned_record.get("payment_method"),
                    "payment_status": cleaned_record.get("payment_status"),
                    "payment_amount": cleaned_record.get("payment_amount"),
                    "currency": cleaned_record.get("currency", "YER"),
                    "total_amount": cleaned_record.get("total_amount"),
                    "items": cleaned_record.get("items", []),
                    "quality_status": "corrected" if is_corrected else "valid",
                    "corrections": corrections,
                    "last_updated_at": now_iso,
                    "run_id": target_run_id,
                }

                val_ops.append(
                    UpdateOne(
                        {"order_id": order_id},
                        {"$set": val_payload},
                        upsert=True,
                    )
                )

            if len(val_ops) >= chunk_size or len(quar_batch) >= chunk_size:
                if quar_batch:
                    quar_col.insert_many(quar_batch, ordered=False)
                    quar_batch.clear()

                if val_ops:
                    ins, upd, unc = _flush_validated_ops(val_col, val_ops)
                    inserted_count += ins
                    updated_count += upd
                    unchanged_count += unc
                    val_ops.clear()

                elapsed_mid = time.perf_counter() - start_time
                rate = processed_rows / elapsed_mid if elapsed_mid > 0 else 0
                print(f"--> Processed: {processed_rows:,} / {raw_count_before:,} rows | Speed: {rate:,.1f} rows/s")

        if quar_batch:
            quar_col.insert_many(quar_batch, ordered=False)
            quar_batch.clear()

        if val_ops:
            ins, upd, unc = _flush_validated_ops(val_col, val_ops)
            inserted_count += ins
            updated_count += upd
            unchanged_count += unc
            val_ops.clear()

    finally:
        cursor.close()
        client.close()

    elapsed = time.perf_counter() - start_time
    throughput = processed_rows / elapsed if elapsed > 0 else 0
    consistency_check = (raw_count_before == (valid_count + corrected_count + quarantine_count))

    metrics_summary = {
        "run_id": target_run_id,
        "engine_used": "python_batch",
        "stage": "quality_validation_and_idempotent_upsert",
        "rows_read": processed_rows,
        "raw_loaded": raw_count_before,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "consistency_equation_passed": consistency_check,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "error_case_counts": error_case_counts,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_rows_per_sec": round(throughput, 2),
    }

    saved_file = save_metrics_report(metrics_summary)

    print("\n" + "=" * 75)
    print("STAGE 2 COMPLETED: QUALITY & UPSERT RESULTS")
    print("=" * 75)
    print(f"Raw Input Records          : {raw_count_before:,}")
    print(f"Valid (Clean) Records      : {valid_count:,}")
    print(f"Corrected (Audit Trail)    : {corrected_count:,}")
    print(f"Quarantined Records        : {quarantine_count:,}")
    print(f"Consistency Check (6.11)   : {'PASSED (Balanced)' if consistency_check else 'FAILED'}")
    print(f"Upsert - Inserted          : {inserted_count:,}")
    print(f"Upsert - Updated           : {updated_count:,}")
    print(f"Upsert - Unchanged         : {unchanged_count:,}")
    print(f"Elapsed Time               : {elapsed:.3f} s")
    print(f"Throughput                 : {throughput:,.2f} rows/s")
    print(f"Report Saved To            : {saved_file}")
    print("=" * 75)


def main():
    parser = argparse.ArgumentParser(description="ELT Quality Transformation & Upsert Pipeline")
    parser.add_argument("--run-id", default=None, help="Target run_id from orders_raw to process")
    parser.add_argument("--chunk-size", type=int, default=50000, help="Batch processing size")
    args = parser.parse_args()

    run_elt_pipeline(args.run_id, args.chunk_size)


if __name__ == "__main__":
    main()