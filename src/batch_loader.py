"""
Python Streaming Batch Ingestion (Stage 1)
===========================================
Section 6.3: Streaming batch loader with strict idempotency (0 duplicate accumulation).
"""
import csv
import time
import uuid
from pathlib import Path
from pymongo import MongoClient
from config.settings import BATCH_SIZE, MONGO_DATABASE, MONGO_URI, RAW_COLLECTION


def run_batch_pipeline(file_path: str, batch_size: int = BATCH_SIZE) -> str:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    raw_col = db[RAW_COLLECTION]

    raw_col.create_index([("run_id", 1)])
    raw_col.create_index([("source_file", 1)])

    file_name = Path(file_path).name

    # مسح أي تشغيل سابق لنفس الملف لضمان بقاء العدد مطابقاً لعدد صفوف الملف بدقة (100,000 فقط)
    raw_col.delete_many({"source_file": file_name})

    run_id = str(uuid.uuid4())
    print("\n" + "=" * 75)
    print("STAGE 1: PYTHON STREAMING BATCH INGESTION (Section 6.3)")
    print("=" * 75)
    print(f"Input File : {file_name}")
    print(f"Run ID     : {run_id}")
    print(f"Batch Size : {batch_size:,}")

    start_time = time.perf_counter()
    batch = []
    total_inserted = 0
    batch_num = 0

    with open(file_path, mode="r", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row_idx, row in enumerate(reader, start=1):
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000000+00:00", time.gmtime())
            doc = {
                "run_id": run_id,
                "source_file": file_name,
                "source_row_number": row_idx,
                "ingested_at": now_iso,
                "engine_used": "python_batch",
                "raw_record": row,
            }
            batch.append(doc)

            if len(batch) >= batch_size:
                batch_num += 1
                b_start = time.perf_counter()
                raw_col.insert_many(batch, ordered=False)
                b_dur = time.perf_counter() - b_start
                total_inserted += len(batch)
                rate = len(batch) / b_dur if b_dur > 0 else 0
                print(f"[*] Batch {batch_num:>3} | Inserted {len(batch):,} rows | Total: {total_inserted:,} | Speed: {rate:,.1f} rows/s")
                batch.clear()

        if batch:
            batch_num += 1
            raw_col.insert_many(batch, ordered=False)
            total_inserted += len(batch)
            print(f"[*] Batch {batch_num:>3} | Inserted {len(batch):,} rows | Total: {total_inserted:,}")
            batch.clear()

    total_time = time.perf_counter() - start_time
    throughput = total_inserted / total_time if total_time > 0 else 0
    print("=" * 75)
    print(f"STAGE 1 (Python Batch) COMPLETED: {total_inserted:,} Raw Records Ingested in {total_time:.2f}s ({throughput:,.2f} rows/s)")
    print("=" * 75)

    client.close()
    return run_id