"""
Idempotency & Upsert Proof Test (Section 6.10 & Section 10)
"""
from datetime import datetime, timezone
from pymongo import MongoClient
import pytest

from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION


def test_idempotency_upsert():
    print("\n" + "=" * 70)
    print("TESTING IDEMPOTENCY & UPSERT CAPABILITIES (Section 6.10)")
    print("=" * 70)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    val_col = db[VALIDATED_COLLECTION]

    initial_count = val_col.count_documents({})
    print(f"[1] Target Database                     : {MONGO_DATABASE}")
    print(f"    Total Validated Records Currently   : {initial_count:,}")

    if initial_count == 0:
        pytest.skip(f"No records found in {VALIDATED_COLLECTION} to test.")
        return

    sample_doc = val_col.find_one()
    order_id = sample_doc["order_id"]
    old_city = sample_doc.get("city")
    test_city = "صنعاء - معدل للتجربة"

    print(f"[2] Selected Order for In-Place Update  : {order_id}")
    print(f"    Original City: '{old_city}' -> Test City: '{test_city}'")

    sample_doc["city"] = test_city
    sample_doc["last_updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    val_col.update_one(
        {"order_id": order_id},
        {"$set": sample_doc},
        upsert=True
    )

    count_after_update = val_col.count_documents({})
    updated_doc = val_col.find_one({"order_id": order_id})

    print(f"[3] Total Validated Records After Upsert: {count_after_update:,}")
    print(f"    Updated City in Database            : '{updated_doc.get('city')}'")

    assert count_after_update == initial_count, "Idempotency failed: record count increased!"
    assert updated_doc.get("city") == test_city, "Upsert failed: record city was not updated!"

    print("\n" + "=" * 70)
    print("IDEMPOTENCY TEST PASSED: 0 DUPLICATES CREATED, 1 RECORD UPDATED IN-PLACE!")
    print("=" * 70)
    client.close()


if __name__ == "__main__":
    test_idempotency_upsert()