from pymongo import MongoClient, ASCENDING
from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def get_database():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[MONGO_DATABASE]
    return client, db


def setup_database(drop_existing: bool = False):
    client, db = get_database()

    if drop_existing:
        print("[*] Dropping existing collections to reset data...")
        db[RAW_COLLECTION].drop()
        db[VALIDATED_COLLECTION].drop()
        db[QUARANTINE_COLLECTION].drop()
        print("[+] Collections dropped successfully.")

    # 1. Raw Layer Index
    db[RAW_COLLECTION].create_index([("run_id", ASCENDING)])
    
    # 2. Validated Layer: Strict Unique Index on order_id (Stable Business Key)
    db[VALIDATED_COLLECTION].create_index([("order_id", ASCENDING)], unique=True)
    db[VALIDATED_COLLECTION].create_index([("run_id", ASCENDING)])

    # 3. Quarantine Layer Index
    db[QUARANTINE_COLLECTION].create_index([("run_id", ASCENDING)])
    db[QUARANTINE_COLLECTION].create_index([("order_id", ASCENDING)])

    print("=" * 60)
    print("MONGODB SETUP COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Database: {MONGO_DATABASE}")
    print(f"Collections configured with official project indexes:")
    print(f" - {RAW_COLLECTION} (index: run_id)")
    print(f" - {VALIDATED_COLLECTION} (unique index: order_id)")
    print(f" - {QUARANTINE_COLLECTION} (index: run_id, order_id)")
    print("=" * 60)

    client.close()


if __name__ == "__main__":
    setup_database(drop_existing=True)