"""
Phase 2 - Requirement 3: Materialized Views with True Incremental Refresh
=========================================================================
Implements 2 Materialized Views built on Aggregations:
1. daily_sales_summary
2. top_products_summary

Uses ObjectId (`_id`) & Watermarks (`mv_watermarks`) to guarantee that:
- Re-running the same dataset does NOT double-count existing orders (Delta = 0).
- Uploading/ingesting a new CSV batch processes ONLY the newly inserted orders!
"""
import argparse
import json
from datetime import datetime, timezone
from bson import ObjectId
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from config import settings


def get_db():
    client = MongoClient(settings.MONGO_URI)
    return client, client[settings.MONGO_DATABASE]


def _ensure_mv_indexes(db):
    db[settings.MV_DAILY_SALES].create_index([("period_date", ASCENDING)], unique=True, name="uq_mv_daily_sales_date")
    db[settings.MV_TOP_PRODUCTS].create_index([("product_name", ASCENDING)], unique=True, name="uq_mv_top_products_name")
    db[settings.MV_WATERMARKS].create_index([("view_name", ASCENDING)], unique=True, name="uq_mv_watermark_view")


def _build_delta_match(watermark_doc: dict, force_full: bool = False) -> dict:
    """فلترة الطلبات الجديدة فقط التي أُضيفت بعد آخر ObjectId مسجل في الـ Watermark"""
    if force_full or not watermark_doc:
        return {}

    last_oid_str = watermark_doc.get("last_object_id")
    if last_oid_str:
        try:
            return {"_id": {"$gt": ObjectId(last_oid_str)}}
        except Exception:
            return {}
    return {}


def _get_latest_source_markers(validated_col):
    """جلب أحدث _id وآخر وقت تحديث في مجموعة orders_validated"""
    latest_doc = validated_col.find_one(
        {},
        projection={"_id": 1, "last_updated_at": 1},
        sort=[("_id", DESCENDING)]
    )
    if not latest_doc:
        return None, None
    return latest_doc.get("last_updated_at"), str(latest_doc.get("_id"))


def refresh_daily_sales_summary(db=None, force_full: bool = False) -> dict:
    """تحديث العرض المادي الأول المستقل: daily_sales_summary تزايدياً للسجلات الجديدة فقط"""
    own_client = None
    if db is None:
        own_client, db = get_db()
        _ensure_mv_indexes(db)

    try:
        validated_col = db[settings.VALIDATED_COLLECTION]
        mv_col = db[settings.MV_DAILY_SALES]
        wm_col = db[settings.MV_WATERMARKS]

        view_name = settings.MV_DAILY_SALES
        wm_doc = wm_col.find_one({"view_name": view_name})
        is_empty_mv = (mv_col.count_documents({}) == 0)

        effective_full = force_full or is_empty_mv or (wm_doc is None) or (not wm_doc.get("last_object_id"))
        if effective_full:
            mv_col.delete_many({})
            delta_match = {}
        else:
            delta_match = _build_delta_match(wm_doc, force_full=False)

        delta_docs_count = validated_col.count_documents(delta_match) if delta_match else validated_col.count_documents({})
        now_iso = datetime.now(timezone.utc).isoformat()

        if delta_docs_count == 0 and not effective_full:
            return {
                "view_name": view_name,
                "refresh_mode": "incremental_noop",
                "delta_records_processed": 0,
                "upserted_or_modified_groups": 0,
                "total_view_documents": mv_col.count_documents({}),
                "last_refreshed_at": now_iso
            }

        latest_ts, latest_oid = _get_latest_source_markers(validated_col)

        pipeline = []
        if delta_match:
            pipeline.append({"$match": delta_match})

        pipeline.extend([
            {"$addFields": {
                "day_period": {
                    "$substrBytes": [{"$ifNull": ["$order_date", "1970-01-01"]}, 0, 10]
                }
            }},
            {"$group": {
                "_id": "$day_period",
                "delta_orders": {"$sum": 1},
                "delta_revenue": {"$sum": {"$ifNull": ["$total_amount", 0.0]}},
                "delta_delivery_cost": {"$sum": {"$ifNull": ["$delivery_cost", 0.0]}}
            }}
        ])

        aggregated_deltas = list(validated_col.aggregate(pipeline, allowDiskUse=True))
        bulk_ops = []

        for row in aggregated_deltas:
            period_date = row["_id"] or "1970-01-01"
            d_orders = int(row.get("delta_orders", 0))
            d_rev = round(float(row.get("delta_revenue", 0.0)), 2)
            d_deliv = round(float(row.get("delta_delivery_cost", 0.0)), 2)

            bulk_ops.append(
                UpdateOne(
                    {"period_date": period_date},
                    {
                        "$inc": {
                            "total_orders": d_orders,
                            "daily_revenue": d_rev,
                            "total_delivery_cost": d_deliv
                        },
                        "$set": {
                            "last_refreshed_at": now_iso
                        }
                    },
                    upsert=True
                )
            )

        modified_groups = 0
        if bulk_ops:
            res = mv_col.bulk_write(bulk_ops, ordered=False)
            modified_groups = res.upserted_count + res.modified_count

        wm_col.update_one(
            {"view_name": view_name},
            {"$set": {
                "view_name": view_name,
                "last_updated_at": latest_ts or now_iso,
                "last_object_id": latest_oid,
                "last_refreshed_at": now_iso,
                "last_mode": "initial_seed" if effective_full else "incremental_delta",
                "last_delta_records": delta_docs_count
            }},
            upsert=True
        )

        return {
            "view_name": view_name,
            "refresh_mode": "initial_seed" if effective_full else "incremental_delta",
            "delta_records_processed": delta_docs_count,
            "upserted_or_modified_groups": modified_groups,
            "total_view_documents": mv_col.count_documents({}),
            "last_refreshed_at": now_iso
        }
    finally:
        if own_client is not None:
            own_client.close()


def refresh_top_products_summary(db=None, force_full: bool = False) -> dict:
    """تحديث العرض المادي الثاني المستقل: top_products_summary تزايدياً للسجلات الجديدة فقط"""
    own_client = None
    if db is None:
        own_client, db = get_db()
        _ensure_mv_indexes(db)

    try:
        validated_col = db[settings.VALIDATED_COLLECTION]
        mv_col = db[settings.MV_TOP_PRODUCTS]
        wm_col = db[settings.MV_WATERMARKS]

        view_name = settings.MV_TOP_PRODUCTS
        wm_doc = wm_col.find_one({"view_name": view_name})
        is_empty_mv = (mv_col.count_documents({}) == 0)

        effective_full = force_full or is_empty_mv or (wm_doc is None) or (not wm_doc.get("last_object_id"))
        if effective_full:
            mv_col.delete_many({})
            delta_match = {}
        else:
            delta_match = _build_delta_match(wm_doc, force_full=False)

        delta_docs_count = validated_col.count_documents(delta_match) if delta_match else validated_col.count_documents({})
        now_iso = datetime.now(timezone.utc).isoformat()

        if delta_docs_count == 0 and not effective_full:
            return {
                "view_name": view_name,
                "refresh_mode": "incremental_noop",
                "delta_records_processed": 0,
                "upserted_or_modified_groups": 0,
                "total_view_documents": mv_col.count_documents({}),
                "last_refreshed_at": now_iso
            }

        latest_ts, latest_oid = _get_latest_source_markers(validated_col)

        pipeline = []
        if delta_match:
            pipeline.append({"$match": delta_match})

        pipeline.extend([
            {"$unwind": "$items"},
            {"$group": {
                "_id": {"$ifNull": ["$items.item_name", "Unknown_Item"]},
                "delta_quantity_sold": {"$sum": {"$ifNull": ["$items.quantity", 0]}},
                "delta_product_revenue": {"$sum": {"$ifNull": ["$items.subtotal", 0.0]}},
                "delta_order_count": {"$sum": 1}
            }}
        ])

        aggregated_deltas = list(validated_col.aggregate(pipeline, allowDiskUse=True))
        bulk_ops = []

        for row in aggregated_deltas:
            product_name = row["_id"] or "Unknown_Item"
            d_qty = int(row.get("delta_quantity_sold", 0))
            d_rev = round(float(row.get("delta_product_revenue", 0.0)), 2)
            d_cnt = int(row.get("delta_order_count", 0))

            bulk_ops.append(
                UpdateOne(
                    {"product_name": product_name},
                    {
                        "$inc": {
                            "total_quantity_sold": d_qty,
                            "total_product_revenue": d_rev,
                            "order_count": d_cnt
                        },
                        "$set": {
                            "last_refreshed_at": now_iso
                        }
                    },
                    upsert=True
                )
            )

        modified_groups = 0
        if bulk_ops:
            res = mv_col.bulk_write(bulk_ops, ordered=False)
            modified_groups = res.upserted_count + res.modified_count

        wm_col.update_one(
            {"view_name": view_name},
            {"$set": {
                "view_name": view_name,
                "last_updated_at": latest_ts or now_iso,
                "last_object_id": latest_oid,
                "last_refreshed_at": now_iso,
                "last_mode": "initial_seed" if effective_full else "incremental_delta",
                "last_delta_records": delta_docs_count
            }},
            upsert=True
        )

        return {
            "view_name": view_name,
            "refresh_mode": "initial_seed" if effective_full else "incremental_delta",
            "delta_records_processed": delta_docs_count,
            "upserted_or_modified_groups": modified_groups,
            "total_view_documents": mv_col.count_documents({}),
            "last_refreshed_at": now_iso
        }
    finally:
        if own_client is not None:
            own_client.close()


def refresh_single_materialized_view(view_name: str, force_full: bool = False) -> dict:
    """تحديث عرض مادي واحد محدد بشكل مستقل وإرجاع إحصائياته وعيناته"""
    client, db = get_db()
    try:
        _ensure_mv_indexes(db)
        if view_name == settings.MV_DAILY_SALES or view_name == "daily_sales_summary":
            metrics = refresh_daily_sales_summary(db, force_full=force_full)
            sample = list(db[settings.MV_DAILY_SALES].find({}, {"_id": 0}).sort("period_date", DESCENDING).limit(10))
            wm = db[settings.MV_WATERMARKS].find_one({"view_name": settings.MV_DAILY_SALES}, {"_id": 0})
            return {
                "status": "success",
                "view_name": settings.MV_DAILY_SALES,
                "metrics": metrics,
                "watermark": wm,
                "sample_top_rows": sample,
            }
        elif view_name == settings.MV_TOP_PRODUCTS or view_name == "top_products_summary":
            metrics = refresh_top_products_summary(db, force_full=force_full)
            sample = list(db[settings.MV_TOP_PRODUCTS].find({}, {"_id": 0}).sort("total_product_revenue", DESCENDING).limit(10))
            wm = db[settings.MV_WATERMARKS].find_one({"view_name": settings.MV_TOP_PRODUCTS}, {"_id": 0})
            return {
                "status": "success",
                "view_name": settings.MV_TOP_PRODUCTS,
                "metrics": metrics,
                "watermark": wm,
                "sample_top_rows": sample,
            }
        else:
            raise ValueError(f"Unknown Materialized View '{view_name}'. Allowed: daily_sales_summary, top_products_summary")
    finally:
        client.close()


def refresh_all_materialized_views(force_full: bool = False) -> dict:
    """تحديث جميع العروض المادية وإرجاع ملخص التحديث والعينات"""
    client, db = get_db()
    try:
        _ensure_mv_indexes(db)
        daily_summary = refresh_daily_sales_summary(db, force_full=force_full)
        products_summary = refresh_top_products_summary(db, force_full=force_full)

        daily_sample = list(
            db[settings.MV_DAILY_SALES]
            .find({}, {"_id": 0})
            .sort("period_date", DESCENDING)
            .limit(10)
        )
        products_sample = list(
            db[settings.MV_TOP_PRODUCTS]
            .find({}, {"_id": 0})
            .sort("total_product_revenue", DESCENDING)
            .limit(10)
        )
        watermarks = list(db[settings.MV_WATERMARKS].find({}, {"_id": 0}))

        return {
            "status": "success",
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
            "views": {
                settings.MV_DAILY_SALES: {
                    "metrics": daily_summary,
                    "sample_top_rows": daily_sample
                },
                settings.MV_TOP_PRODUCTS: {
                    "metrics": products_summary,
                    "sample_top_rows": products_sample
                }
            },
            "watermarks": watermarks
        }
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh Materialized Views Incrementally")
    parser.add_argument(
        "--view",
        choices=["daily_sales_summary", "top_products_summary", "all"],
        default="all",
        help="Name of the Materialized View to refresh independently or 'all'",
    )
    parser.add_argument("--force-full", action="store_true", help="Force full rebuild instead of incremental")
    args = parser.parse_args()

    if args.view == "all":
        result = refresh_all_materialized_views(force_full=args.force_full)
    else:
        result = refresh_single_materialized_view(args.view, force_full=args.force_full)
    print(json.dumps(result, ensure_ascii=False, indent=2))