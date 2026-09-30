"""
Phase 2 - Requirement 2: Aggregation Reports (5 Analytical Pipelines)
=====================================================================
Implements 5 independent MongoDB Aggregation Pipelines on `orders_validated`:
1. sales_by_city        : المبيعات وعدد الطلبات ومتوسط الطلب حسب المدينة
2. top_products         : أفضل المنتجات مبيعاً وإيراداً (عبر $unwind لمصفوفة items)
3. top_customers        : أفضل العملاء إنفاقاً وعدد طلباتهم
4. sales_by_period      : المبيعات اليومية/الشهرية حسب الفترة الزمنية
5. orders_by_status     : توزيع الطلبات والإيرادات حسب الحالة وطريقة الدفع
"""
import argparse
import json
from pymongo import MongoClient
from config import settings


def get_validated_collection():
    client = MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DATABASE]
    return client, db[settings.VALIDATED_COLLECTION]


def get_aggregation_pipelines(limit: int = 20):
    """تعريف الـ Pipelines الخمسة للتقارير التجميعية"""
    return {
        "sales_by_city": {
            "title": "1. تقرير المبيعات حسب المدينة (Sales by City)",
            "description": "يحسب إجمالي المبيعات، عدد الطلبات، ومتوسط قيمة الطلب لكل مدينة.",
            "pipeline": [
                {"$group": {
                    "_id": {"$ifNull": ["$city", "Unknown"]},
                    "total_orders": {"$sum": 1},
                    "total_revenue": {"$sum": {"$ifNull": ["$total_amount", 0.0]}},
                    "avg_order_value": {"$avg": {"$ifNull": ["$total_amount", 0.0]}},
                    "total_delivery_cost": {"$sum": {"$ifNull": ["$delivery_cost", 0.0]}}
                }},
                {"$project": {
                    "_id": 0,
                    "city": "$_id",
                    "total_orders": 1,
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                    "total_delivery_cost": {"$round": ["$total_delivery_cost", 2]}
                }},
                {"$sort": {"total_revenue": -1}},
                {"$limit": int(limit)}
            ]
        },
        "top_products": {
            "title": "2. تقرير أفضل المنتجات مبيعاً (Top Selling Products)",
            "description": "يفكك مصفوفة items لحساب إجمالي الكميات المباعة والإيرادات لكل منتج.",
            "pipeline": [
                {"$unwind": "$items"},
                {"$group": {
                    "_id": {"$ifNull": ["$items.item_name", "Unknown_Item"]},
                    "total_quantity_sold": {"$sum": {"$ifNull": ["$items.quantity", 0]}},
                    "total_product_revenue": {"$sum": {"$ifNull": ["$items.subtotal", 0.0]}},
                    "order_count": {"$sum": 1}
                }},
                {"$project": {
                    "_id": 0,
                    "product_name": "$_id",
                    "total_quantity_sold": 1,
                    "total_product_revenue": {"$round": ["$total_product_revenue", 2]},
                    "order_count": 1
                }},
                {"$sort": {"total_product_revenue": -1}},
                {"$limit": int(limit)}
            ]
        },
        "top_customers": {
            "title": "3. تقرير أفضل العملاء إنفاقاً (Top Customers)",
            "description": "يحدد أعلى العملاء من حيث إجمالي الإنفاق وعدد الطلبات.",
            "pipeline": [
                {"$group": {
                    "_id": "$customer_id",
                    "customer_phone": {"$first": "$customer_phone"},
                    "city": {"$first": "$city"},
                    "orders_count": {"$sum": 1},
                    "total_spent": {"$sum": {"$ifNull": ["$total_amount", 0.0]}},
                    "last_order_date": {"$max": "$order_date"}
                }},
                {"$project": {
                    "_id": 0,
                    "customer_id": "$_id",
                    "customer_phone": 1,
                    "city": 1,
                    "orders_count": 1,
                    "total_spent": {"$round": ["$total_spent", 2]},
                    "last_order_date": 1
                }},
                {"$sort": {"total_spent": -1}},
                {"$limit": int(limit)}
            ]
        },
        "sales_by_period": {
            "title": "4. تقرير المبيعات حسب الفترة الزمنية (Sales by Date Period)",
            "description": "يجمع المبيعات وعدد الطلبات يومياً (YYYY-MM-DD) لمتابعة الأداء الزمني.",
            "pipeline": [
                {"$addFields": {
                    "day_period": {
                        "$substrBytes": [{"$ifNull": ["$order_date", "1970-01-01"]}, 0, 10]
                    }
                }},
                {"$group": {
                    "_id": "$day_period",
                    "total_orders": {"$sum": 1},
                    "daily_revenue": {"$sum": {"$ifNull": ["$total_amount", 0.0]}},
                    "avg_order_amount": {"$avg": {"$ifNull": ["$total_amount", 0.0]}}
                }},
                {"$project": {
                    "_id": 0,
                    "period_date": "$_id",
                    "total_orders": 1,
                    "daily_revenue": {"$round": ["$daily_revenue", 2]},
                    "avg_order_amount": {"$round": ["$avg_order_amount", 2]}
                }},
                {"$sort": {"period_date": -1}},
                {"$limit": int(limit)}
            ]
        },
        "orders_by_status": {
            "title": "5. تقرير توزيع الطلبات حسب الحالة (Orders Distribution by Status)",
            "description": "يحلل توزيع الطلبات وقيمتها المالية حسب حالة الطلب (status).",
            "pipeline": [
                {"$group": {
                    "_id": {"$ifNull": ["$status", "unknown"]},
                    "orders_count": {"$sum": 1},
                    "status_revenue": {"$sum": {"$ifNull": ["$total_amount", 0.0]}},
                    "avg_amount": {"$avg": {"$ifNull": ["$total_amount", 0.0]}}
                }},
                {"$project": {
                    "_id": 0,
                    "status": "$_id",
                    "orders_count": 1,
                    "status_revenue": {"$round": ["$status_revenue", 2]},
                    "avg_amount": {"$round": ["$avg_amount", 2]}
                }},
                {"$sort": {"orders_count": -1}}
            ]
        }
    }


def list_available_aggregations():
    """إرجاع قائمة بجميع تقارير الـ Aggregation الخمسة المتاحة"""
    pipelines = get_aggregation_pipelines()
    return [
        {
            "name": name,
            "title": meta["title"],
            "description": meta["description"]
        }
        for name, meta in pipelines.items()
    ]


def run_aggregation_report(report_name: str, limit: int = 20):
    """تشغيل تقرير تجميعي محدد بالاسم وإرجاع نتائجه الفعلية"""
    client, col = get_validated_collection()
    try:
        pipelines = get_aggregation_pipelines(limit=limit)
        if report_name not in pipelines:
            raise ValueError(f"Unknown aggregation report '{report_name}'. Available: {list(pipelines.keys())}")

        meta = pipelines[report_name]
        results = list(col.aggregate(meta["pipeline"], allowDiskUse=True))
        return {
            "report_name": report_name,
            "title": meta["title"],
            "description": meta["description"],
            "count_returned": len(results),
            "results": results
        }
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MongoDB Aggregation Reports")
    parser.add_argument("--report", type=str, default="all", help="Name of report or 'all'")
    parser.add_argument("--limit", type=int, default=10, help="Max rows per report")
    args = parser.parse_args()

    if args.report == "all":
        for item in list_available_aggregations():
            res = run_aggregation_report(item["name"], limit=args.limit)
            print(f"\n=== {res['title']} ===")
            print(json.dumps(res["results"][:3], ensure_ascii=False, indent=2))
    else:
        res = run_aggregation_report(args.report, limit=args.limit)
        print(json.dumps(res, ensure_ascii=False, indent=2))