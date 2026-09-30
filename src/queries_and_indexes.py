import json
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING, DESCENDING
from config import settings


def get_validated_collection():
    client = MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DATABASE]
    return client, db[settings.VALIDATED_COLLECTION]


def _get_dynamic_defaults(col):
    """سحب قيم ديناميكية من البيانات الفعلية لضمان عدم الاعتماد على قيم ثابتة أثناء الاختبار"""
    sample = col.find_one({"quality_status": {"$in": ["valid", "corrected"]}}) or col.find_one({}) or {}
    items = sample.get("items") or []
    first_item_name = (
        items[0].get("item_name")
        if (items and isinstance(items, list) and isinstance(items[0], dict) and items[0].get("item_name"))
        else "SKU-100"
    )

    sample_amount = float(sample.get("total_amount") or 1000.0)
    min_amt = max(0.0, round(sample_amount * 0.5, 2))
    max_amt = round(sample_amount * 5.0 + 1000.0, 2)

    return {
        "city": sample.get("city") or "صنعاء",
        "status": sample.get("status") or "confirmed",
        "customer_id": sample.get("customer_id") or "CUST-1001",
        "customer_phone": sample.get("customer_phone") or "+967770000000",
        "min_amount": min_amt,
        "max_amount": max_amt,
        "item_name": first_item_name,
    }


def build_query_definitions(col, params=None):
    """تعريف 5 استعلامات عملية مناسبة لبيانات المشروع"""
    defaults = _get_dynamic_defaults(col)
    if params:
        defaults.update({k: v for k, v in params.items() if v is not None})

    return {
        "city_status_recent_orders": {
            "title": "1. طلبات مدينة محددة حسب الحالة مرتبة بالأحدث",
            "description": "يجلب أحدث الطلبات في مدينة معينة وحالة محددة لعمليات التوصيل والمتابعة.",
            "filter": {
                "city": defaults["city"],
                "status": defaults["status"]
            },
            "projection": {
                "_id": 0, "order_id": 1, "city": 1, "status": 1,
                "order_date": 1, "total_amount": 1, "customer_id": 1
            },
            "sort": [("order_date", DESCENDING)],
            "limit": 25,
            "served_by_index": "idx_city_status_order_date (Compound Index)"
        },
        "customer_order_history": {
            "title": "2. سجل طلبات عميل محدد",
            "description": "يسترجع كامل سجل الطلبات لعميل معين مرتباً زمنياً لخدمة العملاء.",
            "filter": {
                "customer_id": defaults["customer_id"]
            },
            "projection": {
                "_id": 0, "order_id": 1, "customer_id": 1, "order_date": 1,
                "status": 1, "total_amount": 1, "payment_status": 1
            },
            "sort": [("order_date", DESCENDING)],
            "limit": 25,
            "served_by_index": "idx_customer_id_order_date (Compound Index)"
        },
        "high_value_orders_range": {
            "title": "3. الطلبات ذات القيمة المالية ضمن نطاق محدد",
            "description": "يبحث عن الطلبات التي تقع قيمتها الإجمالية ضمن شريحة مالية معينة للتدقيق المالي.",
            "filter": {
                "total_amount": {
                    "$gte": float(defaults["min_amount"]),
                    "$lte": float(defaults["max_amount"])
                }
            },
            "projection": {
                "_id": 0, "order_id": 1, "total_amount": 1, "currency": 1,
                "city": 1, "status": 1, "order_date": 1
            },
            "sort": [("total_amount", DESCENDING)],
            "limit": 25,
            "served_by_index": "idx_total_amount (Single Field Index)"
        },
        "lookup_by_customer_phone": {
            "title": "4. البحث عن الطلبات عبر رقم الهاتف الموحد",
            "description": "يستعلم عن الطلبات المرتبطة برقم هاتف يمني موحد (+967) للتحقق السريع.",
            "filter": {
                "customer_phone": defaults["customer_phone"]
            },
            "projection": {
                "_id": 0, "order_id": 1, "customer_phone": 1, "customer_id": 1,
                "city": 1, "total_amount": 1, "status": 1
            },
            "sort": [("order_date", DESCENDING)],
            "limit": 25,
            "served_by_index": "idx_city_status_order_date / collection lookup"
        },
        "orders_by_product_item": {
            "title": "5. الطلبات التي تحتوي على منتج محدد داخل مصفوفة العناصر",
            "description": "يفحص مصفوفة items لاستخراج الطلبات المتضمنة لمنتج أو SKU معين.",
            "filter": {
                "items.item_name": defaults["item_name"]
            },
            "projection": {
                "_id": 0, "order_id": 1, "order_date": 1, "city": 1,
                "total_amount": 1, "items": 1
            },
            "sort": None,
            "limit": 25,
            "served_by_index": "Multikey / Document Filter"
        }
    }


def list_available_queries():
    """إرجاع قائمة بأسماء وتفاصيل الاستعلامات الخمسة المتاحة"""
    client, col = get_validated_collection()
    try:
        defs = build_query_definitions(col)
        return [
            {
                "name": name,
                "title": meta["title"],
                "description": meta["description"],
                "sample_filter": meta["filter"],
                "served_by_index": meta["served_by_index"]
            }
            for name, meta in defs.items()
        ]
    finally:
        client.close()


def execute_named_query(query_name: str, params: dict = None, limit: int = 25):
    """تنفيذ استعلام محدد بالاسم وإرجاع النتائج الفعلية من قاعدة البيانات"""
    client, col = get_validated_collection()
    try:
        defs = build_query_definitions(col, params=params)
        if query_name not in defs:
            raise ValueError(f"Unknown query '{query_name}'. Available: {list(defs.keys())}")

        q = defs[query_name]
        cursor = col.find(q["filter"], q["projection"])
        if q["sort"]:
            cursor = cursor.sort(q["sort"])
        cursor = cursor.limit(int(limit or q["limit"]))

        results = list(cursor)
        return {
            "query_name": query_name,
            "title": q["title"],
            "description": q["description"],
            "applied_filter": q["filter"],
            "count_returned": len(results),
            "results": results
        }
    finally:
        client.close()


CUSTOM_INDEX_SPECS = [
    {
        "name": "idx_city_status_order_date",
        "keys": [("city", ASCENDING), ("status", ASCENDING), ("order_date", DESCENDING)],
        "type": "Compound Index",
        "reason": "يخدم الاستعلامات المركبة حسب المدينة والحالة مع الترتيب التنازلي بالتاريخ (ESR Rule) لمنع المسح الكامل والفرز في الذاكرة."
    },
    {
        "name": "idx_customer_id_order_date",
        "keys": [("customer_id", ASCENDING), ("order_date", DESCENDING)],
        "type": "Compound Index",
        "reason": "يسرّع استرجاع سجل طلبات العميل مرتباً زمنياً ويحول البحث من COLLSCAN إلى IXSCAN."
    },
    {
        "name": "idx_total_amount",
        "keys": [("total_amount", DESCENDING)],
        "type": "Single Field Index",
        "reason": "يخدم استعلامات النطاق المالي ($gte / $lte) والفرز التنازلي حسب إجمالي مبلغ الطلب."
    }
]


def _extract_explain_metrics(explain_doc: dict) -> dict:
    """استخراج مؤشرات الأداء من مخرجات explain('executionStats')"""
    exec_stats = explain_doc.get("executionStats", {})
    query_planner = explain_doc.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    def _find_stages(node):
        stages = []
        if isinstance(node, dict):
            if "stage" in node:
                stages.append(node["stage"])
            for v in node.values():
                stages.extend(_find_stages(v))
        elif isinstance(node, list):
            for item in node:
                stages.extend(_find_stages(item))
        return stages

    stages = _find_stages(winning_plan)
    scan_type = "IXSCAN" if "IXSCAN" in stages else ("COLLSCAN" if "COLLSCAN" in stages else "/".join(stages))

    return {
        "scan_stage": scan_type,
        "all_stages": stages,
        "executionTimeMillis": exec_stats.get("executionTimeMillis", 0),
        "totalKeysExamined": exec_stats.get("totalKeysExamined", 0),
        "totalDocsExamined": exec_stats.get("totalDocsExamined", 0),
        "nReturned": exec_stats.get("nReturned", 0),
    }


def _run_explain_for_three_queries(col, defs):
    """تشغيل explain('executionStats') على 3 استعلامات رئيسية"""
    target_queries = [
        "city_status_recent_orders",
        "customer_order_history",
        "high_value_orders_range"
    ]
    metrics = {}
    for q_name in target_queries:
        q = defs[q_name]
        find_cmd = {
            "find": col.name,
            "filter": q["filter"],
            "projection": q["projection"],
            "limit": int(q["limit"])
        }
        if q["sort"]:
            find_cmd["sort"] = dict(q["sort"])

        raw_explain = col.database.command("explain", find_cmd, verbosity="executionStats")
        metrics[q_name] = _extract_explain_metrics(raw_explain)
    return metrics


def create_indexes_and_benchmark_explain():
    """
    1. يحذف الفهارس المخصصة الثلاثة مؤقتاً لقياس الأداء قبل الفهرسة (Before Indexes).
    2. ينشئ الفهارس الثلاثة (منها Compound Indexes).
    3. يقيس الأداء بعد الفهرسة (After Indexes) باستخدام explain('executionStats').
    4. يحفظ التقرير في reports/index_explain_report.json ويعيده للـ API.
    """
    client, col = get_validated_collection()
    try:
        defs = build_query_definitions(col)

        existing_indexes = col.index_information()
        for spec in CUSTOM_INDEX_SPECS:
            if spec["name"] in existing_indexes:
                col.drop_index(spec["name"])

        before_stats = _run_explain_for_three_queries(col, defs)

        created_indexes = []
        for spec in CUSTOM_INDEX_SPECS:
            idx_name = col.create_index(spec["keys"], name=spec["name"])
            created_indexes.append({
                "index_name": idx_name,
                "keys": spec["keys"],
                "type": spec["type"],
                "reason": spec["reason"]
            })

        after_stats = _run_explain_for_three_queries(col, defs)

        comparisons = []
        for q_name in before_stats.keys():
            b = before_stats[q_name]
            a = after_stats[q_name]
            comparisons.append({
                "query_name": q_name,
                "query_title": defs[q_name]["title"],
                "filter_used": defs[q_name]["filter"],
                "served_by_index": defs[q_name]["served_by_index"],
                "before_index": b,
                "after_index": a,
                "impact_summary": (
                    f"تحول المسح من {b['scan_stage']} إلى {a['scan_stage']}، "
                    f"وانخفض عدد المستندات المفحوصة من {b['totalDocsExamined']} إلى {a['totalDocsExamined']}، "
                    f"وتغير زمن التنفيذ من {b['executionTimeMillis']}ms إلى {a['executionTimeMillis']}ms."
                )
            })

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database": settings.MONGO_DATABASE,
            "collection": settings.VALIDATED_COLLECTION,
            "indexes_created": created_indexes,
            "explain_comparisons": comparisons
        }

        report_path = settings.REPORTS_DIR / "index_explain_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        return report
    finally:
        client.close()


if __name__ == "__main__":
    result = create_indexes_and_benchmark_explain()
    print(json.dumps(result, ensure_ascii=False, indent=2))