"""
Data Quality Rules Engine - Pure Python Schema Validator
Optimized for Accurate Valid vs Corrected Classification.
"""
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

ARABIC_DIGITS_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
ARABIC_DECIMAL_MAP = str.maketrans({"٫": ".", "٬": ","})

KNOWN_WORD_NUMBERS = {
    "ثلاثة آلاف": 3000.0, "ثلاثة الاف": 3000.0,
    "أربعة آلاف": 4000.0, "اربعة الاف": 4000.0,
    "خمسة آلاف": 5000.0, "خمسة الاف": 5000.0,
    "عشرة آلاف": 10000.0, "عشرة الاف": 10000.0,
    "ألفان": 2000.0, "الفان": 2000.0, "ألفين": 2000.0, "الفين": 2000.0,
    "ألف": 1000.0, "الف": 1000.0,
}
STATUS_MAPPING = {
    "قيد الانتظار": "pending", "معلق": "pending", "created": "pending", "pending": "pending",
    "مؤكد": "confirmed", "مدفوع": "confirmed", "paid": "confirmed", "confirmed": "confirmed",
    "قيد الشحن": "shipped", "قيد التوصيل": "shipped", "shipped": "shipped", "in_transit": "shipped",
    "تم التسليم": "delivered", "تم التوصيل": "delivered", "delivered": "delivered", "done": "delivered",
    "مرتجع": "returned", "returned": "returned",
    "ملغي": "cancelled", "ملغى": "cancelled", "cancelled": "cancelled", "canceled": "cancelled",
}

PAYMENT_METHOD_MAPPING = {
    "نقداً عند التسليم": "cash_on_delivery", "نقد": "cash_on_delivery", "كاش": "cash_on_delivery",
    "cash": "cash_on_delivery", "cash on delivery": "cash_on_delivery", "cod": "cash_on_delivery",
    "بطاقة": "card", "بطاقة ائتمان": "card", "card": "card", "credit card": "card",
    "محفظة إلكترونية": "wallet", "محفظة": "wallet", "wallet": "wallet", "e-wallet": "wallet",
}

PAYMENT_STATUS_MAPPING = {
    "بانتظار الدفع": "unpaid", "غير مدفوع": "unpaid", "معلق": "unpaid",
    "pending": "unpaid", "unpaid": "unpaid", "waiting": "unpaid",
    "تم الدفع": "paid", "مدفوع": "paid", "paid": "paid", "approved": "paid",
    "مرفوض": "failed", "فشل": "failed", "failed": "failed", "rejected": "failed",
    "مسترد": "refunded", "refunded": "refunded",
}

DELIVERY_MAPPING = {
    "عادي": "standard", "قياسي": "standard", "normal": "standard", "standard": "standard",
    "سريع": "express", "مستعجل": "express", "fast": "express", "express": "express",
}

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[a-zA-Z0-9]+$")


def normalize_arabic_digits(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return value.translate(ARABIC_DIGITS_MAP).translate(ARABIC_DECIMAL_MAP)


def parse_number_field(val: Any) -> Tuple[Optional[float], bool]:
    """ترجع: (الرقم, هل احتاج لتصحيح جذري ككلمات أو نصوص عربية)"""
    if val is None or isinstance(val, bool):
        return None, False
    if isinstance(val, (int, float)):
        return float(val), False
    
    val_str = str(val).strip()
    if not val_str or val_str.lower() in ["none", "null", "nan", ""]:
        return None, False

    # إذا كان رقماً قياسياً صريحاً (مثل "5000" أو "0" أو "120.50")
    if re.match(r"^-?\d+(\.\d+)?$", val_str):
        return float(val_str), False

    text = normalize_arabic_digits(val_str)
    # 1. إذا كان مكتوباً بالكلمات العربية (تصحيح جوهري)
    for word, num in KNOWN_WORD_NUMBERS.items():
        if word in text:
            return num, True

    for word in ["ريال يمني", "ريال", "ريالات", "لاير", "لاير يمني", "YER", "yer"]:
        text = text.replace(word, "")

    text = text.replace(",", "").strip()
    cleaned = re.sub(r"[^\d.-]", "", text)
    if cleaned in {"", "-", ".", "-."}:
        return None, False

    try:
        return float(cleaned), True
    except ValueError:
        return None, False


def clean_order(raw_record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[str]]:
    record = {k.strip().replace("\ufeff", ""): v for k, v in raw_record.items()}
    corrections: List[Dict[str, Any]] = []
    quarantine_reasons: List[str] = []

    # 1. order_id
    raw_oid = record.get("order_id")
    if raw_oid is None or str(raw_oid).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("MISSING_ORDER_ID")
    else:
        record["order_id"] = str(raw_oid).strip()

    # 2. customer_id
    raw_cid = record.get("customer_id")
    if raw_cid is None or str(raw_cid).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("MISSING_CUSTOMER_ID")
    else:
        record["customer_id"] = str(raw_cid).strip()

    # 3. order_date
    raw_date = record.get("order_date")
    if raw_date is None or str(raw_date).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("INVALID_IMPOSSIBLE_DATE")
    else:
        date_str = str(raw_date).strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", date_str):
            record["order_date"] = date_str
        else:
            text = normalize_arabic_digits(date_str).replace("/", "-").replace(".", "-")
            parsed = None
            for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"]:
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            if not parsed or parsed.year < 2015 or parsed.year > 2030:
                quarantine_reasons.append("INVALID_IMPOSSIBLE_DATE")
            else:
                record["order_date"] = parsed.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4. Status mappings (توحيد الحالة)
    for field, mapping, rule in [("status", STATUS_MAPPING, "STATUS_SYNONYM_MAPPING"),
                                 ("payment_status", PAYMENT_STATUS_MAPPING, "PAYMENT_STATUS_MAPPING"),
                                 ("payment_method", PAYMENT_METHOD_MAPPING, "PAYMENT_METHOD_MAPPING"),
                                 ("delivery_type", DELIVERY_MAPPING, "DELIVERY_TYPE_MAPPING")]:
        val = record.get(field)
        if val is not None:
            clean_v = str(val).strip()
            record[field] = mapping.get(clean_v.lower(), mapping.get(clean_v, clean_v))

    # 5. customer_phone
    raw_phone = record.get("customer_phone")
    if raw_phone is not None and str(raw_phone).strip():
        ph_str = str(raw_phone).strip()
        if re.match(r"^\+967[7]\d{8}$", ph_str):
            record["customer_phone"] = ph_str
        else:
            digits = re.sub(r"\D", "", normalize_arabic_digits(ph_str))
            if digits.startswith("00967"):
                digits = digits[5:]
            elif digits.startswith("967"):
                digits = digits[3:]
            record["customer_phone"] = f"+967{digits}" if (len(digits) == 9 and digits.startswith("7")) else f"+{digits}"

    # 6. customer_email
    raw_email = record.get("customer_email")
    if raw_email is not None and str(raw_email).strip():
        em_str = str(raw_email).strip()
        if EMAIL_REGEX.match(em_str):
            record["customer_email"] = em_str
        else:
            cleaned_em = em_str.lower().replace(" ", "")
            cleaned_em = re.sub(r"@{2,}", "@", cleaned_em)
            cleaned_em = re.sub(r"\.{2,}", ".", cleaned_em)
            if EMAIL_REGEX.match(cleaned_em):
                record["customer_email"] = cleaned_em
            else:
                record["customer_email"] = None
                corrections.append({"field": "customer_email", "original_value": raw_email, "corrected_value": None, "rule_code": "EMAIL_INVALID_CLEARED"})

    # 7. Numeric fields (فقط إذا كان تصحيحاً جذرياً يسجل في corrections)
    raw_dc = record.get("delivery_cost")
    dc_val, dc_chg = parse_number_field(raw_dc)
    record["delivery_cost"] = dc_val if dc_val is not None else 0.0
    if dc_chg:
        corrections.append({"field": "delivery_cost", "original_value": raw_dc, "corrected_value": record["delivery_cost"], "rule_code": "NUMBER_WORD_REPAIRED"})

    raw_pa = record.get("payment_amount")
    pa_val, pa_chg = parse_number_field(raw_pa)
    record["payment_amount"] = pa_val
    if pa_chg:
        corrections.append({"field": "payment_amount", "original_value": raw_pa, "corrected_value": pa_val, "rule_code": "NUMBER_WORD_REPAIRED"})

    raw_ta = record.get("total_amount")
    ta_val, ta_chg = parse_number_field(raw_ta)
    record["total_amount"] = ta_val
    if ta_chg:
        corrections.append({"field": "total_amount", "original_value": raw_ta, "corrected_value": ta_val, "rule_code": "NUMBER_WORD_REPAIRED"})

    if (record["delivery_cost"] < 0) or (pa_val is not None and pa_val < 0) or (ta_val is not None and ta_val < 0):
        quarantine_reasons.append("AMBIGUOUS_NEGATIVE_VALUE")

    # 8. Items Extraction
    items_raw = record.get("items_json")
    if items_raw is None or str(items_raw).strip() in ["", "[]", "null", "None", "nan"]:
        quarantine_reasons.append("EMPTY_ITEMS")
    else:
        text = str(items_raw).strip()
        parsed_items = None
        try:
            cleaned_text = text.replace('""', '"').strip('"').strip("'")
            data = json.loads(cleaned_text)
            if isinstance(data, dict):
                data = [data]
            if isinstance(data, list) and len(data) > 0:
                parsed_items = []
                for it in data:
                    q_val, _ = parse_number_field(it.get("quantity", it.get("qty", 1)))
                    p_val, _ = parse_number_field(it.get("price", it.get("unit_price", 0)))
                    q = int(q_val or 1)
                    p = float(p_val or 0.0)
                    parsed_items.append({
                        "item_name": str(it.get("item_name", it.get("sku", "Product_Item"))),
                        "quantity": q,
                        "unit_price": p,
                        "subtotal": round(q * p, 2),
                    })
        except Exception:
            pass

        if parsed_items:
            record["items"] = parsed_items
        else:
            # إصلاح الـ SKU التالف (تصحيح جوهري يسجل في corrections)
            sku_m = re.search(r'SKU-?\w+', text, re.IGNORECASE)
            if sku_m:
                sku_code = sku_m.group(0).upper()
                eff_tot = ta_val if ta_val is not None else 0.0
                sub_tot = max(0.0, round(eff_tot - record["delivery_cost"], 2))
                record["items"] = [{
                    "item_name": sku_code,
                    "quantity": 1,
                    "unit_price": sub_tot,
                    "subtotal": sub_tot,
                }]
                corrections.append({"field": "items_json", "original_value": items_raw, "corrected_value": record["items"], "rule_code": "REPAIR_CORRUPTED_JSON_SKU"})
            else:
                quarantine_reasons.append("CORRUPTED_ITEMS_JSON")

    # 9. Currency
    curr = record.get("currency")
    if curr is None or str(curr).strip() == "":
        record["currency"] = "YER"
    else:
        c_up = str(curr).strip().upper()
        if c_up in ["YER", "SAR", "USD"]:
            record["currency"] = c_up
        else:
            record["currency"] = "YER"
            corrections.append({"field": "currency", "original_value": curr, "corrected_value": "YER", "rule_code": "DEFAULT_CURRENCY"})

    quarantine_reasons = list(dict.fromkeys(quarantine_reasons))

    # تحديد الحالة بدقة تامة
    if quarantine_reasons:
        record["quality_status"] = "quarantined"
    elif len(corrections) > 0:
        record["quality_status"] = "corrected"
        record["corrections"] = corrections
    else:
        record["quality_status"] = "valid"
        record["corrections"] = []

    return record, corrections, quarantine_reasons