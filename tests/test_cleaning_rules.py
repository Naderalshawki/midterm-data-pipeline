import pytest
from src.quality_rules import (
    normalize_arabic_digits,
    parse_number_field,
    clean_order,
)


def test_rule1_arabic_digits():
    """Rule 1: Arabic Indic Digits to Latin (Section 6.6)"""
    assert normalize_arabic_digits("٥٠٠٠") == "5000"
    assert normalize_arabic_digits("١٢٣") == "123"


def test_rule2_word_numbers_and_currency():
    """Rule 2 & 4: Word numbers and currency stripping (Section 6.6)"""
    val, changed = parse_number_field("خمسة آلاف ريال")
    assert val == 5000.0
    assert changed is True

    val2, changed2 = parse_number_field("ألفان")
    assert val2 == 2000.0
    assert changed2 is True


def test_rule3_thousands_separators():
    """Rule 3: Coerce numbers with commas (Section 6.6)"""
    val, changed = parse_number_field("125,000.00")
    assert val == 125000.0


def test_rule5_to_8_clean_order_pipeline():
    """Rules 5-8: Phone, Email, Date, Status in Full Record (Section 6.6)"""
    raw_doc = {
        "order_id": "ORD-TEST-1",
        "order_date": "31/01/2025",
        "status": "paid",
        "customer_id": "CUST-01",
        "customer_name": "علي",
        "customer_phone": "00967771234567",
        "customer_email": "user@@mail..com",
        "city": "صنعاء",
        "district": "السبعين",
        "delivery_type": "سريع",
        "delivery_cost": "2,000",
        "payment_method": "نقد",
        "payment_status": "مدفوع",
        "payment_amount": "٥٠٠٠ ريال",
        "currency": "YER",
        "total_amount": "7000",
        "items_json": '[{"item_name": "Item A", "price": 5000, "qty": 1}]',
    }
    cleaned_rec, corrections, quarantine_reasons = clean_order(raw_doc)

    assert len(quarantine_reasons) == 0
    assert cleaned_rec["customer_phone"] == "+967771234567"
    assert cleaned_rec["customer_email"] == "user@mail.com"
    assert cleaned_rec["status"] == "confirmed"
    assert cleaned_rec["order_date"] == "2025-01-31T00:00:00Z"
    assert cleaned_rec["delivery_type"] == "express"
    assert cleaned_rec["payment_method"] == "cash_on_delivery"