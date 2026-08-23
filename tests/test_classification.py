import pytest
from src.quality_rules import clean_order


def test_classification_valid_record():
    """Test standard clean record remains valid without corrections"""
    raw_doc = {
        "order_id": "ORD-VALID-1",
        "order_date": "2025-01-31T00:00:00Z",
        "status": "confirmed",
        "customer_id": "CUST-01",
        "customer_name": "أحمد",
        "customer_phone": "+967771234567",
        "customer_email": "ahmed@example.com",
        "city": "صنعاء",
        "district": "السبعين",
        "delivery_type": "standard",
        "delivery_cost": "1500",
        "payment_method": "cash_on_delivery",
        "payment_status": "paid",
        "payment_amount": "5000",
        "currency": "YER",
        "total_amount": "6500",
        "items_json": '[{"item_name": "Item 1", "price": 5000, "qty": 1}]',
    }
    cleaned_rec, corrections, quarantine = clean_order(raw_doc)
    assert len(quarantine) == 0
    assert len(corrections) == 0
    assert cleaned_rec["quality_status"] == "valid"


def test_classification_corrected_record():
    """Test dirty record gets corrected and maintains audit trail"""
    raw_doc = {
        "order_id": "ORD-CORR-1",
        "order_date": "2025-01-31T00:00:00Z",
        "status": "confirmed",
        "customer_id": "CUST-02",
        "customer_name": "سالم",
        "customer_phone": "+967771234567",
        "customer_email": "user@@mail..com",  # dirty email
        "city": "عدن",
        "district": "كريتر",
        "delivery_type": "standard",
        "delivery_cost": "خمسة آلاف ريال",  # word number
        "payment_method": "cash_on_delivery",
        "payment_status": "paid",
        "payment_amount": "5000",
        "currency": "UNKNOWN_CURR",  # invalid currency
        "total_amount": "10000",
        "items_json": '[{"item_name": "Item 2", "price": 5000, "qty": 1}]',
    }
    cleaned_rec, corrections, quarantine = clean_order(raw_doc)
    assert len(quarantine) == 0
    assert len(corrections) > 0
    assert cleaned_rec["quality_status"] == "corrected"


def test_classification_quarantined_record():
    """Test record missing mandatory keys goes to quarantine"""
    raw_doc = {
        "order_id": "",  # Missing mandatory ID
        "customer_id": "CUST-03",
        "items_json": "[]",
    }
    cleaned_rec, corrections, quarantine = clean_order(raw_doc)
    assert "MISSING_ORDER_ID" in quarantine
    assert cleaned_rec["quality_status"] == "quarantined"