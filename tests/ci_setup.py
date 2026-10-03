"""CI Setup Script for Clean Ubuntu Runner (GitHub Actions)"""
import time
from pathlib import Path
from pymongo import MongoClient

def main():
    for _ in range(20):
        try:
            c = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
            c.admin.command("ping")
            print("[+] MongoDB 7.0 is online and ready!")
            c.close()
            break
        except Exception:
            time.sleep(1)

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    csvs = [f for f in data_dir.glob("*.csv") if f.stat().st_size > 0]
    if not csvs:
        sample = data_dir / "01_student_test_small.csv"
        header = "order_id,order_date,customer_id,customer_phone,customer_email,city,district,status,payment_method,payment_status,delivery_type,delivery_cost,payment_amount,total_amount,currency,items_json"
        rows = [header]
        cities = ["?????", "???", "???", "??", "??????"]
        statuses = ["confirmed", "delivered", "pending", "shipped"]
        for i in range(1, 301):
            c = cities[i % len(cities)]
            s = statuses[i % len(statuses)]
            sku = f"SKU-{(i % 6) + 1}"
            items = f'""[{{"item_name":"{sku}","quantity":1,"unit_price":15000,"subtotal":15000}}]""'
            row = f"ORD-{10000+i},2025-02-{(i%28)+1:02d}T10:00:00Z,CUST-{100+(i%50)},771234{i:03d},user{i}@mail.com,{c},??????,{s},cash,paid,standard,1000,15000,16000,YER,{items}"
            rows.append(row)
        sample.write_text(chr(10).join(rows) + chr(10), encoding="utf-8")
        print("[+] Generated fallback CSV:", sample)
    else:
        print("[+] Found existing CSV datasets:", [f.name for f in csvs])

if __name__ == "__main__":
    main()
