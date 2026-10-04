"""
CI Setup & Full Ingestion Script for Clean Ubuntu Runner (GitHub Actions)
"""
import time
from pathlib import Path
from pymongo import MongoClient


def main():
    print("[*] Waiting for MongoDB 7.0 service...")
    for _ in range(25):
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
    sample = data_dir / "01_student_test_small.csv"
    
    # ???? ???? ??? ?????? ???? ??????
    header = "order_id,order_date,customer_id,customer_phone,customer_email,city,district,status,payment_method,payment_status,delivery_type,delivery_cost,payment_amount,total_amount,currency,items_json\n"
    rows = []
    cities = ["?????", "???", "???", "??", "??????"]
    statuses = ["confirmed", "delivered", "pending", "shipped"]
    for i in range(1, 1001):
        c = cities[i % len(cities)]
        s = statuses[i % len(statuses)]
        item_json = f'[{{"item_name":"SKU-{(i%6)+1}","quantity":1,"unit_price":15000,"subtotal":15000}}]'
        rows.append(
            f'ORD-{10000+i},2025-02-{(i%28)+1:02d}T10:00:00Z,CUST-{100+(i%50)},'
            f'771234{i:03d},user{i}@mail.com,{c},??????,{s},cash,paid,standard,'
            f'1000,15000,16000,YER,"{item_json.replace(chr(34), chr(34)*2)}"'
        )
    sample.write_text(header + "\n".join(rows) + "\n", encoding="utf-8")
    print(f"[+] Generated sample dataset at: {sample}")

    # ????? ?? ???????? ??????? (Phase 1 ELT) ???? ????? ???????? ????????? ??? ???????? API
    print("[*] Running Phase 1 ELT pipeline to populate MongoDB...")
    try:
        from src.main import run_master_pipeline
        run_master_pipeline(str(sample))
        print("[+] Phase 1 ELT pipeline executed successfully on CI runner!")
    except Exception as e:
        print(f"[-] Error running pipeline in CI: {e}")
        raise e


if __name__ == "__main__":
    main()
