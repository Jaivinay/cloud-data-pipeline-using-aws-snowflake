# src/utils/generate_raw_data.py
#
# Generates synthetic raw e-commerce data (orders, customers, products) with realistic
# messiness - duplicates, nulls, inconsistent date formats - deliberately, since the
# whole point of the Glue ETL step is to clean this up. A pristine synthetic dataset
# would make the "data quality validation" part of the pipeline pointless to write.

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
random.seed(42)

FIRST_NAMES = ["Alex", "Sam", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Drew", "Cameron"]
LAST_NAMES = ["Smith", "Johnson", "Lee", "Garcia", "Chen", "Patel", "Kim", "Brown", "Davis", "Martinez"]
STATES = ["FL", "CA", "TX", "NY", "WA", "IL", "GA", "OH", "NC", "AZ"]
CATEGORIES = ["Electronics", "Home & Kitchen", "Grocery", "Apparel", "Sports", "Books"]
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]  # inconsistent on purpose - real source systems do this


def gen_customers(n=200):
    rows = []
    for i in range(n):
        rows.append({
            "customer_id": f"C{1000+i}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": None if random.random() < 0.03 else f"user{i}@example.com",  # a few missing emails
            "state": random.choice(STATES),
            "signup_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 900))).strftime(
                random.choice(DATE_FORMATS)
            ),
        })
    # inject a handful of exact-duplicate rows - source systems do this via double-submits
    rows.extend(random.sample(rows, 5))
    return rows


def gen_products(n=80):
    rows = []
    for i in range(n):
        category = random.choice(CATEGORIES)
        rows.append({
            "product_id": f"P{500+i}",
            "product_name": f"{category} Item {i}",
            "category": category,
            "unit_price": round(random.uniform(5, 250), 2),
        })
    return rows


def gen_orders(customers, products, n=3000):
    rows = []
    for i in range(n):
        cust = random.choice(customers)
        prod = random.choice(products)
        qty = random.randint(1, 5)
        order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 500))
        rows.append({
            "order_id": f"O{100000+i}",
            "customer_id": cust["customer_id"],
            "product_id": prod["product_id"],
            "quantity": qty if random.random() > 0.01 else None,  # rare null quantity
            "order_date": order_date.strftime(random.choice(DATE_FORMATS)),
            "unit_price_at_order": prod["unit_price"],
        })
    # a chunk of duplicate order rows - common with retry logic on the source side
    rows.extend(random.sample(rows, 30))
    return rows


def write_csv(rows, path: Path):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    customers = gen_customers()
    products = gen_products()
    orders = gen_orders(customers, products)

    write_csv(customers, RAW_DIR / "customers.csv")
    write_csv(products, RAW_DIR / "products.csv")
    write_csv(orders, RAW_DIR / "orders.csv")

    print(f"customers: {len(customers)} rows -> {RAW_DIR/'customers.csv'}")
    print(f"products:  {len(products)} rows -> {RAW_DIR/'products.csv'}")
    print(f"orders:    {len(orders)} rows -> {RAW_DIR/'orders.csv'}")


if __name__ == "__main__":
    main()
