from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd


# ---------- Config ----------
N_CUSTOMERS = 200
N_ORDERS = 1000
MAX_LINES_PER_ORDER = 5
RETURN_RATE = 0.1  # ~10% of lines will be returned
RANDOM_SEED = 42

OUTPUT_DIR = Path("data")  # reuse your existing data/ folder


# ---------- Static reference data ----------

@dataclass
class City:
    name: str
    country: str
    lat: float
    lon: float


CITIES: List[City] = [
    City("Berlin", "DE", 52.5200, 13.4050),
    City("Munich", "DE", 48.1351, 11.5820),
    City("Vienna", "AT", 48.2082, 16.3738),
    City("Zurich", "CH", 47.3769, 8.5417),
    City("Geneva", "CH", 46.2044, 6.1432),
    City("Amsterdam", "NL", 52.3676, 4.9041),
    City("Rotterdam", "NL", 51.9244, 4.4777),
    City("Paris", "FR", 48.8566, 2.3522),
    City("Lyon", "FR", 45.7640, 4.8357),
    City("Milan", "IT", 45.4642, 9.1900),
    City("Rome", "IT", 41.9028, 12.4964),
]

FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Erik", "Franz",
    "Giulia", "Hans", "Ivana", "Jonas", "Katarina", "Luca",
    "Marta", "Nina", "Oliver", "Paul", "Quinn", "Rosa",
]

LAST_NAMES = [
    "Müller", "Schmidt", "Huber", "Weber", "Fischer", "Klein",
    "Gruber", "Hofer", "Lehmann", "Keller", "Ricci", "Moretti",
]

SEGMENTS = ["Silver", "Gold", "Platinum"]

PRODUCTS = [
    ("KB001", "Keyboard", "Peripherals", 40.0),
    ("MS001", "Mouse", "Peripherals", 25.0),
    ("MN001", "Monitor 24\"", "Displays", 150.0),
    ("MN002", "Monitor 27\"", "Displays", 220.0),
    ("HD001", "External HDD 1TB", "Storage", 80.0),
    ("SD001", "SSD 1TB", "Storage", 130.0),
    ("LT001", "Laptop 14\"", "Computers", 900.0),
    ("LT002", "Laptop 16\"", "Computers", 1300.0),
    ("TB001", "Tablet 11\"", "Computers", 500.0),
    ("PH001", "Phone", "Mobile", 700.0),
    ("RU001", "Router", "Networking", 120.0),
    ("SW001", "Switch 8-port", "Networking", 60.0),
    ("CH001", "Office Chair", "Furniture", 200.0),
    ("DS001", "Desk", "Furniture", 350.0),
    ("PK001", "Backpack", "Accessories", 60.0),
    ("PN001", "Pen Set", "Stationery", 10.0),
    ("NB001", "Notebook", "Stationery", 5.0),
    ("CB001", "HDMI Cable", "Accessories", 15.0),
    ("US001", "USB Hub", "Accessories", 25.0),
    ("PR001", "Printer", "Peripherals", 200.0),
]


def truncated_normal(mean, std, size=None, min_val=None, max_val=None):
    """Gaussian-like values, cut off at min/max if provided."""
    x = np.random.normal(mean, std, size=size)
    if min_val is not None:
        x = np.maximum(x, min_val)
    if max_val is not None:
        x = np.minimum(x, max_val)
    return x


def main():
    np.random.seed(RANDOM_SEED)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ---------- Customers ----------
    customers = []
    for customer_id in range(1, N_CUSTOMERS + 1):
        fn = np.random.choice(FIRST_NAMES)
        ln = np.random.choice(LAST_NAMES)
        city = np.random.choice(CITIES)
        segment = np.random.choice(SEGMENTS, p=[0.6, 0.3, 0.1])  # more Silver than Platinum

        customers.append(
            {
                "customer_id": customer_id,
                "name": f"{fn} {ln}",
                "country": city.country,
                "city": city.name,
                "segment": segment,
                "lat": city.lat + np.random.normal(0, 0.02),  # small jitter per customer
                "lon": city.lon + np.random.normal(0, 0.02),
            }
        )

    customers_df = pd.DataFrame(customers)

    # ---------- Products ----------
    products = []
    for idx, (code, name, category, base_price) in enumerate(PRODUCTS, start=1):
        products.append(
            {
                "product_id": idx,
                "sku": code,
                "product_name": name,
                "category": category,
                "base_price": base_price,
            }
        )

    products_df = pd.DataFrame(products)

    # ---------- Orders & Lines ----------
    orders = []
    order_lines = []

    now = datetime(2024, 12, 31)  # arbitrary benchmark "now"
    order_id = 1
    line_id = 1

    for order_id in range(1, N_ORDERS + 1):
        customer_id = int(np.random.randint(1, N_CUSTOMERS + 1))
        customer = customers_df.loc[customers_df["customer_id"] == customer_id].iloc[0]

        # order date: last 120 days
        days_ago = int(np.random.randint(0, 120))
        order_date = now - timedelta(days=days_ago)

        # number of lines
        n_lines = int(np.random.randint(1, MAX_LINES_PER_ORDER + 1))

        order_line_ids = []

        for line_no in range(1, n_lines + 1):
            product_row = products_df.sample(1).iloc[0]

            # Gaussian-ish quantity & price multiplier
            quantity = int(truncated_normal(mean=3, std=2, size=None, min_val=1))
            price_factor = float(truncated_normal(mean=1.0, std=0.2, size=None, min_val=0.5, max_val=1.5))
            unit_price = float(product_row["base_price"] * price_factor)

            # discount in %, mostly small but sometimes bigger
            discount_pct = float(truncated_normal(mean=0.05, std=0.05, size=None, min_val=0.0, max_val=0.3))

            gross_amount = unit_price * quantity
            discount_amount = gross_amount * discount_pct
            net_amount = gross_amount - discount_amount

            order_lines.append(
                {
                    "line_id": line_id,
                    "order_id": order_id,
                    "line_number": line_no,
                    "customer_id": customer_id,
                    "product_id": int(product_row["product_id"]),
                    "sku": product_row["sku"],
                    "product_name": product_row["product_name"],
                    "category": product_row["category"],
                    "unit_price": round(unit_price, 2),
                    "quantity": quantity,
                    "discount_pct": round(discount_pct, 4),
                    "gross_amount": round(gross_amount, 2),
                    "discount_amount": round(discount_amount, 2),
                    "net_amount": round(net_amount, 2),
                    "order_date": order_date.date().isoformat(),
                }
            )

            order_line_ids.append(line_id)
            line_id += 1

        # order totals = sum line values
        # we'll fill these later via aggregation
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.date().isoformat(),
                "status": np.random.choice(["NEW", "SHIPPED", "DELIVERED", "CANCELLED"], p=[0.1, 0.2, 0.6, 0.1]),
                "currency": "EUR",
                "shipping_country": customer["country"],
                "shipping_city": customer["city"],
                "shipping_lat": customer["lat"],
                "shipping_lon": customer["lon"],
            }
        )

    orders_df = pd.DataFrame(orders)
    order_lines_df = pd.DataFrame(order_lines)

    # Aggregate line values back to orders
    agg = (
        order_lines_df.groupby("order_id")
        .agg(
            total_gross=("gross_amount", "sum"),
            total_discount=("discount_amount", "sum"),
            total_net=("net_amount", "sum"),
            line_count=("line_id", "count"),
        )
        .reset_index()
    )

    orders_df = orders_df.merge(agg, on="order_id", how="left")

    # ---------- Returns / refunds ----------
    # Mark ~10% of lines as returned
    n_lines_total = len(order_lines_df)
    n_returns = int(n_lines_total * RETURN_RATE)
    return_indices = np.random.choice(order_lines_df.index, size=n_returns, replace=False)

    order_lines_df["is_returned"] = False
    order_lines_df["return_quantity"] = 0
    order_lines_df["refund_amount"] = 0.0
    order_lines_df["return_date"] = pd.NaT

    for idx in return_indices:
        row = order_lines_df.loc[idx]
        max_q = int(row["quantity"])
        if max_q <= 0:
            continue

        return_qty = int(np.random.randint(1, max_q + 1))
        # assume full net refund per returned unit
        unit_net = row["net_amount"] / row["quantity"]
        refund = unit_net * return_qty

        order_date = datetime.fromisoformat(str(row["order_date"]))
        # return within 30 days after order
        delta_days = int(np.random.randint(1, 31))
        return_date = order_date + timedelta(days=delta_days)

        order_lines_df.loc[idx, "is_returned"] = True
        order_lines_df.loc[idx, "return_quantity"] = return_qty
        order_lines_df.loc[idx, "refund_amount"] = round(refund, 2)
        order_lines_df.loc[idx, "return_date"] = return_date.date().isoformat()

    # separate returns table (optional)
    returns_df = order_lines_df[order_lines_df["is_returned"]].copy()

    # ---------- Write CSVs ----------
    customers_df.to_csv(OUTPUT_DIR / "customers_extended.csv", index=False)
    products_df.to_csv(OUTPUT_DIR / "products.csv", index=False)
    orders_df.to_csv(OUTPUT_DIR / "orders_header.csv", index=False)
    order_lines_df.to_csv(OUTPUT_DIR / "order_lines.csv", index=False)
    returns_df.to_csv(OUTPUT_DIR / "returns.csv", index=False)

    print("Generated files in", OUTPUT_DIR.resolve())
    print(" customers_extended.csv:", len(customers_df))
    print(" products.csv:", len(products_df))
    print(" orders_header.csv:", len(orders_df))
    print(" order_lines.csv:", len(order_lines_df))
    print(" returns.csv:", len(returns_df))


if __name__ == "__main__":
    main()
