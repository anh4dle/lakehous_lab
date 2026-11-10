import time
import random
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sku_inventory (
            sku_id VARCHAR PRIMARY KEY,
            product_name VARCHAR NOT NULL,
            warehouse_code VARCHAR NOT NULL,
            available_qty INT NOT NULL,
            reorder_threshold INT NOT NULL,
            updated_at TIMESTAMP NOT NULL);
        """)
    cur.execute("""
        CREATE TABLE live_stock_status (
            sku_id VARCHAR(20) PRIMARY KEY,
            product_name VARCHAR(100),
            warehouse_code VARCHAR(10),
            available_qty INT,
            stock_badge VARCHAR(20),
            last_updated TIMESTAMP);
        """)
    
PRODUCTS = [
    ("SKU-101", "Wireless Mechanical Keyboard", "HN-01", 25, 10),
    ("SKU-102", "USB-C Docking Station", "HN-01", 18, 8),
    ("SKU-103", "27-inch 4K Monitor", "HCM-02", 12, 5),
    ("SKU-104", "Ergonomic Gaming Mouse", "HCM-02", 40, 15),
    ("SKU-105", "Noise-Cancelling Headphones", "HN-01", 15, 5),
]

def seed_data(cur):
    for sku_id, name, wh, qty, threshold in PRODUCTS:
        cur.execute("""
            INSERT INTO sku_inventory (sku_id, product_name, warehouse_code, available_qty, reorder_threshold, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (sku_id) DO NOTHING;
        """, (sku_id, name, wh, qty, threshold, datetime.utcnow()))

def run_stream():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    seed_data(cur)
    print("Seeded initial SKUs. Starting live stock updates...")

    while True:
        sku_id = random.choice(PRODUCTS)[0]
        cur.execute("SELECT available_qty FROM sku_inventory WHERE sku_id = %s;", (sku_id,))
        current_qty = cur.fetchone()[0]

        if current_qty <= 2:
            # Simulate warehouse restock (+20 units)
            new_qty = current_qty + 20
            action = "RESTOCK"
        else:
            # Simulate customer purchase (-1 to -3 units)
            sold = random.randint(1, min(3, current_qty))
            new_qty = current_qty - sold
            action = f"SALE (-{sold})"

        cur.execute("""
            UPDATE sku_inventory
            SET available_qty = %s, updated_at = %s
            WHERE sku_id = %s;
        """, (new_qty, datetime.utcnow(), sku_id))

        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {sku_id} | {action} | New Qty: {new_qty}")
        time.sleep(3)

if __name__ == "__main__":
    create_tables()
    run_stream()
