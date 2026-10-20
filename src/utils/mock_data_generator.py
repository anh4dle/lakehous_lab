import json
import uuid
import random
from datetime import datetime, timedelta, date
from src.client.minio_client import MinioClient
from pathlib import Path
from src.core.logger import get_logger
from src.core.config import BASE_SOURCE_NAME
import io
logger = get_logger(__name__)


COUNTRIES = ["VN", "US", "SG", "JP", "KR"]

# -------------------------
# TIME HELPERS
# -------------------------
def random_time_in_day(base_date: date):
    """Generate a random timestamp inside a specific day"""
    start = datetime.combine(base_date, datetime.min.time())
    return start + timedelta(seconds=random.randint(0, 86400 - 1))

def make_partition_path(prefix, base_date: date):
    dt = base_date.strftime("%Y-%m-%d")
    path = Path(BASE_SOURCE_NAME) / prefix / f"dt={dt}"
    return path

# -------------------------
# CUSTOMER GENERATION
# -------------------------
def generate_customers_data(minio_client, base_date, n):
    """Generate customer data in-memory and upload directly to MinIO"""
    customers = []
    id_list = []
    
    for _ in range(n):
        customer_id = str(uuid.uuid4())
        id_list.append(customer_id)
        event_time = random_time_in_day(base_date)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": event_time.isoformat(),
            "customer": {
                "customer_id": customer_id,
                "name": f"User_{customer_id}",
                "email": f"{customer_id.lower()}@mail.com",
                "country": random.choice(COUNTRIES),
                "created_at": event_time.isoformat(),
            },
        }
        customers.append(event)

    # 1. Serialize the list to a JSON string and encode as UTF-8 bytes
    json_data = json.dumps(customers, indent=2)
    json_bytes = json_data.encode('utf-8')
    
    # 2. Define your object key path in MinIO
    object_name = f"raw/customers/{base_date}/customers_{int(datetime.utcnow().timestamp())}.json"

    # 3. Stream directly using BytesIO (no local file write required)
    minio_client.upload_bytes(
        bucket_name="warehouse",
        object_name=object_name,
        data=json_bytes,
        length=len(json_bytes),
        content_type="application/json"
    )
    
    print(f"Successfully streamed {len(customers)} customer records directly to 'warehouse/{object_name}'.")
    return len(customers), id_list

def generate_orders_data(minio_client, base_date, n, active_customer_ids):
    """Generate order data in-memory and upload directly to MinIO"""
    orders = []

    for _ in range(n):
        order_id = str(uuid.uuid4())
        customer_id = random.choice(active_customer_ids) 
        event_time = random_time_in_day(base_date)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": event_time.isoformat(),
            "order": {
                "order_id": order_id,
                "customer_id": customer_id,
                "amount": round(random.uniform(10, 500), 2),
                "status": random.choice(["CREATED", "PAID", "SHIPPED"]),
                "items": random.randint(1, 5),
                "created_at": event_time.isoformat(),
            },
        }

        orders.append(event)

    # 1. Serialize the list to a JSON string and encode as UTF-8 bytes
    json_data = json.dumps(orders, indent=2)
    json_bytes = json_data.encode('utf-8')
    
    # 2. Define the partitioned object path in MinIO
    object_name = f"raw/orders/{base_date}/orders_{int(datetime.utcnow().timestamp())}.json"

    # 3. Stream directly using BytesIO (no local file write required)
    minio_client.upload_bytes(
        bucket_name="warehouse",
        object_name=object_name,
        data=json_bytes,
        length=len(json_bytes),
        content_type="application/json"
    )

    print(f"Successfully streamed {len(orders)} order records directly to 'warehouse/{object_name}'.")
    return len(orders)


# -------------------------
# MAIN MULTI-DAY SIMULATION
# -------------------------
def generate_data(start_date):
    minio_client = MinioClient()
    # simulate daily volume
    customers = random.randint(50, 200)
    orders = random.randint(500, 2000)

    c, id_list = generate_customers_data(minio_client, start_date, customers)
    o = generate_orders_data(minio_client, start_date, orders, id_list)

    logger.info("[%s] customers=%s, orders=%s", start_date, c, o)

    logger.info("DONE")

import argparse
# -------------------------
# ENTRYPOINT
# -------------------------
if __name__ == "__main__":
    # 1. Set up the argument parser
    parser = argparse.ArgumentParser(description="Run daily customer and order simulation.")
    
    # 2. Add arguments with defaults
    parser.add_argument(
        "--start_date", 
        type=str, 
        default= datetime.now().strftime("%Y-%m-%d"), 
        help="The start date for the simulation in YYYY-MM-DD format"
    )

    # 3. Parse the arguments
    args = parser.parse_args()

    # 4. Convert the string date argument into a datetime.date object
    try:
        parsed_start_date = datetime.strptime(args.current_day, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid date format for --current_day: '{args.current_day}'. Use YYYY-MM-DD.")

    # 5. Hand off variables to the execution block
    generate_data(start_date=parsed_start_date)
