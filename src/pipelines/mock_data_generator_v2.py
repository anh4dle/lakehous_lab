import json
import uuid
import argparse
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
    """
    Generate customer data in-memory with:
    - High cardinality field: `device_id` (unique hardware hash per record)
    - Schema evolution: `tier` added after May 10, 2026
    - Duplicate rate: 2.0% duplicates injected
    """
    customers = []
    id_list = []

    for _ in range(n):
        customer_id = str(uuid.uuid4())
        id_list.append(customer_id)
        event_time = random_time_in_day(base_date)

        # High Cardinality Field: Unique device hash per customer record
        device_id = f"dev_{uuid.uuid4().hex}"

        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": event_time.isoformat(),
            "customer": {
                "customer_id": customer_id,
                "name": f"User_{customer_id}",
                "email": f"{customer_id.lower()}@mail.com",
                "country": random.choice(COUNTRIES),
                "device_id": device_id,  # High cardinality attribute
                "created_at": event_time.isoformat(),
            },
        }

        # Schema Evolution: Add 'tier' after May 10, 2026
        if base_date >= date(2026, 5, 10):
            event["customer"]["tier"] = random.choice(["FREE", "PREMIUM", "VIP"])
        else:
            event["customer"]["tier"] = None

        customers.append(event)

    # Ingest Duplicates (2% duplicate rate)
    raw_count = len(customers)
    n_dupes = int(raw_count * 0.02)
    if n_dupes > 0:
        duplicates = [random.choice(customers) for _ in range(n_dupes)]
        customers.extend(duplicates)
        random.shuffle(customers)

    final_count = len(customers)

    json_data = json.dumps(customers, indent=2)
    json_bytes = json_data.encode("utf-8")
    bytes_io = io.BytesIO(json_bytes)
    
    object_name = (
        f"raw/customers/dt={base_date}/customers_{int(datetime.utcnow().timestamp())}.json"
    )

    minio_client.upload_bytes(
        bucket_name="warehouse",
        object_name=object_name,
        data=bytes_io,
        length=len(json_bytes),
        content_type="application/json",
    )

    print(
        f"Successfully streamed {final_count} customer records (incl. {n_dupes} dupes, high-cardinality device_id) to 'warehouse/{object_name}'."
    )
    return final_count, id_list


# -------------------------
# ORDERS GENERATION
# -------------------------
def generate_orders_data(minio_client, base_date, n, active_customer_ids):
    """
    Generate order data in-memory with:
    - High cardinality field: `session_id` (unique tracking session hash per order)
    - Schema evolution: `promo_code` added after May 10, 2026
    - Duplicate rate: 2.0% duplicates injected
    """
    orders = []

    for _ in range(n):
        order_id = str(uuid.uuid4())
        customer_id = random.choice(active_customer_ids) if active_customer_ids else str(uuid.uuid4())
        event_time = random_time_in_day(base_date)

        # High Cardinality Field: Unique session tracking ID per order
        session_id = f"sess_{uuid.uuid4().hex}"

        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": event_time.isoformat(),
            "order": {
                "order_id": order_id,
                "customer_id": customer_id,
                "amount": round(random.uniform(10, 500), 2),
                "status": random.choice(["CREATED", "PAID", "SHIPPED"]),
                "items": random.randint(1, 5),
                "session_id": session_id,  # High cardinality attribute
                "created_at": event_time.isoformat(),
            },
        }

        # Schema Evolution: Add 'promo_code' field after May 10, 2026
        if base_date >= date(2026, 5, 10):
            event["order"]["promo_code"] = random.choice([None, "SUMMER_SALE", "VIP_DEAL"])
        else:
            event["order"]["promo_code"] = None

        orders.append(event)

    # Ingest Duplicates (2% duplicate rate)
    raw_count = len(orders)
    n_dupes = int(raw_count * 0.02)
    if n_dupes > 0:
        duplicates = [random.choice(orders) for _ in range(n_dupes)]
        orders.extend(duplicates)
        random.shuffle(orders)

    final_count = len(orders)

    print(f"[QUALITY CHECK] Date: {base_date} | Total Orders: {final_count} | Dupes: {n_dupes} | High Cardinality session_id Active | Schema Evolution: {base_date >= date(2026, 5, 10)}")

    json_data = json.dumps(orders, indent=2)
    json_bytes = json_data.encode("utf-8")
    bytes_io = io.BytesIO(json_bytes)
    
    object_name = (
        f"raw/orders/dt={base_date}/orders_{int(datetime.utcnow().timestamp())}.json"
    )

    minio_client.upload_bytes(
        bucket_name="warehouse",
        object_name=object_name,
        data=bytes_io,
        length=len(json_bytes),
        content_type="application/json",
    )

    print(
        f"Successfully streamed {final_count} order records (incl. high-cardinality session_id) directly to 'warehouse/{object_name}'."
    )
    return final_count


# -------------------------
# MAIN MULTI-DAY SIMULATION
# -------------------------
def generate_data(start_date):
    minio_client = MinioClient()
    customers = random.randint(50, 200)
    orders = random.randint(500, 2000)

    c, id_list = generate_customers_data(minio_client, start_date, customers)
    o = generate_orders_data(minio_client, start_date, orders, id_list)

    logger.info("[%s] customers=%s, orders=%s", start_date, c, o)
    logger.info("DONE")


# -------------------------
# ENTRYPOINT
# -------------------------
def main(argv=None):
    minio_client = MinioClient()
    etl_date = minio_client.get_max_raw_date() or "2026-05-01"

    parser = argparse.ArgumentParser(
        description="Run daily customer and order simulation."
    )

    parser.add_argument(
        "--start_date",
        type=str,
        default=etl_date,
        help="The start date for the simulation in YYYY-MM-DD format",
    )

    args = parser.parse_args(argv)

    try:
        parsed_start_date = datetime.strptime(str(args.start_date), "%Y-%m-%d").date() + timedelta(days=1)
    except ValueError:
        raise ValueError(
            f"Invalid date format for --start_date: '{args.start_date}'. Use YYYY-MM-DD."
        )

    generate_data(start_date=parsed_start_date)


if __name__ == "__main__":
    main()