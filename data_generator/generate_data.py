import json
import uuid
import random
from datetime import datetime, timedelta, date
from pathlib import Path

OUTPUT = Path("data_generator/output")
STATE_FILE = Path("state.json")

COUNTRIES = ["VN", "US", "SG", "JP", "KR"]


# -------------------------
# STATE MANAGEMENT
# -------------------------
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"customer_seq": 1000, "order_seq": 10000, "last_date": "2026-05-01"}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# -------------------------
# TIME HELPERS
# -------------------------
def random_time_in_day(base_date: date):
    """Generate a random timestamp inside a specific day"""
    start = datetime.combine(base_date, datetime.min.time())
    return start + timedelta(seconds=random.randint(0, 86400 - 1))


def make_partition_path(prefix, base_date: date):
    dt = base_date.strftime("%Y-%m-%d")
    path = OUTPUT / prefix / f"dt={dt}"
    path.mkdir(parents=True, exist_ok=True)
    return path


# -------------------------
# CUSTOMER GENERATION
# -------------------------
def generate_customers_for_day(state, base_date, n):
    customers = []
    path = make_partition_path("customers", base_date)

    for _ in range(n):
        state["customer_seq"] += 1
        customer_id = f"C{state['customer_seq']}"

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

    file_path = path / f"customers_{int(datetime.utcnow().timestamp())}.json"
    file_path.write_text(json.dumps(customers, indent=2))

    return len(customers)


# -------------------------
# ORDER GENERATION
# -------------------------
def generate_orders_for_day(state, base_date, n):
    orders = []
    path = make_partition_path("orders", base_date)

    for _ in range(n):
        state["order_seq"] += 1
        order_id = f"O{state['order_seq']}"
        customer_id = f"C{random.randint(1001, state['customer_seq'])}"
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

    file_path = path / f"orders_{int(datetime.utcnow().timestamp())}.json"
    file_path.write_text(json.dumps(orders, indent=2))

    return len(orders)


# -------------------------
# MAIN MULTI-DAY SIMULATION
# -------------------------
def run_simulation(num_days=7):
    state = load_state()
    start_date_str = state["last_date"]
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

    total_c = 0
    total_o = 0
    current_day = start_date
    for i in range(num_days):
        current_day = start_date + timedelta(days=i)
        if current_day >= datetime.now().date() - timedelta(days=1):
            break

        # simulate daily volume
        customers = random.randint(50, 200)
        orders = random.randint(500, 2000)

        c = generate_customers_for_day(state, current_day, customers)
        o = generate_orders_for_day(state, current_day, orders)

        print(f"[{current_day}] customers={c}, orders={o}")

        total_c += c
        total_o += o
    state["last_date"] = current_day.strftime("%Y-%m-%d")
    print("Showing state", state)
    save_state(state)

    print("\nDONE")
    print(f"Total customers: {total_c}")
    print(f"Total orders: {total_o}")


# -------------------------
# ENTRYPOINT
# -------------------------
if __name__ == "__main__":
    run_simulation(num_days=7)
