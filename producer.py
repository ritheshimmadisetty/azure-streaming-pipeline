import asyncio
import json
import random
import os
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker('en_IN')  # Indian locale for realistic data

# Connection details
CONNECTION_STRING = os.getenv('EVENT_HUBS_CONNECTION_STRING')
ORDERS_HUB = "orders-stream"
CLICKS_HUB = "clickstream"

# Sample data
PRODUCTS = [
    {"id": 1, "name": "Laptop", "category": "Electronics", "price": 45000},
    {"id": 2, "name": "Phone", "category": "Electronics", "price": 15000},
    {"id": 3, "name": "T-Shirt", "category": "Clothing", "price": 599},
    {"id": 4, "name": "Jeans", "category": "Clothing", "price": 1499},
    {"id": 5, "name": "Rice 5kg", "category": "Grocery", "price": 350},
    {"id": 6, "name": "Coffee", "category": "Grocery", "price": 450},
    {"id": 7, "name": "Chair", "category": "Furniture", "price": 4500},
    {"id": 8, "name": "Desk", "category": "Furniture", "price": 8000},
]

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai", "Pune"]
STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled"]

def generate_order_event():
    """Generate a realistic order event"""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "event_type": "order",
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "customer_id": random.randint(1, 500),
        "customer_name": fake.name(),
        "city": random.choice(CITIES),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "unit_price": product["price"],
        "quantity": quantity,
        "total_amount": product["price"] * quantity,
        "status": random.choice(STATUSES),
        "timestamp": datetime.utcnow().isoformat(),
        "platform": random.choice(["mobile", "web", "app"])
    }

def generate_click_event():
    """Generate a realistic clickstream event"""
    return {
        "event_type": "click",
        "session_id": f"SESS-{random.randint(10000, 99999)}",
        "customer_id": random.randint(1, 500),
        "page": random.choice(["home", "product", "cart", "checkout", "search"]),
        "product_id": random.randint(1, 8),
        "action": random.choice(["view", "add_to_cart", "remove", "purchase", "search"]),
        "city": random.choice(CITIES),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "timestamp": datetime.utcnow().isoformat()
    }

async def send_events():
    """Send events to Azure Event Hubs continuously"""

    orders_producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=ORDERS_HUB
    )
    clicks_producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=CLICKS_HUB
    )

    print("🚀 Starting event producer...")
    print("Sending events to Azure Event Hubs every 2 seconds")
    print("Press Ctrl+C to stop\n")

    event_count = 0

    async with orders_producer, clicks_producer:
        while True:
            # Send 1 order event
            order_batch = await orders_producer.create_batch()
            order = generate_order_event()
            order_batch.add(EventData(json.dumps(order)))
            await orders_producer.send_batch(order_batch)

            # Send 2-3 click events per order (realistic ratio)
            click_batch = await clicks_producer.create_batch()
            for _ in range(random.randint(2, 3)):
                click = generate_click_event()
                click_batch.add(EventData(json.dumps(click)))
            await clicks_producer.send_batch(click_batch)

            event_count += 1
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Event #{event_count} sent | "
                  f"Order: {order['order_id']} | "
                  f"Product: {order['product_name']} | "
                  f"Amount: ₹{order['total_amount']:,}")

            # Wait 2 seconds before next event
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(send_events())