import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import uuid

class OrdersProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.fake = Faker()
        
        # Sample data for realistic orders
        self.restaurants = [
            {"id": "rest_001", "name": "Pizza Palace", "cuisine": "Italian"},
            {"id": "rest_002", "name": "Burger Barn", "cuisine": "American"},
            {"id": "rest_003", "name": "Sushi Spot", "cuisine": "Japanese"},
            {"id": "rest_004", "name": "Taco Town", "cuisine": "Mexican"},
            {"id": "rest_005", "name": "Curry Corner", "cuisine": "Indian"}
        ]
        
        self.customers = [f"cust_{str(i).zfill(3)}" for i in range(1, 101)]
        self.drivers = [f"driver_{str(i).zfill(3)}" for i in range(1, 21)]
        
        self.menu_items = {
            "rest_001": [
                {"id": "item_001", "name": "Margherita Pizza", "price": 12.99},
                {"id": "item_002", "name": "Pepperoni Pizza", "price": 14.99},
                {"id": "item_003", "name": "Caesar Salad", "price": 8.99}
            ],
            "rest_002": [
                {"id": "item_004", "name": "Classic Burger", "price": 9.99},
                {"id": "item_005", "name": "Chicken Sandwich", "price": 11.99},
                {"id": "item_006", "name": "French Fries", "price": 4.99}
            ],
            "rest_003": [
                {"id": "item_007", "name": "California Roll", "price": 8.99},
                {"id": "item_008", "name": "Salmon Sashimi", "price": 15.99},
                {"id": "item_009", "name": "Miso Soup", "price": 3.99}
            ]
        }

    def generate_order(self, late_arrival=False):
        """Generate a realistic order with optional late arrival simulation"""
        restaurant = random.choice(self.restaurants)
        customer_id = random.choice(self.customers)
        driver_id = random.choice(self.drivers)
        
        # Simulate late-arriving data (up to 48 hours late)
        if late_arrival:
            event_time = datetime.now() - timedelta(
                hours=random.randint(1, 48),
                minutes=random.randint(0, 59)
            )
        else:
            event_time = datetime.now() - timedelta(minutes=random.randint(0, 30))
        
        order_id = str(uuid.uuid4())
        
        # Generate order items
        items = []
        if restaurant["id"] in self.menu_items:
            num_items = random.randint(1, 4)
            selected_items = random.sample(self.menu_items[restaurant["id"]], 
                                         min(num_items, len(self.menu_items[restaurant["id"]])))
            
            for item in selected_items:
                quantity = random.randint(1, 3)
                items.append({
                    "item_id": item["id"],
                    "item_name": item["name"],
                    "quantity": quantity,
                    "unit_price": item["price"],
                    "total_price": round(item["price"] * quantity, 2)
                })
        
        order_amount = sum(item["total_price"] for item in items)
        delivery_fee = round(random.uniform(2.99, 5.99), 2)
        tip_amount = round(random.uniform(0, order_amount * 0.2), 2)
        total_amount = round(order_amount + delivery_fee + tip_amount, 2)
        
        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "restaurant_id": restaurant["id"],
            "restaurant_name": restaurant["name"],
            "driver_id": driver_id,
            "event_timestamp": event_time.isoformat(),
            "order_timestamp": event_time.isoformat(),
            "order_status": random.choice(["placed", "confirmed", "preparing", "ready", "picked_up", "delivered"]),
            "items": items,
            "order_amount": order_amount,
            "delivery_fee": delivery_fee,
            "tip_amount": tip_amount,
            "total_amount": total_amount,
            "payment_method": random.choice(["credit_card", "debit_card", "cash", "digital_wallet"]),
            "delivery_address": self.fake.address(),
            "special_instructions": random.choice([None, "Leave at door", "Ring doorbell", "Call when arrived"])
        }
        
        return order

    def produce_orders(self, num_orders=100, delay_seconds=1, late_arrival_probability=0.1):
        """Produce orders to Kafka topic"""
        print(f"Starting to produce {num_orders} orders...")
        
        for i in range(num_orders):
            # Simulate late-arriving data
            late_arrival = random.random() < late_arrival_probability
            order = self.generate_order(late_arrival=late_arrival)
            
            # Send to Kafka
            future = self.producer.send(
                'orders-topic',
                key=order['order_id'],
                value=order
            )
            
            # Log the order
            status = "LATE" if late_arrival else "REAL-TIME"
            print(f"[{status}] Sent order {i+1}/{num_orders}: {order['order_id']} - ${order['total_amount']}")
            
            time.sleep(delay_seconds)
        
        # Ensure all messages are sent
        self.producer.flush()
        print("All orders sent successfully!")

    def close(self):
        self.producer.close()

if __name__ == "__main__":
    producer = OrdersProducer()
    try:
        # Produce orders continuously
        while True:
            producer.produce_orders(num_orders=10, delay_seconds=2, late_arrival_probability=0.15)
            print("Batch completed. Waiting 30 seconds before next batch...")
            time.sleep(30)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close() 