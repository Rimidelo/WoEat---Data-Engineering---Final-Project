import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import uuid

class OrdersProducer:
    def __init__(self, bootstrap_servers='kafka:29092'):
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
        """Generate a realistic order with separate order and order items"""
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
        
        # Calculate delivery and prep times
        prep_start_time = event_time + timedelta(minutes=random.randint(2, 8))
        prep_duration = random.randint(15, 45)
        prep_end_time = prep_start_time + timedelta(minutes=prep_duration)
        
        delivery_duration = random.randint(20, 60)
        delivery_time = event_time + timedelta(minutes=delivery_duration)
        
        # Generate order items
        order_items = []
        total_amount = 0
        
        if restaurant["id"] in self.menu_items:
            num_items = random.randint(1, 4)
            selected_items = random.sample(self.menu_items[restaurant["id"]], 
                                         min(num_items, len(self.menu_items[restaurant["id"]])))
            
            for idx, item in enumerate(selected_items):
                quantity = random.randint(1, 3)
                item_price = item["price"]
                
                order_item = {
                    "order_item_id": f"{order_id}_item_{idx+1}",
                    "order_id": order_id,
                    "item_id": item["id"],
                    "quantity": str(quantity),
                    "item_price": str(item_price),
                    "order_time": event_time.isoformat()
                }
                order_items.append(order_item)
                total_amount += quantity * item_price
        
        # Add delivery fee and tip
        delivery_fee = round(random.uniform(2.99, 5.99), 2)
        tip_amount = round(random.uniform(0, total_amount * 0.2), 2)
        total_amount = round(total_amount + delivery_fee, 2)
        
        # Determine order status
        status = random.choice(["placed", "confirmed", "preparing", "ready", "picked_up", "delivered"])
        
        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "restaurant_id": restaurant["id"],
            "driver_id": driver_id,
            "order_timestamp": event_time.isoformat(),
            "order_status": status,
            "delivery_timestamp": delivery_time.isoformat() if status == "delivered" else None,
            "total_amount": str(total_amount),
            "prep_start_timestamp": prep_start_time.isoformat(),
            "prep_end_timestamp": prep_end_time.isoformat(),
            "tip_amount": str(tip_amount)
        }
        
        return order, order_items

    def produce_orders(self, num_orders=100, delay_seconds=1, late_arrival_probability=0.1):
        """Produce orders and order items to Kafka topics"""
        print(f"Starting to produce {num_orders} orders...")
        
        for i in range(num_orders):
            # Simulate late-arriving data
            late_arrival = random.random() < late_arrival_probability
            order, order_items = self.generate_order(late_arrival=late_arrival)
            
            # Send order to Kafka
            future = self.producer.send(
                'orders-topic',
                key=order['order_id'],
                value=order
            )
            
            # Send order items to Kafka
            for order_item in order_items:
                item_future = self.producer.send(
                    'order-items-topic',
                    key=order_item['order_item_id'],
                    value=order_item
                )
            
            # Log the order
            status = "LATE" if late_arrival else "REAL-TIME"
            print(f"[{status}] Sent order {i+1}/{num_orders}: {order['order_id']} with {len(order_items)} items - ${order['total_amount']}")
            
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