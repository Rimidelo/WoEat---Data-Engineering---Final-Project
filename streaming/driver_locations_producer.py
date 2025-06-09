import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import uuid

class DriverLocationsProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Driver data
        self.drivers = [
            {"id": f"driver_{str(i).zfill(3)}", "name": f"Driver {i}", "status": "available"}
            for i in range(1, 21)
        ]
        
        # City boundaries (approximate coordinates for a city)
        self.city_bounds = {
            "min_lat": 32.0500,  # Tel Aviv area
            "max_lat": 32.1200,
            "min_lon": 34.7400,
            "max_lon": 34.8200
        }
        
        # Initialize driver positions
        self.driver_positions = {}
        for driver in self.drivers:
            self.driver_positions[driver["id"]] = {
                "lat": random.uniform(self.city_bounds["min_lat"], self.city_bounds["max_lat"]),
                "lon": random.uniform(self.city_bounds["min_lon"], self.city_bounds["max_lon"]),
                "status": random.choice(["available", "busy", "offline"])
            }

    def generate_location_update(self, driver_id, late_arrival=False):
        """Generate a realistic location update for a driver"""
        current_pos = self.driver_positions[driver_id]
        
        # Simulate movement (small random changes)
        lat_change = random.uniform(-0.001, 0.001)  # ~100m movement
        lon_change = random.uniform(-0.001, 0.001)
        
        new_lat = max(self.city_bounds["min_lat"], 
                     min(self.city_bounds["max_lat"], current_pos["lat"] + lat_change))
        new_lon = max(self.city_bounds["min_lon"], 
                     min(self.city_bounds["max_lon"], current_pos["lon"] + lon_change))
        
        # Update position
        self.driver_positions[driver_id]["lat"] = new_lat
        self.driver_positions[driver_id]["lon"] = new_lon
        
        # Occasionally change status
        if random.random() < 0.1:  # 10% chance to change status
            current_pos["status"] = random.choice(["available", "busy", "offline"])
        
        # Simulate late-arriving data (up to 48 hours late)
        if late_arrival:
            event_time = datetime.now() - timedelta(
                hours=random.randint(1, 48),
                minutes=random.randint(0, 59)
            )
        else:
            event_time = datetime.now() - timedelta(seconds=random.randint(0, 30))
        
        location_update = {
            "location_id": str(uuid.uuid4()),
            "driver_id": driver_id,
            "timestamp": event_time.isoformat(),
            "latitude": round(new_lat, 6),
            "longitude": round(new_lon, 6),
            "status": current_pos["status"],
            "speed": round(random.uniform(0, 60), 1),  # km/h
            "heading": random.randint(0, 359),  # degrees
            "accuracy": round(random.uniform(3, 10), 1)  # meters
        }
        
        return location_update

    def produce_locations(self, duration_minutes=60, update_interval_seconds=5, late_arrival_probability=0.05):
        """Produce driver location updates to Kafka topic"""
        print(f"Starting to produce driver locations for {duration_minutes} minutes...")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        update_count = 0
        
        while datetime.now() < end_time:
            # Send updates for all drivers
            for driver in self.drivers:
                # Skip offline drivers occasionally
                if self.driver_positions[driver["id"]]["status"] == "offline" and random.random() < 0.7:
                    continue
                
                # Simulate late-arriving data
                late_arrival = random.random() < late_arrival_probability
                location_update = self.generate_location_update(driver["id"], late_arrival=late_arrival)
                
                # Send to Kafka
                future = self.producer.send(
                    'driver-locations-topic',
                    key=location_update['driver_id'],
                    value=location_update
                )
                
                update_count += 1
                
                # Log occasionally to avoid spam
                if update_count % 50 == 0:
                    status = "LATE" if late_arrival else "REAL-TIME"
                    print(f"[{status}] Sent {update_count} location updates...")
            
            time.sleep(update_interval_seconds)
        
        # Ensure all messages are sent
        self.producer.flush()
        print(f"Location updates completed! Total updates sent: {update_count}")

    def close(self):
        self.producer.close()

if __name__ == "__main__":
    producer = DriverLocationsProducer()
    try:
        # Produce location updates continuously
        while True:
            producer.produce_locations(duration_minutes=10, update_interval_seconds=3, late_arrival_probability=0.08)
            print("Location batch completed. Waiting 60 seconds before next batch...")
            time.sleep(60)
    except KeyboardInterrupt:
        print("Stopping location producer...")
    finally:
        producer.close() 