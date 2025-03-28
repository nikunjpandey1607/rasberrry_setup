#!/usr/bin/env python3
import time
import base64
import os
import uuid
from datetime import datetime
import RPi.GPIO as GPIO
from picamera import PiCamera
from pymongo import MongoClient
import io

# MongoDB connection
MONGODB_URI = os.environ.get("MONGODB_URI")
DB_NAME = os.environ.get("DB_NAME", "sensor_data")
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

# Collections
sensor_readings_collection = db["sensor_readings"]
image_data_collection = db["image_data"]

# Sensor pins
TRIG_PIN = 23
ECHO_PIN = 24

# Initialize GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setup(TRIG_PIN, GPIO.OUT)
GPIO.setup(ECHO_PIN, GPIO.IN)

# Initialize camera
camera = PiCamera()
camera.resolution = (1024, 768)

# Current mission ID
current_mission_id = str(uuid.uuid4())

def read_water_level():
    """Read water level using ultrasonic sensor."""
    try:
        GPIO.output(TRIG_PIN, True)
        time.sleep(0.00001)
        GPIO.output(TRIG_PIN, False)
        
        pulse_start = time.time()
        timeout = pulse_start + 1
        while GPIO.input(ECHO_PIN) == 0:
            if time.time() > timeout:
                return None
            pulse_start = time.time()
        
        pulse_end = time.time()
        timeout = pulse_end + 1
        while GPIO.input(ECHO_PIN) == 1:
            if time.time() > timeout:
                return None
            pulse_end = time.time()
        
        pulse_duration = pulse_end - pulse_start
        distance = pulse_duration * 17150
        max_height = 100
        return max(0, min(100, max_height - distance))
    except Exception as e:
        print(f"Ultrasonic sensor error: {e}")
        return None

def capture_image():
    """Capture an image from the camera and return base64 encoded data."""
    try:
        stream = io.BytesIO()
        camera.capture(stream, format='jpeg')
        stream.seek(0)
        image_data = base64.b64encode(stream.read()).decode('utf-8')
        return image_data
    except Exception as e:
        print(f"Camera error: {e}")
        return None

def collect_and_send_data():
    """Collect data from all sensors and send to MongoDB."""
    try:
        timestamp = datetime.utcnow()
        
        # Read water level
        water_level = read_water_level()
        
        # Capture image
        image_data = capture_image()
        
        # Create sensor reading document
        sensor_reading = {
            "_id": str(uuid.uuid4()),
            "missionId": current_mission_id,
            "timestamp": timestamp,
            "waterLevel": water_level if water_level is not None else 0,
            "createdAt": timestamp
        }
        
        # Create image data document
        image_doc = {
            "_id": str(uuid.uuid4()),
            "missionId": current_mission_id,
            "timestamp": timestamp,
            "imageData": image_data,
            "createdAt": timestamp
        }
        
        # Send data to MongoDB
        if water_level is not None:
            sensor_readings_collection.insert_one(sensor_reading)
            print("Sensor reading sent to MongoDB")
        
        if image_data is not None:
            image_data_collection.insert_one(image_doc)
            print("Image data sent to MongoDB")
        
    except Exception as e:
        print(f"Error collecting or sending data: {e}")

def main():
    """Main function to run the data collection loop."""
    print("Starting sensor data collection...")

    try:
        while True:
            print("Collecting data...")
            collect_and_send_data()
            print(f"Waiting for next collection cycle (2 minutes)...")
            time.sleep(120)  # Wait for 2 minutes
    except KeyboardInterrupt:
        print("Data collection stopped by user")
    finally:
        # Clean up
        GPIO.cleanup()
        camera.close()
        print("Resources cleaned up and mission completed")

if __name__ == "__main__":
    main()
