#!/usr/bin/env python3
import time
import base64
import json
import os
from datetime import datetime
import uuid
import board
import adafruit_dht
import RPi.GPIO as GPIO
from picamera import PiCamera
import serial
import pynmea2
from pymongo import MongoClient
import io

# MongoDB connection
MONGODB_URI = os.environ.get("MONGODB_URI")
DB_NAME = os.environ.get("DB_NAME", "sensor_data")
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

# Collections
missions_collection = db["missions"]
sensor_readings_collection = db["sensor_readings"]
image_data_collection = db["image_data"]
system_status_collection = db["system_status"]

# Sensor pins
DHT_PIN = board.D4  # GPIO4
TRIG_PIN = 23  # GPIO23
ECHO_PIN = 24  # GPIO24

# GPS serial port
GPS_PORT = "/dev/ttyAMA0"
GPS_BAUD = 9600

# Initialize sensors
dht_sensor = adafruit_dht.DHT22(DHT_PIN)

# Initialize GPIO for ultrasonic sensor
GPIO.setmode(GPIO.BCM)
GPIO.setup(TRIG_PIN, GPIO.OUT)
GPIO.setup(ECHO_PIN, GPIO.IN)

# Initialize camera
camera = PiCamera()
camera.resolution = (1024, 768)

# Initialize GPS
gps_serial = serial.Serial(GPS_PORT, baudrate=GPS_BAUD, timeout=1)

# Current mission ID (you should set this when starting a mission)
current_mission_id = str(uuid.uuid4())

def get_gps_coordinates():
    """Get GPS coordinates from the GPS module."""
    try:
        # Read data from GPS
        line = gps_serial.readline().decode('utf-8')
        
        # Check if it's a GPGGA sentence (contains location data)
        if line.startswith('$GPGGA'):
            msg = pynmea2.parse(line)
            latitude = msg.latitude
            longitude = msg.longitude
            altitude = msg.altitude
            
            return {
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude
            }
    except Exception as e:
        print(f"GPS error: {e}")
    
    # Return default values if GPS fails
    return {
        "latitude": 0.0,
        "longitude": 0.0,
        "altitude": 0.0
    }

def read_dht22():
    """Read temperature and humidity from DHT22 sensor."""
    try:
        temperature = dht_sensor.temperature
        humidity = dht_sensor.humidity
        return temperature, humidity
    except Exception as e:
        print(f"DHT22 error: {e}")
        return None, None

def read_water_level():
    """Read water level using RCWL1655 ultrasonic sensor."""
    try:
        # Send 10us pulse to trigger
        GPIO.output(TRIG_PIN, True)
        time.sleep(0.00001)
        GPIO.output(TRIG_PIN, False)
        
        # Wait for echo to start
        pulse_start = time.time()
        timeout = pulse_start + 1  # 1 second timeout
        
        while GPIO.input(ECHO_PIN) == 0:
            if time.time() > timeout:
                return None
            pulse_start = time.time()
            
        # Wait for echo to end
        pulse_end = time.time()
        timeout = pulse_end + 1  # 1 second timeout
        
        while GPIO.input(ECHO_PIN) == 1:
            if time.time() > timeout:
                return None
            pulse_end = time.time()
            
        # Calculate distance
        pulse_duration = pulse_end - pulse_start
        distance = pulse_duration * 17150  # Speed of sound = 343 m/s = 34300 cm/s
                                          # Distance = (time * speed) / 2 (round trip)
        
        # Convert to water level (adjust based on your setup)
        # Assuming sensor is mounted at the top of a container with max height of 100cm
        max_height = 100  # cm
        water_level = max_height - distance
        
        return max(0, min(100, water_level))  # Ensure value is between 0-100
    except Exception as e:
        print(f"Ultrasonic sensor error: {e}")
        return None

def capture_image():
    """Capture an image from the camera and return base64 encoded data."""
    try:
        # Create in-memory stream
        stream = io.BytesIO()
        
        # Capture image to stream
        camera.capture(stream, format='jpeg')
        
        # Reset stream position
        stream.seek(0)
        
        # Read image data and encode as base64
        image_data = base64.b64encode(stream.read()).decode('utf-8')
        
        return image_data
    except Exception as e:
        print(f"Camera error: {e}")
        return None

def check_system_status(humidity_reading, water_level_reading):
    """Check system status and return status object."""
    # This is a simplified status check
    battery_voltage = 11.1  # Example value, replace with actual reading if available
    signal_strength = 80    # Example value, replace with actual reading if available
    
    return {
        "_id": str(uuid.uuid4()),
        "missionId": current_mission_id,
        "timestamp": datetime.utcnow(),
        "batteryVoltage": battery_voltage,
        "signalStrength": signal_strength,
        "gpsStatus": True,  # Simplified, should check actual GPS status
        "cameraStatus": True,  # Simplified, should check actual camera status
        "sensorStatus": {
            "temperature": humidity_reading is not None,
            "humidity": humidity_reading is not None,
            "airQuality": False,  # Not implemented
            "flame": False,  # Not implemented
            "waterLevel": water_level_reading is not None,
            "ultrasonic": water_level_reading is not None
        },
        "createdAt": datetime.utcnow()
    }

def collect_and_send_data():
    """Collect data from all sensors and send to MongoDB."""
    try:
        # Get timestamp
        timestamp = datetime.utcnow()
        
        # Get GPS coordinates
        location = get_gps_coordinates()
        
        # Read DHT22 sensor
        temperature, humidity = read_dht22()
        
        # Read water level
        water_level = read_water_level()
        
        # Capture image
        image_data = capture_image()
        
        # Create sensor reading document
        sensor_reading = {
            "_id": str(uuid.uuid4()),
            "missionId": current_mission_id,
            "timestamp": timestamp,
            "location": location,
            "humidity": humidity if humidity is not None else 0,
            "temperature": temperature if temperature is not None else 0,
            "waterLevel": water_level if water_level is not None else 0,
            "signalStrength": 80,  # Example value
            "createdAt": timestamp
        }
        
        # Create image data document
        image_doc = {
            "_id": str(uuid.uuid4()),
            "missionId": current_mission_id,
            "timestamp": timestamp,
            "location": location,
            "imageData": image_data,
            "createdAt": timestamp
        }
        
        # Check system status
        system_status = check_system_status(humidity, water_level)
        
        # Send data to MongoDB
        if humidity is not None or water_level is not None:
            sensor_readings_collection.insert_one(sensor_reading)
            print("Sensor reading sent to MongoDB")
        
        if image_data is not None:
            image_data_collection.insert_one(image_doc)
            print("Image data sent to MongoDB")
        
        system_status_collection.insert_one(system_status)
        print("System status sent to MongoDB")
        
    except Exception as e:
        print(f"Error collecting or sending data: {e}")

def main():
    """Main function to run the data collection loop."""
    print("Starting sensor data collection...")
    
    # Create a mission if one doesn't exist
    mission = {
        "_id": current_mission_id,
        "name": "Automated Sensor Mission",
        "startTime": datetime.utcnow(),
        "status": "active",
        "description": "Automated data collection from Raspberry Pi sensors",
        "location": get_gps_coordinates(),
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow()
    }
    
    try:
        missions_collection.insert_one(mission)
        print(f"Created new mission with ID: {current_mission_id}")
    except Exception as e:
        print(f"Error creating mission: {e}")
    
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
        gps_serial.close()
        
        # Update mission status
        missions_collection.update_one(
            {"_id": current_mission_id},
            {
                "$set": {
                    "endTime": datetime.utcnow(),
                    "status": "completed",
                    "updatedAt": datetime.utcnow()
                }
            }
        )
        print("Resources cleaned up and mission completed")

if __name__ == "__main__":
    main()
