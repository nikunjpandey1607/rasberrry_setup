#!/usr/bin/env python3
import time
import base64
import json
import os
from datetime import datetime
import uuid
import RPi.GPIO as GPIO
import cv2
import serial
import pynmea2
from pymongo import MongoClient
import io
import threading
import pigpio

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
DHT_PIN = 4  # GPIO4
TRIG_PIN = 23  # GPIO23
ECHO_PIN = 24  # GPIO24

# GPS serial port
GPS_PORT = "/dev/ttyAMA0"
GPS_BAUD = 9600

# Initialize GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setup(TRIG_PIN, GPIO.OUT)
GPIO.setup(ECHO_PIN, GPIO.IN)

# Initialize pigpio for DHT22
pi = pigpio.pi()
if not pi.connected:
    print("Failed to connect to pigpio daemon")
    exit(1)

# Initialize camera with OpenCV
camera = cv2.VideoCapture(0)  # Use 0 for default camera (usually USB webcam)
if not camera.isOpened():
    print("Failed to open camera")
    exit(1)

# Set camera resolution
camera.set(cv2.CAP_PROP_FRAME_WIDTH, 1024)
camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 768)

# Initialize GPS
gps_serial = serial.Serial(GPS_PORT, baudrate=GPS_BAUD, timeout=1)

# Current mission ID
current_mission_id = str(uuid.uuid4())

# Global variables for DHT22 readings
dht22_temperature = None
dht22_humidity = None
dht22_last_read = 0
dht22_read_interval = 2  # seconds

class DHT22Sensor:
    """
    A class to read DHT22 temperature/humidity sensor using pigpio
    """
    def __init__(self, pi, gpio):
        self.pi = pi
        self.gpio = gpio
        self.high_tick = 0
        self.bit = 40
        self.temperature = 0
        self.humidity = 0
        self.either_edge_cb = None
        self.state = 0
        self.bits = 0
        self.code = 0
        self.last_code = 0

    def _cb(self, gpio, level, tick):
        """
        Accumulate the 40 data bits from the DHT22 sensor.
        """
        diff = pigpio.tickDiff(self.high_tick, tick)

        if level == 0:
            # Edge length determines if bit is 1 or 0
            if diff >= 50:
                val = 1
                if diff >= 200:  # Bad bit?
                    self.bits = 0
                    self.code = 0
                    return
            else:
                val = 0

            if self.state >= 2 and self.state <= 41:
                self.code <<= 1
                self.code |= val
                self.bits += 1

            self.state += 1

        elif level == 1:
            self.high_tick = tick
            if self.state == 1:
                self.bits = 0
                self.code = 0

    def read(self):
        """
        Trigger a new relative humidity and temperature reading.
        """
        self.state = 0
        self.bits = 0
        self.code = 0
        self.high_tick = 0

        # Set GPIO as output
        self.pi.set_mode(self.gpio, pigpio.OUTPUT)

        # Send trigger pulse
        self.pi.write(self.gpio, 0)
        time.sleep(0.017)  # 17ms
        self.pi.set_mode(self.gpio, pigpio.INPUT)
        
        # Start callback to collect data bits
        self.either_edge_cb = self.pi.callback(self.gpio, pigpio.EITHER_EDGE, self._cb)
        
        # Wait for reading to complete
        time.sleep(0.2)
        
        # Cancel callback
        if self.either_edge_cb:
            self.either_edge_cb.cancel()
            self.either_edge_cb = None

        # Process the data
        if self.bits >= 40:  # Got enough bits?
            self.last_code = self.code
            
            # Extract data
            humidity_high = (self.code >> 32) & 0xFF
            humidity_low = (self.code >> 24) & 0xFF
            temperature_high = (self.code >> 16) & 0xFF
            temperature_low = (self.code >> 8) & 0xFF
            checksum = self.code & 0xFF
            
            # Verify checksum
            calc_checksum = ((humidity_high + humidity_low + 
                             temperature_high + temperature_low) & 0xFF)
            
            if calc_checksum == checksum:
                # Calculate humidity and temperature
                self.humidity = ((humidity_high * 256) + humidity_low) / 10.0
                
                # Handle negative temperatures
                if temperature_high & 0x80:
                    temperature_high = temperature_high & 0x7F
                    self.temperature = -((temperature_high * 256) + temperature_low) / 10.0
                else:
                    self.temperature = ((temperature_high * 256) + temperature_low) / 10.0
                
                return True
        
        return False

# Create DHT22 sensor instance
dht22_sensor = DHT22Sensor(pi, DHT_PIN)

def read_dht22_sensor():
    """Read temperature and humidity from DHT22 sensor using pigpio."""
    global dht22_temperature, dht22_humidity, dht22_last_read
    
    # Check if we need to read (don't read too frequently)
    current_time = time.time()
    if current_time - dht22_last_read < dht22_read_interval:
        return
    
    dht22_last_read = current_time
    
    try:
        if dht22_sensor.read():
            dht22_temperature = dht22_sensor.temperature
            dht22_humidity = dht22_sensor.humidity
            print(f"DHT22 read: Temperature={dht22_temperature}°C, Humidity={dht22_humidity}%")
        else:
            print("Failed to read from DHT22 sensor")
    except Exception as e:
        print(f"DHT22 reading error: {e}")

def dht22_reader_thread():
    """Thread function to continuously read DHT22 sensor."""
    while True:
        try:
            read_dht22_sensor()
        except Exception as e:
            print(f"DHT22 reading error: {e}")
        time.sleep(2)  # Wait before next reading attempt

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
    """Read temperature and humidity from global variables."""
    return dht22_temperature, dht22_humidity

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
        # Capture frame from camera
        ret, frame = camera.read()
        
        if not ret:
            print("Failed to capture image")
            return None
        
        # Encode image as JPEG
        ret, buffer = cv2.imencode('.jpg', frame)
        
        if not ret:
            print("Failed to encode image")
            return None
        
        # Convert to base64
        image_data = base64.b64encode(buffer).decode('utf-8')
        
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
        "cameraStatus": camera.isOpened(),  # Check if camera is open
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
    
    # Start DHT22 reader thread
    dht_thread = threading.Thread(target=dht22_reader_thread, daemon=True)
    dht_thread.start()
    
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
        camera.release()
        gps_serial.close()
        pi.stop()  # Stop pigpio
        
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
