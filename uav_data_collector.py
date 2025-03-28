#!/usr/bin/env python3

import os
import time
import json
import datetime
import random
import uuid
import base64
import io
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
import logging
import threading
import cv2
import numpy as np

# Set up logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
  handlers=[
      logging.FileHandler("/home/pi/uav_data_collector/logs/data_collector.log"),
      logging.StreamHandler()
  ]
)
logger = logging.getLogger("UAV_Data_Collector")

# Load environment variables
load_dotenv()

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI")
MISSION_ID = os.getenv("MISSION_ID")
DEVICE_ID = os.getenv("DEVICE_ID")

# Connect to MongoDB
try:
  client = MongoClient(MONGODB_URI)
  db = client.get_database()
  logger.info("Connected to MongoDB successfully")
except Exception as e:
  logger.error(f"Failed to connect to MongoDB: {e}")
  exit(1)

# Buffer directory for offline operation
BUFFER_DIR = "/home/pi/uav_data_collector/buffer"
os.makedirs(os.path.join(BUFFER_DIR, "sensor_readings"), exist_ok=True)
os.makedirs(os.path.join(BUFFER_DIR, "images"), exist_ok=True)
os.makedirs(os.path.join(BUFFER_DIR, "system_status"), exist_ok=True)

# Lock for thread safety
buffer_lock = threading.Lock()

# Mission start time for battery calculation
MISSION_START_TIME = datetime.datetime.now()

# Function to buffer data
def buffer_data(data, data_type):
  with buffer_lock:
      try:
          timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
          
          if data_type == "sensor":
              buffer_file = os.path.join(BUFFER_DIR, "sensor_readings", f"sensor_{timestamp}.json")
          elif data_type == "image":
              buffer_file = os.path.join(BUFFER_DIR, "images", f"image_{timestamp}.json")
          elif data_type == "status":
              buffer_file = os.path.join(BUFFER_DIR, "system_status", f"status_{timestamp}.json")
          else:
              logger.error(f"Unknown data type: {data_type}")
              return False
          
          with open(buffer_file, 'w') as f:
              json.dump(data, f, default=str)
          
          logger.info(f"Data buffered to {buffer_file}")
          return True
      except Exception as e:
          logger.error(f"Error buffering data: {e}")
          return False

# Function to process buffered data
def process_buffer():
  try:
      for data_type in ["sensor", "status", "image"]:
          buffer_dir = os.path.join(BUFFER_DIR, 
                                   "sensor_readings" if data_type == "sensor" else 
                                   "images" if data_type == "image" else 
                                   "system_status")
          
          files = [os.path.join(buffer_dir, f) for f in os.listdir(buffer_dir) 
                   if os.path.isfile(os.path.join(buffer_dir, f)) and f.endswith('.json')]
          
          if not files:
              continue
              
          logger.info(f"Processing {len(files)} buffered {data_type} files")
          
          for file_path in files:
              try:
                  with open(file_path, 'r') as f:
                      data = json.load(f)
                  
                  # Convert string timestamps back to datetime objects
                  if 'timestamp' in data and isinstance(data['timestamp'], str):
                      data['timestamp'] = datetime.datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                  if 'createdAt' in data and isinstance(data['createdAt'], str):
                      data['createdAt'] = datetime.datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00'))
                  
                  if data_type == "sensor":
                      db.sensorReadings.insert_one(data)
                  elif data_type == "image":
                      db.images.insert_one(data)
                  elif data_type == "status":
                      db.systemStatus.insert_one(data)
                  
                  # Remove the file after successful processing
                  os.remove(file_path)
                  logger.info(f"Processed and removed {file_path}")
              except Exception as e:
                  logger.error(f"Error processing buffered file {file_path}: {e}")
                  # Don't remove the file if processing failed
  except Exception as e:
      logger.error(f"Error in process_buffer: {e}")

# Function to get system status
def get_system_status():
  try:
      # Get CPU temperature
      with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
          cpu_temp = float(f.read()) / 1000.0
      
      # Get memory usage
      with open('/proc/meminfo', 'r') as f:
          mem_info = f.readlines()
      
      mem_total = int(mem_info[0].split()[1])
      mem_free = int(mem_info[1].split()[1])
      mem_usage = (mem_total - mem_free) / mem_total * 100
      
      # Get disk usage
      disk = os.statvfs('/')
      disk_total = disk.f_frsize * disk.f_blocks
      disk_free = disk.f_frsize * disk.f_bfree
      disk_usage = (disk_total - disk_free) / disk_total * 100
      
      # Get hostname and IP
      import socket
      hostname = socket.gethostname()
      try:
          ip_address = socket.gethostbyname(hostname)
      except:
          ip_address = "127.0.0.1"
      
      return {
          "cpu_temperature": cpu_temp,
          "memory_usage": mem_usage,
          "disk_usage": disk_usage,
          "hostname": hostname,
          "ip_address": ip_address
      }
  except Exception as e:
      logger.error(f"Error getting system status: {e}")
      return {}

# Function to read sensor data
def read_sensor_data():
  try:
      # In a real application, you would read from actual sensors
      # For example, to read from a DHT22 temperature/humidity sensor:
      # import Adafruit_DHT
      # humidity, temperature = Adafruit_DHT.read_retry(Adafruit_DHT.DHT22, 4)
      
      # For a BMP280 pressure/temperature sensor:
      # import board
      # import adafruit_bmp280
      # i2c = board.I2C()
      # bmp280 = adafruit_bmp280.Adafruit_BMP280_I2C(i2c)
      # temperature = bmp280.temperature
      # pressure = bmp280.pressure
      
      # For RCWL-1655 water level sensor:
      # import RPi.GPIO as GPIO
      # GPIO.setmode(GPIO.BCM)
      # TRIG = 23  # GPIO pin for trigger
      # ECHO = 24  # GPIO pin for echo
      # GPIO.setup(TRIG, GPIO.OUT)
      # GPIO.setup(ECHO, GPIO.IN)
      # GPIO.output(TRIG, False)
      # time.sleep(0.2)
      # GPIO.output(TRIG, True)
      # time.sleep(0.00001)
      # GPIO.output(TRIG, False)
      # while GPIO.input(ECHO) == 0:
      #     pulse_start = time.time()
      # while GPIO.input(ECHO) == 1:
      #     pulse_end = time.time()
      # pulse_duration = pulse_end - pulse_start
      # distance = pulse_duration * 17150  # Speed of sound in cm/s
      # water_level = 150 - distance  # Assuming sensor is 150cm from bottom
      
      # For this example, we'll generate random data
      temperature = 25.0 + random.uniform(-5.0, 5.0)
      humidity = 60.0 + random.uniform(-10.0, 10.0)
      air_quality = 40.0 + random.uniform(-10.0, 20.0)
      flame_detected = False  # Default to clear (not detected)
      water_level = 120.0 + random.uniform(-20.0, 20.0)  # Water level in cm
      
      # Calculate battery level based on 20-minute operation constraint
      mission_duration = (datetime.datetime.now() - MISSION_START_TIME).total_seconds() / 60  # in minutes
      battery_level = max(0, 100 - (mission_duration / 20) * 100)
      signal_strength = 90.0 + random.uniform(-15.0, 5.0)
      
      # Simulated GPS location (adjust to your area)
      latitude = 28.6139 + random.uniform(-0.01, 0.01)
      longitude = 77.2090 + random.uniform(-0.01, 0.01)
      altitude = 220.0 + random.uniform(-10.0, 10.0)
      
      return {
          "temperature": round(temperature, 1),
          "humidity": round(humidity, 1),
          "airQuality": round(air_quality, 1),
          "flameDetected": flame_detected,
          "waterLevel": round(water_level, 1),
          "batteryLevel": round(battery_level, 1),
          "signalStrength": round(signal_strength, 1),
          "location": {
              "latitude": round(latitude, 6),
              "longitude": round(longitude, 6),
              "altitude": round(altitude, 1)
          }
      }
  except Exception as e:
      logger.error(f"Error reading sensor data: {e}")
      return {}

# Function to capture image using OpenCV instead of PiCamera
def capture_and_detect():
  try:
      # Try to use a camera if available
      camera_available = False
      try:
          cap = cv2.VideoCapture(0)  # Try to open the default camera
          if cap.isOpened():
              camera_available = True
              ret, frame = cap.read()
              if ret:
                  # Resize the image to reduce size
                  frame = cv2.resize(frame, (640, 480))
                  # Convert to JPEG format
                  _, buffer = cv2.imencode('.jpg', frame)
                  image_data = base64.b64encode(buffer).decode('utf-8')
                  cap.release()
              else:
                  camera_available = False
          else:
              camera_available = False
      except Exception as e:
          logger.warning(f"Could not access camera: {e}")
          camera_available = False
      
      # If camera is not available, create a test image
      if not camera_available:
          logger.info("Camera not available, creating test image")
          # Create a blank image
          img = np.zeros((480, 640, 3), dtype=np.uint8)
          
          # Draw some shapes to simulate objects
          cv2.rectangle(img, (100, 100), (300, 300), (0, 255, 0), -1)  # Green rectangle
          cv2.circle(img, (500, 200), 100, (0, 0, 255), -1)  # Red circle
          cv2.putText(img, "UAV Test Image", (150, 400), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
          
          # Encode the image to base64
          _, buffer = cv2.imencode('.jpg', img)
          image_data = base64.b64encode(buffer).decode('utf-8')
      
      # In a real application, you would perform object detection here
      # For example, using a pre-trained model with OpenCV DNN or TensorFlow
      
      # Simulated detections
      detections = []
      detection_types = ["Person", "Vehicle", "Building", "Tree", "Road", "Water", "Fire"]
      
      # Add 1-3 random detections
      for _ in range(random.randint(1, 3)):
          detection_type = random.choice(detection_types)
          confidence = round(random.uniform(0.7, 0.98), 2)
          
          # Create a bounding box
          x = random.uniform(0.1, 0.7)
          y = random.uniform(0.1, 0.7)
          width = random.uniform(0.1, 0.3)
          height = random.uniform(0.1, 0.3)
          
          detections.append({
              "type": detection_type,
              "confidence": confidence,
              "boundingBox": {
                  "x": x,
                  "y": y,
                  "width": width,
                  "height": height
              }
          })
      
      # Simulated GPS location (adjust to your area)
      latitude = 28.6139 + random.uniform(-0.01, 0.01)
      longitude = 77.2090 + random.uniform(-0.01, 0.01)
      altitude = 220.0 + random.uniform(-10.0, 10.0)
      
      return {
          "imageData": f"data:image/jpeg;base64,{image_data}",
          "detections": detections,
          "location": {
              "latitude": round(latitude, 6),
              "longitude": round(longitude, 6),
              "altitude": round(altitude, 1)
          }
      }
  except Exception as e:
      logger.error(f"Error capturing and detecting image: {e}")
      return None

# Function to collect and send data
def collect_and_send_data():
  try:
      # Get current timestamp
      timestamp = datetime.datetime.now()
      
      # Read sensor data
      sensor_data = read_sensor_data()
      
      # Get system status
      system_status = get_system_status()
      
      # Prepare sensor reading document
      sensor_reading = {
          "missionId": MISSION_ID,
          "deviceId": DEVICE_ID,
          "timestamp": timestamp,
          "location": sensor_data.get("location", {}),
          "temperature": sensor_data.get("temperature"),  # Optional field
          "humidity": sensor_data.get("humidity"),  # Optional field
          "airQuality": sensor_data.get("airQuality"),  # Optional field
          "flameDetected": sensor_data.get("flameDetected", False),  # Optional, default to False
          "waterLevel": sensor_data.get("waterLevel"),  # New field for RCWL-1655 sensor
          "batteryLevel": sensor_data.get("batteryLevel", 0),
          "signalStrength": sensor_data.get("signalStrength", 0),
          "createdAt": timestamp
      }
      
      # Prepare system status document
      status_doc = {
          "missionId": MISSION_ID,
          "deviceId": DEVICE_ID,
          "timestamp": timestamp,
          "batteryLevel": sensor_data.get("batteryLevel", 0),
          "batteryVoltage": 11.7 * (sensor_data.get("batteryLevel", 0) / 100),  # Scale voltage based on battery level
          "batteryTemperature": system_status.get("cpu_temperature", 0),
          "motorStatus": {
              "motor1": True,
              "motor2": True,
              "motor3": True,
              "motor4": True
          },
          "signalStrength": sensor_data.get("signalStrength", 0),
          "gpsStatus": True,
          "cameraStatus": True,
          "sensorStatus": {
              "temperature": True,
              "humidity": True,
              "airQuality": True,
              "flame": True,
              "waterLevel": True,  # Added water level sensor status
              "ultrasonic": True
          },
          "systemInfo": system_status,
          "createdAt": timestamp
      }
      
      # Capture image and perform object detection (every 5 minutes)
      current_minute = timestamp.minute
      if current_minute % 5 == 0:
          image_data = capture_and_detect()
          if image_data:
              # Prepare image document
              image_doc = {
                  "missionId": MISSION_ID,
                  "deviceId": DEVICE_ID,
                  "timestamp": timestamp,
                  "location": image_data.get("location", {}),
                  "imageData": image_data.get("imageData", ""),
                  "detections": image_data.get("detections", []),
                  "createdAt": timestamp
              }
              
              try:
                  # Insert image data into MongoDB
                  db.images.insert_one(image_doc)
                  logger.info(f"Image data sent to MongoDB successfully at {timestamp}")
              except Exception as e:
                  logger.warning(f"Failed to send image data to MongoDB: {e}")
                  # Buffer the image data for later transmission
                  buffer_data(image_doc, "image")
                  logger.info("Image data buffered for later transmission")
      
      try:
          # Insert data into MongoDB
          db.sensorReadings.insert_one(sensor_reading)
          db.systemStatus.insert_one(status_doc)
          
          logger.info(f"Data sent to MongoDB successfully at {timestamp}")
          print(f"Data sent to MongoDB successfully at {timestamp}")
          print(f"Temperature: {sensor_data.get('temperature')}°C, Humidity: {sensor_data.get('humidity')}%")
          
          # Process any buffered data
          process_buffer()
          
      except Exception as e:
          logger.warning(f"Failed to send data to MongoDB: {e}")
          print(f"Failed to send data to MongoDB: {e}")
          
          # Buffer the data for later transmission
          buffer_data(sensor_reading, "sensor")
          buffer_data(status_doc, "status")
          
          logger.info("Data buffered for later transmission")
          print("Data buffered for later transmission")
          
  except Exception as e:
      logger.error(f"Error collecting and sending data: {e}")
      print(f"Error: {e}")

# Main function
if __name__ == "__main__":
  logger.info("UAV Data Collector started")
  
  while True:
      try:
          collect_and_send_data()
          # Wait for 2 minutes before the next collection
          time.sleep(120)
      except KeyboardInterrupt:
          logger.info("UAV Data Collector stopped by user")
          break
      except Exception as e:
          logger.error(f"Unexpected error: {e}")
          # Wait a bit before retrying
          time.sleep(30)

