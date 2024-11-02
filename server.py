import time
import json
import pickle
import numpy as np
import paho.mqtt.client as mqtt
from joblib import load

# Load the model
model = load('random_forest_model.joblib')

# MQTT setup
MQTT_BROKER = "localhost"  # Change to your broker address if needed
PORT = 1883
TOPIC_SENSOR_DATA = "dam/sensor-data"
TOPIC_ALERTS = "dam/alerts"

def publish_mqtt(client, topic, message):
    """Publish a message to an MQTT topic."""
    client.publish(topic, message)
    print(f"Published to {topic}: {message}")

def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe(TOPIC_SENSOR_DATA)

def on_message(client, userdata, msg):
    """Callback when a message is received from MQTT"""
    try:
        data = json.loads(msg.payload.decode())
        # Process the sensor data
        process_sensor_data(data['load_value'], data['tilt_status'], data['distance'])
        # Process expert system logic
        response = expert_system(data)
        response_message = json.dumps(response)
        publish_mqtt(client, TOPIC_ALERTS, response_message)
    except json.JSONDecodeError:
        print("Received data is not in JSON format")
    except KeyError as e:
        print(f"Missing key in data: {e}")

def expert_system(data):
    """Process the expert system logic based on dam safety criteria."""
    water_level = data.get("load_value")  # Adjust as needed
    strain = data.get("tilt_status")  # Adjust as needed
    if water_level > 5 and strain > 2:  # Example condition
        return {"alert": "High risk of dam overflow or structural issue"}
    return {"status": "normal"}

def process_sensor_data(load_value, tilt_status, distance):
    """Process sensor data and decide if an alert is needed."""
    sensor_data = np.array([[load_value, tilt_status, distance]])
    prediction = model.predict(sensor_data)
    if prediction == 1:
        alert_message = f"Alert! Abnormal conditions detected. Load: {load_value}, Tilt: {tilt_status}, Distance: {distance}"
        publish_mqtt(mqtt_client, TOPIC_ALERTS, {"alert": alert_message})
    else:
        print("All conditions normal.")

# MQTT client setup
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, PORT, 60)
mqtt_client.loop_start()

try:
    print("Monitoring sensors...")
    while True:
        # Simulated sensor data (replace these with actual sensor data in practice)
        load_value = np.random.randint(0, 1000)  # Placeholder for load sensor reading
        tilt_status = np.random.choice([0, 1])  # Placeholder for tilt sensor reading
        distance = np.random.uniform(0, 400)  # Placeholder for ultrasonic sensor reading

        # Publish sensor data to MQTT
        sensor_data = json.dumps({
            "load_value": load_value,
            "tilt_status": tilt_status,
            "distance": distance
        })
        publish_mqtt(mqtt_client, TOPIC_SENSOR_DATA, sensor_data)

        time.sleep(5)  # Adjust this delay as needed

except KeyboardInterrupt:
    print("Shutting down.")
finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

