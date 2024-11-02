import time
import pickle
import numpy as np
import paho.mqtt.client as mqtt
from joblib import load

# Load the model
model = load('random_forest_model.joblib')

# MQTT setup
MQTT_BROKER = "localhost"  # Change to your broker address if needed
TOPIC_SENSOR_DATA = "sensor/data"
TOPIC_ALERT_ACTIONS = "sensor/alerts"

def publish_mqtt(client, topic, message):
    """Publish a message to an MQTT topic."""
    client.publish(topic, message)
    print(f"Published to {topic}: {message}")

def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe(TOPIC_ALERT_ACTIONS)

def on_message(client, userdata, msg):
    """Callback when a message is received from MQTT"""
    try:
        # Decode the received message and check for actions
        action = msg.payload.decode()
        if action == "sound_siren":
            print("Triggering siren via MQTT")
        elif action.startswith("send_sms"):
            message = action.split(":", 1)[1]  # Extract message after 'send_sms:'
            print(f"Sending SMS notification via MQTT: {message}")
        else:
            print("Received unrecognized action")
    except Exception as e:
        print(f"Failed to process MQTT message: {e}")

def process_sensor_data(load_value, tilt_status, distance):
    """Process sensor data and decide if an alert is needed."""
    sensor_data = np.array([[load_value, tilt_status, distance]])
    prediction = model.predict(sensor_data)
    if prediction == 1:
        alert_message = f"Alert! Abnormal conditions detected. Load: {load_value}, Tilt: {tilt_status}, Distance: {distance}"
        publish_mqtt(mqtt_client, TOPIC_ALERT_ACTIONS, "sound_siren")
        publish_mqtt(mqtt_client, TOPIC_ALERT_ACTIONS, f"send_sms:{alert_message}")
    else:
        print("All conditions normal.")

# MQTT client setup
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, 1883, 60)
mqtt_client.loop_start()

try:
    print("Monitoring sensors...")

    while True:
        # Simulated sensor data (replace these with actual sensor data in practice)
        load_value = np.random.randint(0, 1000)  # Placeholder for load sensor reading
        tilt_status = np.random.choice([0, 1])  # Placeholder for tilt sensor reading
        distance = np.random.uniform(0, 400)  # Placeholder for ultrasonic sensor reading

        # Publish sensor data to MQTT
        sensor_data_message = f"{load_value},{tilt_status},{distance}"
        publish_mqtt(mqtt_client, TOPIC_SENSOR_DATA, sensor_data_message)

        # Process sensor data locally to determine if an alert is necessary
        process_sensor_data(load_value, tilt_status, distance)

        time.sleep(5)  # Adjust this delay as needed

except KeyboardInterrupt:
    print("Shutting down.")
finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
