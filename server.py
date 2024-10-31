import json
import paho.mqtt.client as mqtt
import ssl

# MQTT settings
BROKER = "localhost"  
PORT = 1883  # Standard MQTT over SSL port
TOPIC_SUB = "dam/sensor-data"
TOPIC_PUB = "dam/alerts"

# Expert system processing logic
def expert_system(data):
    water_level = data.get("water_level")
    strain = data.get("strain")
    # Dam safety condition example: modify as per project requirements
    if water_level > 5 and strain > 2:
        return {"alert": "High risk of dam overflow or structural issue"}
    return {"status": "normal"}

# Callback function for when a message is received from MQTT
def on_message(client, userdata, message):
    try:
        data = json.loads(message.payload.decode())
        response = expert_system(data)
        response_message = json.dumps(response)
        # Publish response back to the specified topic
        client.publish(TOPIC_PUB, response_message)
        print(f"Processed data: {data}, Response: {response}")
    except json.JSONDecodeError:
        print("Received data is not in JSON format")

# Initialize MQTT client
client = mqtt.Client()

# Set up TLS/SSL settings
#client.tls_set(ca_certs="/root/Dam_Safety_Server/ca.pem",    # Certificate authority, if required
 #              certfile="/root/Dam_Safety_Server/cert.pem",  # Your SSL certificate
  #             keyfile="/root/Dam_Safety_Server/key.pem")    # Your SSL private key

# Define the connection and message handling callbacks
client.on_connect = lambda client, userdata, flags, rc: client.subscribe(TOPIC_SUB)
client.on_message = on_message

# Connect to the MQTT broker with SSL/TLS
client.connect(BROKER, PORT,keepalive=60)
print(f"Connected securely to MQTT broker at {BROKER}:{PORT}")
client.loop_forever()
