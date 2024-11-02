import time
import pickle
import numpy as np
import RPi.GPIO as GPIO
from hx711 import HX711
import serial
from joblib import load

# Load the model
model = load('random_forest_model.joblib')


# GPIO Setup
GPIO.setmode(GPIO.BCM)

# Load Sensor HX711 setup
# Initialize HX711 Load Sensor
hx711 = HX711(dout_pin=5, pd_sck_pin=6)  # Replace pins with the ones connected
hx711.set_reading_format("MSB", "MSB")

# Initial tare (zero out the scale) after a delay
time.sleep(0.4)  # Wait 400 ms before the first reading
hx711.reset()
hx711.tare()
 # Tare the scale at startup

# Tilt Sensor setup (SW420D)
TILT_PIN = 17
GPIO.setup(TILT_PIN, GPIO.IN)

# Ultrasonic Sensor setup (HC-SR04)
TRIG_PIN = 23
ECHO_PIN = 24
GPIO.setup(TRIG_PIN, GPIO.OUT)
GPIO.setup(ECHO_PIN, GPIO.IN)

# Siren setup (SR-A77)
SIREN_PIN = 18
GPIO.setup(SIREN_PIN, GPIO.OUT)
GPIO.output(SIREN_PIN, GPIO.LOW)

# GSM Module setup (SIM800L)
gsm_serial = serial.Serial("/dev/serial0", baudrate=9600, timeout=1)

def send_sms_notification(message, phone_number="+1234567890"):
    """Sends an SMS using the GSM module"""
    try:
        gsm_serial.write(b'AT\r')
        time.sleep(1)
        gsm_serial.write(b'AT+CMGF=1\r')  # Set SMS text mode
        time.sleep(1)
        gsm_serial.write(f'AT+CMGS="{phone_number}"\r'.encode())
        time.sleep(1)
        gsm_serial.write(f"{message}\x1A".encode())  # Send message with Ctrl+Z to end
        print("SMS sent.")
    except Exception as e:
        print(f"Failed to send SMS: {e}")

def sound_siren():
    """Activates the siren alarm"""
    GPIO.output(SIREN_PIN, GPIO.HIGH)
    time.sleep(1)  # Sound siren for 1 second
    GPIO.output(SIREN_PIN, GPIO.LOW)

def read_ultrasonic_distance():
    """Returns the distance measured by the ultrasonic sensor"""
    GPIO.output(TRIG_PIN, True)
    time.sleep(0.00001)
    GPIO.output(TRIG_PIN, False)

    start_time = time.time()
    stop_time = time.time()

    while GPIO.input(ECHO_PIN) == 0:
        start_time = time.time()
    while GPIO.input(ECHO_PIN) == 1:
        stop_time = time.time()

    elapsed = stop_time - start_time
    distance = (elapsed * 34300) / 2  # Speed of sound is 34300 cm/s
    return distance

def read_tilt_sensor():
    """Reads the tilt sensor status"""
    return GPIO.input(TILT_PIN)

def twos_complement(value, bits=24):
    """Converts HX711 reading to 2's complement for signed data interpretation."""
    if value & (1 << (bits - 1)):
        value -= 1 << bits
    return value

def read_load_sensor():
    """Reads and converts load sensor value from HX711 with 2's complement logic."""
    raw_value = hx711.read_long()
    if raw_value is not None:
        converted_value = twos_complement(raw_value)
        return converted_value
    else:
        print("Load sensor read failed")
        return 0


def monitor_sensors():
    """Monitors sensors and triggers actions based on model prediction"""
    print("Monitoring sensors...")

    while True:
        load_value = read_load_sensor()
        tilt_status = read_tilt_sensor()
        distance = read_ultrasonic_distance()

        # Format data for model input
        sensor_data = np.array([[load_value, tilt_status, distance]])

        # Predict using the model
        prediction = model.predict(sensor_data)
        if prediction == 1:  # Assuming '1' indicates an alert condition
            print("Abnormal condition detected! Activating alarm.")
            sound_siren()
            send_sms_notification("Alert! Abnormal conditions detected by dam sensors.")
        else:
            print("All conditions normal.")

        time.sleep(2)  # Wait before next reading

try:
    monitor_sensors()
except KeyboardInterrupt:
    print("Shutting down.")
finally:
    GPIO.cleanup()
    gsm_serial.close()
