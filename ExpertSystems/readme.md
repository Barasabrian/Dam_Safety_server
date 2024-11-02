# Dam Condition Prediction Model

The model in this repository is created by feeding simulated sensor data present in the repository. The data contains simulated values of load, tilt status, and ultrasonic distance (representing water level).

## How the Model Learns

### Data-Driven Learning
The model utilizes the training data generated, which consists of combinations of load values, tilt statuses, and distances along with their corresponding labels. It learns to associate specific feature values with each label, distinguishing between critical conditions and normal conditions.

### Feature Importance
This model is based on the Random Forest algorithm, which creates multiple decision trees from random subsets of the training data. Each tree makes predictions based on the rules it has learned. Through this process, the model identifies patterns such as:
- A load value of 400 with a tilt status of 0 is considered safe.
- A load value of 600 with a tilt status of 0 indicates a critical condition.
- A load value of 300 with a tilt status of 1 is critical, regardless of the distance.

    The data for training is generated using `datasimulation.py`, which incorporates critical conditions based on the following logic:
    ```python
    if (load_value > 600 and distance < 200) or (tilt_status == 1 and load_value > 300) or (distance < 50 and load_value > 400):

### Complex Interactions
The model does not rely solely on fixed thresholds for decision-making. Instead, it learns from interactions between features. For instance, it might find that a low load combined with a tilt condition is more critical than a higher load with no tilt, depending on the training data it analyzes.

## Using the Model

Once trained, this model can be used to predict whether the dam is in a critical condition based on new sensor readings.

### Testing the Model
To test the model, modify the values of load, tilt, and distance in the `testthemodel.py` file. 

- A prediction of **1** indicates that the dam is in a **critical condition**.
- A prediction of **0** indicates that the dam is **not in a critical condition**.

## Getting Started


1. Clone this repository.
2. Create and activate the Python environment:
   - **Windows**:
     ```bash
     python -m venv venv
     venv\Scripts\activate
     ```
   - **Linux**:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```

3. Install the necessary dependencies:
   ```bash
   pip install -r requirements.txt

4. Use the testthemodel.py script to test predictions
   ```bash
   python testthemodel.py


# MQTT-Based Sensor Monitoring System

This project monitors simulated sensor data using an MQTT broker. Sensor data is published to MQTT topics, and actions such as sending SMS notifications or activating alarms can be triggered based on model predictions.

## Features

- **Sensor Data Publishing**: Publishes simulated sensor data (load sensor, tilt sensor, and ultrasonic distance) to an MQTT topic.
- **Alert System**: Processes sensor data using a pre-trained machine learning model and publishes alerts to an MQTT topic when an abnormal condition is detected.
- **Subscriber Actions**: Listens for MQTT messages to trigger actions like sounding an alarm or sending SMS notifications.

## Requirements

- Python 3.x
- MQTT broker (e.g., [Mosquitto](https://mosquitto.org/))
- Python packages:
  - `paho-mqtt`
  - `joblib`
  - `numpy`
  


```bash
Follow the "Getting Started" instructions upto step 3
```

Run the simulation 

```bash
python raspberrycode2.py
```
