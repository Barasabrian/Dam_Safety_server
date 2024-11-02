# Assuming you've loaded the model
from joblib import load
import numpy as np

# Load the model
model = load('random_forest_model.joblib')

# Prepare new input data
load_value = 400  # Example load value in grams
tilt_status = 1   # Example tilt status (1 for tilted)
distance = 150.0  # Example distance in centimeters

# Create a 2D array-like structure for the model
new_data = np.array([[load_value, tilt_status, distance]])

# Make a prediction
prediction = model.predict(new_data)
print("Predicted class:", prediction)
