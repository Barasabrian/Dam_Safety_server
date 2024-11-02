import pandas as pd
import random

def generate_sensor_data():
    """Generates synthetic data for the sensors with variance."""
    # Generate a synthetic load value (e.g., -100 to 1000 grams)
    load_value = random.randint(-100, 1000)
    
    # Generate tilt status (0 for normal, 1 for tilted)
    tilt_status = random.randint(0, 1)
    
    # Generate a synthetic distance measurement (e.g., 0 to 400 cm)
    distance = random.uniform(0, 400)
    
    # Introduce random noise to load_value
    load_noise = random.uniform(-10, 10)  # Adding noise in the range of -10 to 10 grams
    load_value += load_noise
    
    # Ensure load_value stays within realistic bounds
    load_value = max(-100, min(1000, load_value))
    
    # Introduce random noise to distance for realism
    distance_noise = random.uniform(-5, 5)  # Adding noise in the range of -5 to 5 cm
    distance += distance_noise
    
    # Ensuring distance stays within realistic bounds
    distance = max(0, min(400, distance))
    
    return load_value, tilt_status, distance

def generate_training_data(num_samples=100000):
    """Generates a dataset for training with varied critical conditions."""
    data = []
    
    for _ in range(num_samples):
        load_value, tilt_status, distance = generate_sensor_data()
        
        # Randomly set thresholds for critical conditions
        load_threshold = random.randint(300, 700)  # Random threshold for load_value
        distance_threshold = random.uniform(0, 250)  # Random threshold for distance
        
        # Define critical conditions with variability
        critical_condition = False
        if (load_value > load_threshold and distance < distance_threshold) or \
           (tilt_status == 1 and load_value > load_threshold / 2) or \
           (distance < 50 and load_value > load_threshold / 1.5):
            critical_condition = True
            
        # Assign label based on critical condition
        label = 1 if critical_condition else 0
        
        # Introduce random noise to labels to simulate errors
        if random.random() < 0.05:  # 5% chance to flip the label
            label = 1 - label
            
        data.append([load_value, tilt_status, distance, label])
    
    # Create a DataFrame
    df = pd.DataFrame(data, columns=['load_value', 'tilt_status', 'distance', 'label'])
    return df

# Example usage
training_data = generate_training_data()
training_data.to_csv("sensor_training_data.csv", index=False)
