import json
import pandas as pd
import os
import glob

def transform_weather_data():
    # Create staged directory if not exists
    os.makedirs("../data/staged", exist_ok=True)

    # Find latest weather JSON file
    files = sorted(glob.glob("../data/raw/weather_*.json"))
    if not files:
        print("No weather files found in ../data/raw/")
        return None

    latest_file = files[-1]
    
    # Load JSON
    with open(latest_file, "r") as f:
        data = json.load(f)

    hourly = data["hourly"]

    # Convert to DataFrame
    df = pd.DataFrame({
        "time": hourly["time"],
        "temperature_C": hourly["temperature_2m"],
        "humidity_percent": hourly["relative_humidity_2m"],
        "wind_speed_kmph": [w * 3.6 for w in hourly["wind_speed_10m"]],  # Convert m/s to km/h
    })

    df["city"] = "Hyderabad"
    df["extracted_at"] = pd.Timestamp.now()

    # Save to CSV
    output_path = "../data/staged/weather_cleaned.csv"
    df.to_csv(output_path, index=False)
    print(f"Transformed data saved to: {output_path}")
    return df

if __name__ == "__main__":
    transform_weather_data()
