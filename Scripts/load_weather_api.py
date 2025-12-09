import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

def load_to_supabase():
    csv_path = "../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    batch_size = 20
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size]
        batch = batch_df.where(pd.notnull(batch_df), None).to_dict(orient="records")

        for r in batch:
            r["temperature_c"] = r.get("temperature_C", None)
            r["wind_speed_kmph"] = r.get("wind_speed_kmph", None)
            r["humidity_percent"] = r.get("humidity_percent", None)
            r["city"] = r.get("city", "Hyderabad")
            if "temperature_C" in r:
                del r["temperature_C"]

        response = supabase.table("weatherdata").insert(batch).execute()

        if response.data is None:
            print("❗ Error inserting batch:", batch)
        else:
            print(f"✅ Inserted rows {i+1} → {min(i+batch_size, len(df))}")

        time.sleep(0.2)

    print("Finished loading weather data.")

if __name__ == "__main__":
    load_to_supabase()
