# PHASE 1: Real-Time Data Pipeline (Python + Kafka + Spark)

# --- Step 1: Simulate Real-Time Data Feed using Kafka Producer ---
# This script reads the CSV and sends rows to a Kafka topic as if they are real-time events.

from kafka import KafkaProducer
import time
import pandas as pd
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = 'online_retail'

# Read the Excel file
df = pd.read_csv('online_retail_data.csv')
df.drop(["Unnamed: 0"], axis=1, inplace=True)

# Convert DataFrame rows to dict and send to Kafka
for i, row in df[2200:2500].iterrows():
    # message = row.dropna().to_dict()  # Drop NaNs for cleaner payloads
    # message = row.to_dict()
    message = json.loads(row.to_json())
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(1)  # Simulate delay
    # if i >= 10:  # Only send first 10 rows for test
    #     break
