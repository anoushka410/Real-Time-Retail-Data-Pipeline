# PHASE 1: Real-Time Data Pipeline (Python + Kafka)

# --- Step 2: Read Real-Time Data Feed using Kafka Consumer ---
# This script reads the data record-by-record from a Kafka topic through a consumer and processes it.


from kafka import KafkaConsumer
from kafka import KafkaProducer
import pandas as pd
import json
import os
from mysql.connector import Error
import logging
import psycopg2
from psycopg2 import OperationalError
from pydantic import BaseModel, ValidationError
from prometheus_client import Counter, start_http_server
from dotenv import load_dotenv


# Fetch Environment variables
load_dotenv()

HOST = os.getenv("HOST")
DBNAME = os.getenv("DBNAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
PORT= os.getenv("HPORTOST")


# Define a Data Validation Schema in Pydantic
class RetailEvent(BaseModel):
    InvoiceNo: str
    StockCode: str
    Description: str
    Quantity: int
    InvoiceDate: str
    UnitPrice: float
    CustomerID: float
    Country: str

# Start Prometheus metrics server (on port 8000 by default)
start_http_server(8000)

# Define custom metrics
records_invalid = Counter('records_invalid', 'Total records with invalidated schema')
records_processed = Counter('records_processed_total', 'Total records processed')
records_success = Counter('records_success_total', 'Total successful inserts')
records_failed = Counter('records_failed_total', 'Total failed inserts due to duplicates')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to send Invalid Data to Dead Letter Queue (DLQ)
def send_to_dlq(dlq_producer, bad_data, error="Duplicate"):

    if error == "Duplicate":
        # Convert bad_data to JSON-serializable dict
        record_dict = {
            "InvoiceNo": bad_data[0],
            "StockCode": bad_data[1],
            "Description": bad_data[2],
            "Quantity": bad_data[3],
            "InvoiceDate": bad_data[4].isoformat() if isinstance(bad_data[4], (pd.Timestamp)) else str(bad_data[4]),
            "UnitPrice": bad_data[5],
            "CustomerID": bad_data[6],
            "Country": bad_data[7],
            "TotalAmount": bad_data[8]
        }
    else:
        record_dict = bad_data

    # Send invalid messages to a Kafka Producer
    dlq_message = json.dumps({"error": error, "data": record_dict})
    dlq_producer.send("online_retail_dlq", dlq_message.encode("utf-8"))

    # Store invalid messages in a local log file for easy monitoring
    with open("dlq_records.log", "a") as f:
        f.write(f"{record_dict}\n")

# Function to apply transformations
def transform_data(df):
    # Drop rows with missing CustomerID or InvoiceDate
    df.dropna(subset=["CustomerID", "InvoiceDate"], inplace=True)
    
    # Remove duplicates based on InvoiceNo and StockCode
    df.drop_duplicates(subset=["InvoiceNo", "StockCode"], inplace=True)
    
    # Convert InvoiceDate to datetime format
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
    
    # Handle negative quantities
    df = df[df['Quantity'] >= 0]
    
    # Handle negative unit prices
    df = df[df['UnitPrice'] >= 0]
    
    # Convert CustomerID to string and remove decimal points
    df['CustomerID'] = df['CustomerID'].astype(str).str.replace('.0', '')
    
    # Add derived columns
    df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
    
    # Remove any remaining rows with invalid dates
    # df = df[df['InvoiceDate'].notna()]
    
    return df

def create_database_connection():
    try:
        # connection = mysql.connector.connect(
        #     host='localhost',
        #     user='postgres',
        #     password='postgresql',
        #     database='real_time_db'
        # )

        connection = psycopg2.connect(
            database=DBNAME,
            user=USERNAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        
        return connection
    except Error as e:
        logging.error(f"Error connecting to Database: {e}")
        return None

def create_table_if_not_exists(cursor):
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS online_retail (
            InvoiceNo VARCHAR(20),
            StockCode VARCHAR(20),
            Description TEXT,
            Quantity INT,
            InvoiceDate TIMESTAMP,
            UnitPrice DECIMAL(10,2),
            CustomerID VARCHAR(20),
            Country VARCHAR(50),
            TotalAmount DECIMAL(10,2),
            PRIMARY KEY (InvoiceNo, StockCode)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
    except Error as e:
        logging.error(f"Error creating table: {e}")

def store_in_database(cursor, df):
    try:
        
        # Prepare the insert query (while discarding duplicate entries)
        insert_query = """
        INSERT INTO online_retail 
        (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country, TotalAmount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (InvoiceNo, StockCode, InvoiceDate) DO NOTHING
        """
        # ON CONFLICT (InvoiceNo, StockCode) DO UPDATE SET
        # Description = EXCLUDED.Description,
        # Quantity = EXCLUDED.Quantity,
        # UnitPrice = EXCLUDED.UnitPrice,
        # TotalAmount = EXCLUDED.TotalAmount
        
        # Convert DataFrame to list of tuples
        data = [tuple(x) for x in df[['InvoiceNo', 'StockCode', 'Description', 'Quantity', 
                                     'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country', 'TotalAmount']].values]
        
        success_count = 0
        for record in data:
            records_processed.inc()  # Increment processed count
            try:
                cursor.execute(insert_query, record)
                if cursor.rowcount == 1:
                    records_success.inc()  # Successfully inserted
                    success_count += 1
                else:
                    # Insert was skipped due to conflict
                    records_failed.inc()
                    send_to_dlq(dlq_producer, record)

            except Exception as e:
                connection.rollback()
                print(f"Error processing record: {e}")

        connection.commit()
        print("Batch processing completed.")
        # Execute batch insert
        # cursor.executemany(insert_query, data)
        # connection.commit()
        print(f"Successfully stored {success_count} records in the database")
        
    except Error as e:
        logging.error(f"Error storing data in database: {e}")

def process_data(buffer, cursor):
    # Create a DataFrame from the buffer
    df = pd.DataFrame(buffer)
    
    # Apply transformations
    df_clean = transform_data(df)
    
    if not df_clean.empty:
        if cursor:
            try:                
                # Store data in database
                store_in_database(cursor, df_clean)
            
            except OperationalError:
                print("Connection lost. Reconnecting...")
                connection = create_database_connection()
                cursor = connection.cursor()
                # Store data in database
                store_in_database(cursor, df_clean)
            # finally:
            #     connection.close()

            # Save to CSV as backup
            df_clean.to_csv("cleaned_data.csv", mode='a', header=False, index=False)
            
            print(f"Processed batch of {len(df_clean)} records")
    else:
        logging.warning("No valid records to process in this batch")

if __name__ == "__main__":
    
    # Connect to Postgres database
    connection = create_database_connection()
    cursor = connection.cursor()

    if cursor:
        # Create table if it doesn't exist
        # create_table_if_not_exists(cursor)
        print("Postgres Connection established!")
    else:
        print("Postgres connection could not be established")
    
    # Set up a Kafka Producer for the Dead Letter Queue
    dlq_producer = KafkaProducer(bootstrap_servers="localhost:9092")
    
    # Set up the Kafka Consumer
    consumer = KafkaConsumer(
        'online_retail',  # Topic name
        bootstrap_servers='localhost:9092',  # Kafka server
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='retail-consumer-group'
    )

    print("Starting Kafka consumer...")

    # List to hold incoming records temporarily
    buffer = []
    batch_size = 5  # Process in batches of 5 records

    try:
        for message in consumer:
            record = message.value
            print(f"Received record: {record}")
            try:
                event = RetailEvent(**record)  # Validation
            except ValidationError as e:
                send_to_dlq(dlq_producer, record, str(e))
                records_invalid.inc()   # Increment invalid count
                continue
            
            buffer.append(record)

            # Process in batches
            if len(buffer) >= batch_size:
                process_data(buffer, cursor)

                # Reset buffer after processing
                buffer = []

    except KeyboardInterrupt:
        print("Stopping consumer...")
        # Process any remaining records
        if buffer:
            process_data(buffer, cursor)

    finally:
        consumer.close()
        cursor.close()