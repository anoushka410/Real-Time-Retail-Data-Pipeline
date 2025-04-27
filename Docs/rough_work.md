## Run Kafka on Windows using .bat scripts

Open Command Prompt (not PowerShell)

### Start Zookeeper:

🔹 cd D:\Kafka\kafka_2.12-3.9.0  
🔹 .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Open a new command prompt for each process below.

### Start Kafka Broker:

🔹 cd D:\Kafka\kafka_2.12-3.9.0  
🔹 .\bin\windows\kafka-server-start.bat .\config\server.properties

### Create Kafka Topics:

🔹 .\bin\windows\kafka-topics.bat --create --topic online_retail --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1  
🔹 .\bin\windows\kafka-topics.bat --create --topic online_retail_dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

### Command to List Topics (Windows)

🔹 .\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

### Start Producer:

🔹 bin/kafka-console-producer.sh --topic online_retail --bootstrap-server localhost:9092

### Start Consumer:
🔹 bin/kafka-console-consumer.sh --topic online_retail --bootstrap-server localhost:9092 --from-beginning

🔹 .\bin\windows\kafka-console-consumer.bat --topic online_retail --bootstrap-server localhost:9092 --from-beginning

### Run Kafka Exporter:
🔹 .\kafka_exporter.exe --kafka.server=localhost:9092

### Run Prometheus:
🔹 prometheus.exe --config.file=prometheus.yml

## Next To Do

- Reject duplicates instead of overwriting
- ✅ Explore DBT for data transformations
- Add chart related to partitions (fetch data by month) in streamlit dashboard
- Check with new dataset
- PPT file
- Readme file
- Notion file with flow / outline of steps


1. Query Optimization

- Use indexes on InvoiceDate, Country, and Description columns in PostgreSQL.  
- Use partitioning (by date) for large historical data.


✅ 2. Real-Time Validation & Monitoring

- Kafka Consumer Side: 
    - Validate incoming data schema with Pydantic or Cerberus.  
    - Log invalid messages to a DLQ (Dead Letter Queue).
- Monitoring Stack:  
    - Integrate Prometheus for Kafka & consumer metrics (e.g., lag, success/failure count).  
    - Visualize in Grafana — show alerts for high lag or drop in ingestion.  


3. Scalability Strategy (Discussion Points)

- Horizontal Scaling: Use multiple Kafka consumers in a consumer group for parallel processing.  
- Backpressure Handling: Use Kafka's built-in batching and offset control to throttle if needed.  

- Event-driven architecture:
    - For production, consider Apache Flink or Spark Structured Streaming for scalable processing.

- Future Considerations:  
    - Offload transformations to dbt jobs via Airflow DAGs.
    - Move to managed Kafka (e.g., Confluent) for high availability.


## Why DBT is not used

✅ Practical Use of dbt:

- Transformations on stored batch data in PostgreSQL (e.g., summary tables, cleaned views).  
- Modeling historical data for reports beyond the 24h real-time window.  
- Documenting transformations, enforcing SQL logic standards.


⚠️ Limitations in this scenario:  

- DBT works post-ingestion, so not suited for real-time/event-based logic.  
- Would require you to periodically snapshot streaming data into tables, then run dbt.