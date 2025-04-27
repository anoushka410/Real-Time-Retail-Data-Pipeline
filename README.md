# Real-Time Retail Analytics Pipeline

This project demonstrates a real-time data pipeline and analytics dashboard for an online retail dataset. It simulates streaming transactional data, processes it using open-source tools, and visualizes insights through a live dashboard.

---

## Approach Taken

1. Simulated real-time ingestion of transactions using **Kafka Producer**.
2. Streamed, validated, and cleaned events using a **Kafka Consumer** (Python).
3. Stored curated data in a **partitioned PostgreSQL** table for efficient querying.
4. Built a **live-updating Streamlit dashboard** with transaction insights.
5. Exposed **custom Prometheus metrics** (success, failure, invalid counts) from the consumer.
6. Sent metrics to **Grafana Cloud** for real-time pipeline monitoring and alerting.

---

## Tech Stack Used

- **Ingestion**: Apache Kafka Producer
- **Processing**: Kafka Consumer (Python, psycopg2)
- **Storage**: PostgreSQL s
- **Visualization**: Streamlit (with Matplotlib and Seaborn)
- **Monitoring**: Prometheus + Grafana Cloud

---

## 📁 Repository Structure

```
├── kafka_producer/             # Kafka producer for simulated data feed
├── kafka_consumer/             # Python consumer, validation, DB insert, metrics
├── dashboard/                  # Streamlit dashboard app
├── db/                         # SQL scripts for PostgreSQL setup (partitioning, indexing)
├── monitoring/                 # Prometheus agent config and setup
├── requirements.txt
├── README.md
└── architecture.png            # Pipeline architecture diagram
```

---

## 🚀 Quick Start (Local)

```bash
# Install Python dependencies
pip install -r requirements.txt

# Start Kafka and Zookeeper (example)
docker-compose up

# Run Kafka Producer
python kafka_producer/producer.py

# Run Kafka Consumer (with Prometheus metrics exposed)
python kafka_consumer/consumer.py

# Run Streamlit Dashboard
streamlit run dashboard/app.py
```
---