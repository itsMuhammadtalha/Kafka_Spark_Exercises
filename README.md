# Kafka & Spark Streaming Exercises

A real-time, event-driven streaming pipeline utilizing Apache Kafka, PySpark Structured Streaming, TimescaleDB, and a Streamlit dashboard.

## Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Java 11 (For PySpark)

## Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/itsMuhammadtalha/Kafka_Spark_Exercises.git
   cd Kafka_Spark_Exercises
   cd "Fraud Detection"
   ```

2. **Start the Infrastructure**
   ```bash
   # Starts Kafka, Zookeeper, and TimescaleDB
   docker compose up -d
   ```

3. **Install Dependencies**
   ```bash
   python -m venv venv
   .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

## Running the Pipeline

You need to run these components simultaneously in three separate terminals:

1. **Start the Real-Time Dashboard**
   ```bash
   streamlit run dashboard.py
   ```

2. **Start the Transaction Generator**
   ```bash
   python fraud_producer.py
   ```

3. **Start the Spark Streaming Engine**
   ```bash
   python fraud_processor.py
   ```

**Note**: To stop the databases and Kafka correctly, run `docker compose down -v`.
