# Real-Time Fraud Detection System

### Video Demo

<div style="position: relative; padding-bottom: 56.25%; height: 0;"><iframe src="https://www.loom.com/embed/0792b6e9dd2a4a5e8463ec914f5bed0f" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

## 1. Boot up the Infrastructure
Open a terminal here and start Kafka and TimescaleDB:
```powershell
docker compose up -d
```

## 2. Setup your Python Environment
Make sure your environment has all the required packages:
```powershell
pip install -r requirements.txt
```

## 3. Run the Pipeline!
Open **three** separate terminal windows to run the components concurrently.

**Terminal 1 (The Dashboard):**
```powershell
streamlit run dashboard.py
```

**Terminal 2 (The Data Generator):**
```powershell
python fraud_producer.py
```

**Terminal 3 (The Streaming Engine):**
```powershell
python fraud_processor.py
```

### What's happening underneath:
- `fraud_producer.py` creates randomized Point-of-Sale logs into `transactions-topic`.
- `fraud_processor.py` reads them "at-least-once" (using the `./checkpoints` folder).
- It strictly deduplicates overlapping `transaction_id`s over a 10-minute watermark.
- If it sees a transaction > $5000, it flags it as `is_fraud = True`.
- It writes the aggregate data securely into your local TimescaleDB instance.
- It concurrently fires the explicit fraud events back into `fraud-alerts-topic`.
- The `dashboard.py` instantly picks up the materialized views from TimescaleDB and alerts you on screen!
