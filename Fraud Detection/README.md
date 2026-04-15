
[![Video Demo](https://cdn.loom.com/sessions/thumbnails/0792b6e9dd2a4a5e8463ec914f5bed0f-with-play.gif)](https://www.loom.com/share/0792b6e9dd2a4a5e8463ec914f5bed0f)

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
