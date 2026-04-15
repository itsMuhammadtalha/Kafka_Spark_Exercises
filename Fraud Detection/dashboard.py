import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine

# Database Connection to TimescaleDB
engine = create_engine('postgresql://admin:admin_password@localhost:5433/fraud_db')

st.set_page_config(page_title="Real-Time Fraud Dashboard", layout="wide")
st.title(" Event-Driven Fraud Detection Dashboard")

placeholder = st.empty()

while True:
    try:
        # Query total transactions vs fraud
        query = """
        SELECT 
            is_fraud, 
            COUNT(*) as tx_count, 
            SUM(amount) as total_volume 
        FROM transactions 
        GROUP BY is_fraud
        """
        df = pd.read_sql(query, engine)
        
        with placeholder.container():
            st.markdown("###  Live Transaction Volumes")
            st.dataframe(df, use_container_width=True)
            
            if not df.empty and True in df['is_fraud'].values:
                st.error("🚨 FRAUD EVENTS DETECTED IN THE PIPELINE!")
                
                # Fetch recent frauds
                recent_frauds = pd.read_sql("SELECT * FROM transactions WHERE is_fraud = TRUE ORDER BY timestamp DESC LIMIT 5", engine)
                st.markdown("### 🛑 Last 5 Fraud Attempts Blocked")
                st.dataframe(recent_frauds, use_container_width=True)
            
    except Exception as e:
        # DB might not be ready or table not created yet
        st.warning("Waiting for the Spark Streaming pipeline to inject live data...")
        
    time.sleep(2)
