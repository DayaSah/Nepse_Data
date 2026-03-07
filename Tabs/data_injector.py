import streamlit as st
import json
import pandas as pd
from pymongo import MongoClient
import requests
import time
from datetime import datetime

# --- DATABASE CONNECTION ---
@st.cache_resource
def init_connection():
    try:
        uri = st.secrets["mongo"]["uri"] 
        client = MongoClient(uri)
        return client
    except Exception as e:
        st.error(f"Database Connection Failed: {e}")
        return None

def run():
    st.title("💉 Multiversal Data Injector (V2)")
    st.markdown("Inject raw API dumps or auto-fetch data directly into the **Master Matrix**.")
    
    client = init_connection()
    if not client: return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]
    
    tab1, tab2, tab3 = st.tabs([
        "📡 NepseAlpha Auto-Fetcher", 
        "📂 Manual JSON Injector", 
        "⚙️ API Diagnostics"
    ])
    
    with tab1:
        st.header("Automated API Extraction")
        st.markdown("Directly extract floorsheet data and sync with Master Collection.")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            fetch_stock = st.text_input("Stock Symbol (e.g., ULHC):", key="fetch_stock").upper().strip()
        with col2:
            fetch_range = st.selectbox("Date Range", ["1month", "3month", "6month", "1year", "2year", "custom"], index=4)
        with col3:
            fetch_mode = st.selectbox("Extraction Mode", ["Single Broker", "ALL Brokers (Stealth Scan)"])
            
        specific_broker = ""
        if fetch_mode == "Single Broker":
            specific_broker = st.text_input("Enter Broker ID (e.g., 44):").strip()
            
        if st.button("⚡ Initiate Extraction Protocol", type="primary"):
            if not fetch_stock:
                st.error("❌ Stock Symbol is required.")
                return
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://nepsealpha.com/floorsheet-history"
            }
            
            brokers_to_scan = [specific_broker] if fetch_mode == "Single Broker" else [str(i) for i in range(1, 100)]
            st.info(f"Target locked on {fetch_stock}. Scanning nodes...")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            total_processed = 0
            
            for i, broker_id in enumerate(brokers_to_scan):
                status_text.text(f"Scanning TMS-{broker_id}...")
                url = f"https://nepsealpha.com/floorsheet-history/filter?symbol={fetch_stock}&broker={broker_id}&dateRangeType={fetch_range}"
                
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    if response.status_code == 200:
                        records = response.json().get("data", [])
                        if records:
                            # --- V2 SYNC LOGIC ---
                            for record in records:
                                date_val = record.get("date")
                                if not date_val: continue
                                
                                # 1. Prepare standardized document
                                doc = {
                                    "stock": fetch_stock,
                                    "broker": str(broker_id),
                                    "date": date_val,
                                    "b_qty": int(record.get("b_qty", 0)),
                                    "s_qty": int(record.get("s_qty", 0)),
                                    "b_amt": float(record.get("b_amt", 0)),
                                    "s_amt": float(record.get("s_amt", 0))
                                }
                                
                                # 2. Upsert into Master Collection (V2 Core)
                                master_col.update_one(
                                    {"stock": fetch_stock, "broker": str(broker_id), "date": date_val},
                                    {"$set": doc},
                                    upsert=True
                                )
                                total_processed += 1
                            
                except Exception as e:
                    st.warning(f"Node {broker_id} connection skipped.")
                
                progress_bar.progress((i + 1) / len(brokers_to_scan))
                if fetch_mode == "ALL Brokers (Stealth Scan)": time.sleep(1.0)
                    
            st.success(f"✅ Protocol Finished! {total_processed} snapshots synced to Master Matrix.")

    with tab2:
        st.header("📂 Manual JSON Payload Injector")
        st.markdown("Paste raw JSON from NepseAlpha to bypass all blocks.")
        
        colA, colB = st.columns(2)
        with colA:
            m_stock = st.text_input("Symbol:", key="m_s").upper().strip()
        with colB:
            m_broker = st.text_input("Broker ID:", key="m_b").strip()
            
        json_payload = st.text_area("Paste JSON:", height=200)
        
        if st.button("💉 Inject Into Master Matrix"):
            if not m_stock or not m_broker or not json_payload:
                st.error("Missing credentials or payload.")
                return
            
            try:
                data = json.loads(json_payload)
                records = data.get("data", [])
                count = 0
                for r in records:
                    date_v = r.get("date")
                    if date_v:
                        doc = {
                            "stock": m_stock, "broker": str(m_broker), "date": date_v,
                            "b_qty": int(r.get("b_qty", 0)), "s_qty": int(r.get("s_qty", 0)),
                            "b_amt": float(r.get("b_amt", 0)), "s_amt": float(r.get("s_amt", 0))
                        }
                        master_col.update_one(
                            {"stock": m_stock, "broker": str(m_broker), "date": date_v},
                            {"$set": doc}, upsert=True
                        )
                        count += 1
                st.success(f"Successfully injected {count} records into Master Matrix.")
            except Exception as e:
                st.error(f"Injection Failed: {e}")

    with tab3:
        st.header("🧪 API Diagnostics")
        test_url = st.text_input("URL:", placeholder="https://nepsealpha.com/...")
        if st.button("🔍 Test Connection"):
            res = requests.get(test_url, headers={"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"})
            st.write(f"Status: {res.status_code}")
            st.json(res.text[:500])
