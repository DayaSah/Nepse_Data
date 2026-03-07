import streamlit as st
import json
import pandas as pd
from pymongo import MongoClient
import requests
import time

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
    st.title("💉 Multiversal Data Injector")
    st.markdown("Inject raw API dumps or auto-fetch data directly into the Quantum Database.")
    
    client = init_connection()
    
    # Create the Tabs
    tab1, tab2, tab3 = st.tabs([
        "📡 NepseAlpha Auto-Fetcher", 
        "📂 Manual File Injector", 
        "⚙️ API Settings"
    ])
    
    with tab1:
        st.header("Automated API Extraction")
        st.markdown("Directly extract floorsheet data from NepseAlpha without downloading files.")
        
        # User Inputs
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
            
        if st.button("⚡ Initiate Extraction Protocol"):
            if not fetch_stock:
                st.error("❌ Stock Symbol is required.")
                return
            if fetch_mode == "Single Broker" and not specific_broker:
                st.error("❌ Broker ID is required for Single mode.")
                return
            if not client:
                st.error("❌ MongoDB connection is not active.")
                return
                
            db = client["StockHoldingByTMS"]
            
            # Setup headers to look EXACTLY like a real human browser doing an AJAX request
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://nepsealpha.com/floorsheet-history",
                "X-Requested-With": "XMLHttpRequest", # <-- THIS IS THE MAGIC KEY
                "Connection": "keep-alive"
            }
            
            # Determine which brokers to scan
            brokers_to_scan = [specific_broker] if fetch_mode == "Single Broker" else [str(i) for i in range(1, 100)]
            
            st.info(f"Target locked on {fetch_stock}. Initiating extraction...")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            total_inserted = 0
            total_updated = 0
            brokers_found = 0
            
            for i, broker_id in enumerate(brokers_to_scan):
                status_text.text(f"Scanning TMS-{broker_id} for {fetch_stock}...")
                
                # Generate dynamic fsk timestamp
                fsk = int(time.time() * 1000)
                url = f"https://nepsealpha.com/floorsheet-history/filter?fsk=1772847797646&symbol={fetch_stock}&broker={broker_id}&dateRangeType={fetch_range}"
                
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        try:
                            json_data = response.json()
                            records = json_data.get("data", [])
                            
                            # If no data exists for this broker, skip quietly
                            if not records or len(records) == 0:
                                pass
                            else:
                                brokers_found += 1
                                collection_name = f"{fetch_stock}_{broker_id}"
                                collection = db[collection_name]
                                
                                # Upsert logic (Merge Database)
                                for record in records:
                                    date_val = record.get("date")
                                    if date_val:
                                        query = {"date": date_val}
                                        new_values = {"$set": {
                                            "b_qty": int(record.get("b_qty", 0)),
                                            "s_qty": int(record.get("s_qty", 0)),
                                            "b_amt": float(record.get("b_amt", 0)),
                                            "s_amt": float(record.get("s_amt", 0))
                                        }}
                                        result = collection.update_one(query, new_values, upsert=True)
                                        if result.matched_count > 0:
                                            total_updated += 1
                                        else:
                                            total_inserted += 1
                                            
                        except json.JSONDecodeError:
                            st.warning(f"Failed to parse JSON for Broker {broker_id}. They might have blocked the IP.")
                            break # Stop the loop if Cloudflare blocks us
                            
                except Exception as e:
                    st.error(f"Connection failed at Broker {broker_id}: {e}")
                    
                # Progress Bar
                progress_bar.progress((i + 1) / len(brokers_to_scan))
                
                # STEALTH MECHANISM: Pause to avoid IP Ban (Only if scanning all)
                if fetch_mode == "ALL Brokers (Stealth Scan)" and i < len(brokers_to_scan) - 1:
                    time.sleep(1.5) # Pauses for 1.5 seconds between each broker
                    
            status_text.text("Extraction Protocol Complete!")
            st.success(f"✅ Scanning Finished! Found data in {brokers_found} Broker Nodes.")
            st.markdown(f"- **New Records Inserted:** {total_inserted}")
            st.markdown(f"- **Existing Records Updated:** {total_updated}")

    with tab2:
        st.header("Manual TMS File Upload")
        st.markdown("Use this if the API Auto-Fetcher is blocked by Cloudflare.")
        
        col1, col2 = st.columns(2)
        with col1:
            manual_stock = st.text_input("Stock Symbol (e.g., NHPC):", key="man_stock").upper().strip()
        with col2:
            manual_tms = st.text_input("Broker TMS ID (e.g., 58):", key="man_tms").strip()
            
        uploaded_file = st.file_uploader("Upload Data File (.txt or .json format)", type=["txt", "json"])
        
        if st.button("🚀 Inject Manual Data"):
            if not manual_stock or not manual_tms:
                st.error("❌ Please enter both Stock Symbol and TMS ID.")
            elif uploaded_file is None:
                st.error("❌ Please upload a data file.")
            elif client is None:
                st.error("❌ MongoDB connection is not active.")
            else:
                try:
                    file_content = uploaded_file.read().decode("utf-8")
                    json_data = json.loads(file_content)
                    records = json_data.get("data", [])
                    
                    if not records:
                        st.error("No valid 'data' array found in the file.")
                    else:
                        db = client["StockHoldingByTMS"]
                        collection_name = f"{manual_stock}_{manual_tms}"
                        collection = db[collection_name]
                        
                        prog = st.progress(0)
                        u_count, i_count = 0, 0
                        
                        for i, record in enumerate(records):
                            date_val = record.get("date")
                            if date_val:
                                query = {"date": date_val}
                                new_values = {"$set": {
                                    "b_qty": int(record.get("b_qty", 0)),
                                    "s_qty": int(record.get("s_qty", 0)),
                                    "b_amt": float(record.get("b_amt", 0)),
                                    "s_amt": float(record.get("s_amt", 0))
                                }}
                                result = collection.update_one(query, new_values, upsert=True)
                                if result.matched_count > 0: u_count += 1
                                else: i_count += 1
                            prog.progress((i + 1) / len(records))
                            
                        st.success(f"✅ Injection Complete for **{collection_name}**! Inserted: {i_count}, Updated: {u_count}")
                except Exception as e:
                    st.error(f"❌ An error occurred: {e}")

    with tab3:
        st.header("🧪 API Diagnostics & Raw Tester")
        st.markdown("Paste the exact Request URL from your browser's Network tab to see exactly what the server is returning. We will view the unmodified response.")

        # User inputs the exact URL they copied from the browser
        test_url = st.text_input("Paste Raw API URL here:", placeholder="https://nepsealpha.com/floorsheet-history/filter?fsk=...")

        if st.button("🔍 Run Diagnostic Fetch"):
            if not test_url:
                st.warning("⚠️ Please paste a URL first.")
            else:
                # The exact headers we are currently using
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://nepsealpha.com/floorsheet-history",
                    "X-Requested-With": "XMLHttpRequest",
                    "Connection": "keep-alive"
                }

                st.info("Transmission sent. Waiting for server response...")
                
                try:
                    # Fire the request
                    response = requests.get(test_url, headers=headers, timeout=10)

                    # 1. Show Status Code
                    if response.status_code == 200:
                        st.success(f"Status Code: {response.status_code} (OK)")
                    else:
                        st.error(f"Status Code: {response.status_code} (Blocked/Failed)")

                    # 2. Show Headers (Helps us see if Cloudflare intercepted it)
                    st.markdown("### 📡 Response Headers")
                    st.json(dict(response.headers))

                    # 3. Show the RAW Body
                    st.markdown("### 📦 Raw Response Body")
                    
                    try:
                        # If it is valid JSON, it will format it nicely
                        json_data = response.json()
                        st.success("✅ Valid JSON payload received!")
                        st.json(json_data)
                    except json.JSONDecodeError:
                        # IF IT FAILS TO PARSE JSON, THIS WILL REVEAL THE HTML BLOCK PAGE
                        st.error("🛑 Response is NOT valid JSON. The server returned raw text/HTML.")
                        st.text_area("Unmodified Text Output:", response.text, height=400)

                except Exception as e:
                    st.error(f"Critical Transmission Failure: {e}")
