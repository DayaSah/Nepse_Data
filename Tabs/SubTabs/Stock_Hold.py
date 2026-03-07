import streamlit as st
import pandas as pd
from pymongo import MongoClient

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_db_connection():
    try:
        uri = st.secrets["mongo"]["uri"]
        client = MongoClient(uri)
        return client
    except Exception as e:
        return None

def run():
    st.header("TMS Stock Inventory Scanner")
    st.markdown("Input a Broker ID to calculate their accumulated volume and average buy/sell prices.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed. Ensure Streamlit secrets are active.")
        return

    # 1. User Input
    col1, col2 = st.columns(2)
    with col1:
        tms_id = st.text_input("Enter Broker TMS ID (e.g., 58):", key="tms_hold_input").strip()
    with col2:
        stock_symbol = st.text_input("Enter Stock Symbol (e.g., NHPC):", key="stock_hold_input").upper().strip()

    # 2. Execution Logic
    if st.button("📡 Scan Database"):
        if not tms_id or not stock_symbol:
            st.warning("⚠️ Please enter both TMS ID and Stock Symbol.")
            return
            
        collection_name = f"{stock_symbol}_{tms_id}"
        db = client["StockHoldingByTMS"]
        
        # Check if the collection exists
        if collection_name not in db.list_collection_names():
            st.error(f"❌ No data found for Broker {tms_id} holding {stock_symbol}. Did you inject it?")
            return
            
        # Fetch data from MongoDB
        collection = db[collection_name]
        cursor = collection.find().sort("date", 1) # Sort by date ascending
        data = list(cursor)
        
        if not data:
            st.warning(f"Collection {collection_name} exists but is empty.")
            return

        # 3. Data Processing (Pandas)
        df = pd.DataFrame(data)
        df = df.drop(columns=["_id"]) # Remove Mongo object ID
        
        # Calculate Aggregates
        total_b_qty = df["b_qty"].sum()
        total_s_qty = df["s_qty"].sum()
        net_holding = total_b_qty - total_s_qty
        
        total_b_amt = df["b_amt"].sum()
        total_s_amt = df["s_amt"].sum()
        
        # Avoid division by zero
        avg_buy_price = (total_b_amt / total_b_qty) if total_b_qty > 0 else 0
        avg_sell_price = (total_s_amt / total_s_qty) if total_s_qty > 0 else 0
        
        # 4. Multiversal Display UI
        st.success(f"✅ Data Retrieved for {stock_symbol} at TMS-{tms_id}")
        
        # Top Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Net Holding Volume", f"{net_holding:,}")
        m2.metric("Total Bought", f"{total_b_qty:,}")
        m3.metric("Total Sold", f"{total_s_qty:,}")
        
        # Color code the Holding status
        if net_holding > 0:
            m4.metric("Status", "🟢 Accumulating")
        elif net_holding < 0:
            m4.metric("Status", "🔴 Distributing")
        else:
            m4.metric("Status", "⚪ Neutral")

        # Price Averages
        st.markdown("### 💰 Price Averages")
        p1, p2 = st.columns(2)
        p1.info(f"**Average Buy Price:** Rs. {avg_buy_price:.2f}")
        p2.error(f"**Average Sell Price:** Rs. {avg_sell_price:.2f}")

        # Charts
        st.markdown("### 📈 Volume Over Time")
        # Ensure date is datetime for charting
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        
        # Plot Buy vs Sell Volume
        st.bar_chart(df[['b_qty', 's_qty']])
        
        # Raw Data Table
        st.markdown("### 🗃️ Raw Ledger Matrix")
        st.dataframe(df, use_container_width=True)
