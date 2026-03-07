import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

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
    st.markdown("Select a target from the available Database Nodes and apply temporal filters.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed. Ensure Streamlit secrets are active.")
        return

    db = client["StockHoldingByTMS"]
    collections = db.list_collection_names()

    if not collections:
        st.warning("⚠️ No data found in the Quantum Database. Please inject data first.")
        return

    # 1. Parse Available Data for Dropdowns
    stock_tms_map = {}
    for coll in collections:
        if "_" in coll:
            stock, tms = coll.split("_", 1)
            if stock not in stock_tms_map:
                stock_tms_map[stock] = []
            stock_tms_map[stock].append(tms)

    stocks_available = sorted(list(stock_tms_map.keys()))

    # 2. User Inputs (Dynamic Dropdowns)
    st.markdown("### 🎯 Target Lock")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_stock = st.selectbox("Select Stock Symbol", stocks_available)
    with col2:
        tms_available = sorted(stock_tms_map.get(selected_stock, []))
        selected_tms = st.selectbox("Select Broker TMS ID", tms_available)
    with col3:
        time_horizon = st.selectbox(
            "Temporal Filter (Date Range)", 
            ["All Time", "Last 7 Days", "Last 15 Days", "Last 30 Days", "Last 3 Months", "Custom Range"]
        )

    # Custom Date range logic
    custom_dates = []
    if time_horizon == "Custom Range":
        custom_dates = st.date_input("Select Custom Dates", [])

    # 3. Execution Logic
    if st.button("📡 Scan Database"):
        collection_name = f"{selected_stock}_{selected_tms}"
        collection = db[collection_name]
        
        # Fetch all data and process with Pandas for easier date manipulation
        data = list(collection.find().sort("date", 1))
        
        if not data:
            st.warning(f"Data anomaly: {collection_name} is empty.")
            return

        df = pd.DataFrame(data)
        df = df.drop(columns=["_id"], errors="ignore")
        
        # Clean Data Types
        df['date'] = pd.to_datetime(df['date'])
        numeric_cols = ['b_qty', 's_qty', 'b_amt', 's_amt']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 4. Apply Temporal Filter
        latest_date = df['date'].max()
        
        if time_horizon == "Last 7 Days":
            df = df[df['date'] >= (latest_date - timedelta(days=7))]
        elif time_horizon == "Last 15 Days":
            df = df[df['date'] >= (latest_date - timedelta(days=15))]
        elif time_horizon == "Last 30 Days":
            df = df[df['date'] >= (latest_date - timedelta(days=30))]
        elif time_horizon == "Last 3 Months":
            df = df[df['date'] >= (latest_date - timedelta(days=90))]
        elif time_horizon == "Custom Range" and len(custom_dates) == 2:
            start_date, end_date = custom_dates
            df = df[(df['date'] >= pd.to_datetime(start_date)) & (df['date'] <= pd.to_datetime(end_date))]

        if df.empty:
            st.warning("⚠️ No records found in the selected temporal range.")
            return

        # 5. Advanced Matrix Calculations
        # Handle division by zero using numpy/pandas safe operations
        df['Buy Rate'] = (df['b_amt'] / df['b_qty']).fillna(0).round(2)
        df['Sell Rate'] = (df['s_amt'] / df['s_qty']).fillna(0).round(2)
        
        # Net calculations
        df['Net Qty'] = df['b_qty'] - df['s_qty']
        df['Net Amt'] = df['b_amt'] - df['s_amt']
        
        # Cumulative calculations (requires dataframe sorted by date ascending)
        df = df.sort_values('date', ascending=True)
        df['Cum Qty'] = df['Net Qty'].cumsum()
        df['Cum Amt'] = df['Net Amt'].cumsum()

        # 6. Top Metrics (Based on filtered data)
        total_b_qty = df["b_qty"].sum()
        total_s_qty = df["s_qty"].sum()
        net_holding = total_b_qty - total_s_qty
        
        total_b_amt = df["b_amt"].sum()
        total_s_amt = df["s_amt"].sum()
        
        avg_buy_price = (total_b_amt / total_b_qty) if total_b_qty > 0 else 0
        avg_sell_price = (total_s_amt / total_s_qty) if total_s_qty > 0 else 0
        
        # Display the UI
        st.success(f"✅ Data Retrieved for {selected_stock} at TMS-{selected_tms} | Range: {time_horizon}")
        
        # Top Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Net Holding Volume", f"{int(net_holding):,}")
        m2.metric("Total Bought", f"{int(total_b_qty):,}")
        m3.metric("Total Sold", f"{int(total_s_qty):,}")
        
        if net_holding > 0:
            m4.metric("Phase Status", "🟢 Accumulating")
        elif net_holding < 0:
            m4.metric("Phase Status", "🔴 Distributing")
        else:
            m4.metric("Phase Status", "⚪ Neutral")

        # Price Averages
        st.markdown("### 💰 Capital Deployment Averages")
        p1, p2 = st.columns(2)
        p1.info(f"**Average Buy Price:** Rs. {avg_buy_price:.2f}")
        p2.error(f"**Average Sell Price:** Rs. {avg_sell_price:.2f}")
        
        # 7. Formatted Output Table
        st.markdown("### 🗃️ Advanced Ledger Matrix")
        
        # Reorder and format columns for human readability
        display_df = df[['date', 'b_qty', 'Buy Rate', 'b_amt', 's_qty', 'Sell Rate', 's_amt', 'Net Qty', 'Net Amt', 'Cum Qty', 'Cum Amt']].copy()
        display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
        
        # Make the table fill the container
        st.dataframe(display_df, use_container_width=True, hide_index=True)
