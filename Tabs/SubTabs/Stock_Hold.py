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
        st.error(f"Connection Error: {e}")
        return None

@st.cache_data(ttl=600)  # Cache stock list for 10 minutes
def get_available_assets():
    client = get_db_connection()
    if not client: return [], {}
    db = client["StockHoldingByTMS"]
    
    # NEW V2 LOGIC: Get unique stocks directly from the master collection
    stocks = sorted(db["market_trades"].distinct("stock"))
    return stocks

def run():
    st.header("🏢 TMS Stock Inventory Scanner (V2)")
    st.markdown("Query the **Master Ledger Matrix** for specific broker signatures.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed. Check secrets.toml.")
        return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]

    # 1. Dynamic Asset Selection (V2 Optimized)
    stocks_available = get_available_assets()

    if not stocks_available:
        st.warning("⚠️ No data found in the 'market_trades' collection.")
        return

    st.markdown("### 🎯 Target Lock")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_stock = st.selectbox("Select Stock Symbol", stocks_available)
    
    with col2:
        # Fetch only brokers who have actually traded THIS specific stock
        brokers_for_stock = sorted(master_col.distinct("broker", {"stock": selected_stock}), key=lambda x: int(x))
        selected_tms = st.selectbox("Select Broker ID", brokers_for_stock)
    
    with col3:
        time_horizon = st.selectbox(
            "Temporal Filter", 
            ["All Time", "Last 7 Days", "Last 15 Days", "Last 30 Days", "Last 3 Months", "Custom Range"]
        )

    custom_dates = []
    if time_horizon == "Custom Range":
        custom_dates = st.date_input("Select Range", [])

    # 2. Execution Logic
    if st.button("📡 Scan Master Matrix", type="primary"):
        # V2 QUERY: Filtering by stock AND broker in one collection
        query = {"stock": selected_stock, "broker": str(selected_tms)}
        
        # Add temporal filter to the MongoDB query for speed
        if time_horizon != "All Time":
            days_map = {"Last 7 Days": 7, "Last 15 Days": 15, "Last 30 Days": 30, "Last 3 Months": 90}
            if time_horizon in days_map:
                cutoff = (datetime.now() - timedelta(days=days_map[time_horizon])).strftime('%Y-%m-%d')
                query["date"] = {"$gte": cutoff}
            elif time_horizon == "Custom Range" and len(custom_dates) == 2:
                query["date"] = {
                    "$gte": custom_dates[0].strftime('%Y-%m-%d'),
                    "$lte": custom_dates[1].strftime('%Y-%m-%d')
                }

        data = list(master_col.find(query).sort("date", 1))
        
        if not data:
            st.warning(f"No records found for {selected_stock} by Broker {selected_tms} in this range.")
            return

        # 3. Data Processing
        df = pd.DataFrame(data)
        
        # Standardize Columns (V2 uses b_qty, s_qty, etc.)
        df['date'] = pd.to_datetime(df['date'])
        numeric_cols = ['b_qty', 's_qty', 'b_amt', 's_amt']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 4. Advanced Calculations
        df['Buy Rate'] = (df['b_amt'] / df['b_qty']).fillna(0).round(2)
        df['Sell Rate'] = (df['s_amt'] / df['s_qty']).fillna(0).round(2)
        df['Net Qty'] = df['b_qty'] - df['s_qty']
        df['Net Amt'] = df['b_amt'] - df['s_amt']
        
        df = df.sort_values('date', ascending=True)
        df['Cum Qty'] = df['Net Qty'].cumsum()
        
        # 5. Top Metrics
        total_b_qty = df["b_qty"].sum()
        total_s_qty = df["s_qty"].sum()
        net_holding = df["Net Qty"].sum() # Use sum of filtered range
        
        avg_buy_price = (df["b_amt"].sum() / total_b_qty) if total_b_qty > 0 else 0
        avg_sell_price = (df["s_amt"].sum() / total_s_qty) if total_s_qty > 0 else 0
        
        st.success(f"✅ Matrix Analysis Complete: {selected_stock} @ Broker {selected_tms}")
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Range Net Qty", f"{int(net_holding):,}")
        m2.metric("Total Bought", f"{int(total_b_qty):,}")
        m3.metric("Total Sold", f"{int(total_s_qty):,}")
        
        # Phase Detection
        if net_holding > 500: # Threshold for significance
            m4.metric("Phase", "🟢 Accumulating", delta="Bullish")
        elif net_holding < -500:
            m4.metric("Phase", "🔴 Distributing", delta="-Bearish")
        else:
            m4.metric("Phase", "⚪ Neutral", delta="Chop")

        # 6. UI Enhancements
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.info(f"**Est. Avg Buy Price:** Rs. {avg_buy_price:.2f}")
        with c2:
            st.error(f"**Est. Avg Sell Price:** Rs. {avg_sell_price:.2f}")

        # 7. Final Output Matrix
        st.markdown("### 🗃️ Detailed Transaction Ledger")
        display_df = df[['date', 'b_qty', 'Buy Rate', 'b_amt', 's_qty', 'Sell Rate', 's_amt', 'Net Qty', 'Cum Qty']].copy()
        display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
        
        st.dataframe(
            display_df.sort_values('date', ascending=False), 
            use_container_width=True, 
            hide_index=True
        )
