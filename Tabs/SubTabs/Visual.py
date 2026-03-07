import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import timedelta

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
    st.header("👁️ Visual Trade Flow Matrix")
    st.markdown("Analyze isolated market behavior across specific temporal windows. All charts represent localized activity.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed.")
        return

    db = client["StockHoldingByTMS"]
    collections = db.list_collection_names()

    if not collections:
        st.warning("⚠️ No data found in the Database.")
        return

    # 1. Parse Available Data
    stock_tms_map = {}
    for coll in collections:
        if "_" in coll:
            stock, tms = coll.split("_", 1)
            if stock not in stock_tms_map:
                stock_tms_map[stock] = []
            stock_tms_map[stock].append(tms)

    stocks_available = sorted(list(stock_tms_map.keys()))

    # 2. Dynamic Inputs
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_stock = st.selectbox("Select Stock", stocks_available, key="vis_stock")
    with col2:
        tms_available = sorted(stock_tms_map.get(selected_stock, []))
        selected_tms = st.selectbox("Select TMS ID", tms_available, key="vis_tms")
    with col3:
        time_horizon = st.selectbox(
            "Time Horizon (Strict Filter)", 
            ["All Time", "Last 7 Days", "Last 15 Days", "Last 30 Days", "Last 3 Months", "Custom Range"],
            key="vis_time"
        )

    custom_dates = []
    if time_horizon == "Custom Range":
        custom_dates = st.date_input("Select Custom Dates", [])

    # 3. Execution
    if st.button("📊 Render Visualizations"):
        collection_name = f"{selected_stock}_{selected_tms}"
        data = list(db[collection_name].find().sort("date", 1))
        
        if not data:
            st.warning("Data anomaly: Collection is empty.")
            return

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        for col in ['b_qty', 's_qty', 'b_amt', 's_amt']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 4. STRICT TEMPORAL FILTERING (Forgetting the past)
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
            df = df[(df['date'] >= pd.to_datetime(custom_dates[0])) & (df['date'] <= pd.to_datetime(custom_dates[1]))]

        if df.empty:
            st.warning("⚠️ No trades occurred during this isolated time window.")
            return

        # 5. Windowed Calculations (Starts from 0 for this timeline)
        df = df.sort_values('date', ascending=True)
        df['Net Qty'] = df['b_qty'] - df['s_qty']
        df['Cum Net Qty'] = df['Net Qty'].cumsum() # Accumulation specific to this window
        
        df['Buy Rate'] = (df['b_amt'] / df['b_qty']).fillna(0)
        df['Sell Rate'] = (df['s_amt'] / df['s_qty']).fillna(0)

        # Clean index for charting
        chart_df = df.set_index('date')

        # --- VISUALIZATION MATRIX ---
        st.success(f"✅ Visualizing behavior for {selected_stock} at TMS-{selected_tms} | Window: {time_horizon}")

        # Graph 1: Cumulative Net Accumulation
        st.markdown("### 🌊 Isolated Accumulation Curve (Net Holding)")
        st.markdown("*How their inventory grew or shrank specifically during this timeframe.*")
        st.area_chart(chart_df['Cum Net Qty'], color="#00ff00" if chart_df['Cum Net Qty'].iloc[-1] >= 0 else "#ff0000")

        colA, colB = st.columns(2)
        
        with colA:
            # Graph 2: Buy vs Sell Volume
            st.markdown("### 📊 Daily Volume Dynamics")
            st.markdown("*Raw number of shares bought vs sold per day.*")
            st.bar_chart(chart_df[['b_qty', 's_qty']], color=["#1f77b4", "#d62728"])

        with colB:
            # Graph 3: Capital Flow (Rupees)
            st.markdown("### 💸 Capital Deployment Flow")
            st.markdown("*Total Rs spent buying vs Rs gained selling.*")
            st.bar_chart(chart_df[['b_amt', 's_amt']], color=["#2ca02c", "#ff7f0e"])

        # Graph 4: Price Execution
        st.markdown("### 🎯 Price Execution Tracker")
        st.markdown("*Comparing the average prices they bought and sold at on specific days.*")
        
        import numpy as np
        
        # Use numpy's NaN instead of Pandas NA to keep data types strictly numeric for Streamlit
        price_df = chart_df[['Buy Rate', 'Sell Rate']].replace(0, np.nan).dropna(how='all')
        
        # Force the columns to be purely float types to eliminate any mixed-type anomalies
        price_df['Buy Rate'] = pd.to_numeric(price_df['Buy Rate'], errors='coerce')
        price_df['Sell Rate'] = pd.to_numeric(price_df['Sell Rate'], errors='coerce')

        st.line_chart(price_df, color=["#17becf", "#e377c2"])
        
        # Filter out 0 rates for a cleaner line chart
        price_df = chart_df[['Buy Rate', 'Sell Rate']].replace(0, pd.NA).dropna(how='all')
        st.line_chart(price_df, color=["#17becf", "#e377c2"])
