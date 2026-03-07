import streamlit as st
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import timedelta
import plotly.express as px
import plotly.graph_objects as go

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
    st.header("🏢 TMS Holdings & Market Share")
    st.markdown("Surgically analyze exactly which brokers control the liquidity and float of a specific stock.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed.")
        return

    db = client["StockHoldingByTMS"]
    collections = db.list_collection_names()

    if not collections:
        st.warning("⚠️ No data found in the Database.")
        return

    # 1. Parse Available Stocks
    unique_stocks = set()
    for coll in collections:
        if "_" in coll:
            stock = coll.split("_")[0]
            unique_stocks.add(stock)
            
    stocks_available = sorted(list(unique_stocks))

    # 2. Dynamic Inputs
    col1, col2 = st.columns(2)
    with col1:
        target_stock = st.selectbox("🎯 Select Stock to Analyze", stocks_available, key="tms_hold_stock")
    with col2:
        time_horizon = st.selectbox(
            "⏳ Temporal Window", 
            ["All Time", "Last 7 Days", "Last 15 Days", "Last 30 Days", "Last 3 Months", "Custom Range"],
            key="tms_hold_time"
        )

    custom_dates = []
    if time_horizon == "Custom Range":
        custom_dates = st.date_input("Select Custom Dates", [])

    if st.button("🔍 Map Market Control"):
        # Find all collections belonging to this stock
        target_collections = [c for c in collections if c.startswith(f"{target_stock}_")]
        
        if not target_collections:
            st.error(f"No broker data found for {target_stock}.")
            return
            
        master_data = []
        timeline_data = [] # To store daily cumulative data for top brokers
        
        for coll_name in target_collections:
            tms_id = coll_name.split("_")[1]
            data = list(db[coll_name].find())
            
            if not data:
                continue
                
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            for col in ['b_qty', 's_qty', 'b_amt', 's_amt']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
            # Apply Temporal Filter
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
                continue
                
            # Broker's overall stats for this window
            total_b_qty = df['b_qty'].sum()
            total_s_qty = df['s_qty'].sum()
            total_b_amt = df['b_amt'].sum()
            total_s_amt = df['s_amt'].sum()
            
            net_qty = total_b_qty - total_s_qty
            avg_buy = (total_b_amt / total_b_qty) if total_b_qty > 0 else 0
            
            master_data.append({
                "Broker": f"TMS-{tms_id}",
                "Net Holding": net_qty,
                "Buy Vol": total_b_qty,
                "Sell Vol": total_s_qty,
                "Total Vol": total_b_qty + total_s_qty,
                "Avg Buy Price": round(avg_buy, 2)
            })

            # Timeline tracking for the multi-line chart
            df = df.sort_values('date')
            df['Net Accumulation'] = (df['b_qty'] - df['s_qty']).cumsum()
            df['Broker'] = f"TMS-{tms_id}"
            timeline_data.append(df[['date', 'Broker', 'Net Accumulation']])

        if not master_data:
            st.warning("No trading activity found for this stock in this specific timeframe.")
            return

        # 3. Data Structuring
        mdf = pd.DataFrame(master_data)
        time_df = pd.concat(timeline_data, ignore_index=True) if timeline_data else pd.DataFrame()

        accumulators = mdf[mdf['Net Holding'] > 0].copy()
        distributors = mdf[mdf['Net Holding'] < 0].copy()
        
        total_positive_holding = accumulators['Net Holding'].sum() if not accumulators.empty else 1 # Avoid div by 0
        
        # Calculate Percentage Holdings (Market Share of Accumulated Shares)
        if not accumulators.empty:
            accumulators['% Control'] = (accumulators['Net Holding'] / total_positive_holding) * 100
            accumulators = accumulators.sort_values('Net Holding', ascending=False)
        
        st.success(f"✅ Market Control Scanned for {target_stock}")

        # --- VISUALIZATION MATRIX ---
        
        # 🟢 CHART 1 & 2: Control Donuts
        st.markdown("### 🥧 Market Control Breakdown")
        colA, colB = st.columns(2)
        with colA:
            if not accumulators.empty:
                st.markdown("#### Who is Hoarding? (Net Buyers)")
                fig_hoard = px.pie(
                    accumulators.head(10), values='Net Holding', names='Broker', hole=0.4,
                    title="Top 10 Accumulators Control %",
                    color_discrete_sequence=px.colors.sequential.Greens_r
                )
                fig_hoard.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_hoard, use_container_width=True)
            else:
                st.info("No net accumulation in this period.")

        with colB:
            if not distributors.empty:
                st.markdown("#### Who is Dumping? (Net Sellers)")
                # Make negative values positive for the pie chart weight
                distributors['Abs Dumping'] = distributors['Net Holding'].abs()
                fig_dump = px.pie(
                    distributors.sort_values('Abs Dumping', ascending=False).head(10), 
                    values='Abs Dumping', names='Broker', hole=0.4,
                    title="Top 10 Distributors Control %",
                    color_discrete_sequence=px.colors.sequential.Reds_r
                )
                fig_dump.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_dump, use_container_width=True)
            else:
                st.info("No net distribution in this period.")

        # 🟢 CHART 3: The Accumulation Race (Multi-line Time Series)
        st.markdown("### 🏎️ The Accumulation Race")
        st.markdown("*How the Top 5 Net Buyers built their positions day by day.*")
        if not accumulators.empty and not time_df.empty:
            top_5_brokers = accumulators.head(5)['Broker'].tolist()
            top_5_time_df = time_df[time_df['Broker'].isin(top_5_brokers)]
            
            fig_race = px.line(
                top_5_time_df, x="date", y="Net Accumulation", color="Broker",
                markers=True, title="Cumulative Net Holding Over Time (Top 5 Brokers)"
            )
            st.plotly_chart(fig_race, use_container_width=True)

        # 🟢 CHART 4: VWAP Risk Scatter
        st.markdown("### 🎯 Broker Positioning & Risk (VWAP Map)")
        st.markdown("*X-Axis = Quantity Held. Y-Axis = Average Buy Price. Size = Total Trading Volume. Find the whales who bought cheap!*")
        
        # Filter to only accumulators who actually bought something
        valid_acc = accumulators[accumulators['Avg Buy Price'] > 0]
        if not valid_acc.empty:
            fig_risk = px.scatter(
                valid_acc, x="Net Holding", y="Avg Buy Price", size="Total Vol", color="Broker",
                hover_data=["Buy Vol", "Sell Vol", "% Control"],
                title="Smart Money vs Dumb Money",
                labels={"Net Holding": "Total Shares Retained", "Avg Buy Price": "Average Entry Price (Rs)"}
            )
            st.plotly_chart(fig_risk, use_container_width=True)

        # 🟢 DATA MATRIX
        st.markdown("### 🗃️ Market Share Ledger")
        display_df = mdf.sort_values("Net Holding", ascending=False).reset_index(drop=True)
        # Format for readability
        display_df['Avg Buy Price'] = display_df['Avg Buy Price'].apply(lambda x: f"Rs. {x:,.2f}")
        st.dataframe(display_df, use_container_width=True)
