import streamlit as st
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
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
        st.error(f"Connection Error: {e}")
        return None

@st.cache_data(ttl=600)
def get_unique_stocks_v2():
    client = get_db_connection()
    if not client: return []
    db = client["StockHoldingByTMS"]
    # V2: Direct distinct call on master collection
    return sorted(db["market_trades"].distinct("stock"))

def run():
    st.header("🐋 Leviathan Protocol (Whale Tracker)")
    st.markdown("Scan the entire multiversal database to uncover which broker nodes are secretly hoarding or dumping massive volume.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed.")
        return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]

    # 1. Dynamic Inputs
    stocks_available = get_unique_stocks_v2()

    if not stocks_available:
        st.warning("⚠️ No data found in the Master Matrix.")
        return

    col1, col2 = st.columns(2)
    with col1:
        target_stock = st.selectbox("🎯 Target Stock to Scan", stocks_available, key="whale_stock")
    with col2:
        time_horizon = st.selectbox(
            "⏳ Temporal Window", 
            ["All Time", "Last 7 Days", "Last 15 Days", "Last 30 Days", "Last 3 Months", "Custom Range"],
            key="whale_time"
        )

    custom_dates = []
    if time_horizon == "Custom Range":
        custom_dates = st.date_input("Select Custom Dates", [])

    # 3. Execution - High-Speed Aggregator
    if st.button("🌊 Initiate Deep Scan", type="primary"):
        # Build Match Query
        match_query = {"stock": target_stock}
        
        if time_horizon != "All Time":
            days_map = {"Last 7 Days": 7, "Last 15 Days": 15, "Last 30 Days": 30, "Last 3 Months": 90}
            if time_horizon in days_map:
                cutoff = (datetime.now() - timedelta(days=days_map[time_horizon])).strftime('%Y-%m-%d')
                match_query["date"] = {"$gte": cutoff}
            elif time_horizon == "Custom Range" and len(custom_dates) == 2:
                match_query["date"] = {
                    "$gte": custom_dates[0].strftime('%Y-%m-%d'),
                    "$lte": custom_dates[1].strftime('%Y-%m-%d')
                }

        # MongoDB Aggregation Pipeline (Deep Scan)
        pipeline = [
            {"$match": match_query},
            {"$group": {
                "_id": "$broker",
                "Buy Vol": {"$sum": "$b_qty"},
                "Sell Vol": {"$sum": "$s_qty"},
                "Buy Amt": {"$sum": "$b_amt"},
                "Sell Amt": {"$sum": "$s_amt"}
            }}
        ]
        
        raw_data = list(master_col.aggregate(pipeline))
        
        if not raw_data:
            st.warning("No trading activity found across any brokers in this specific timeframe.")
            return
            
        # Process Aggregated Data
        mdf = pd.DataFrame(raw_data)
        mdf.rename(columns={"_id": "Broker"}, inplace=True)
        mdf['Broker'] = mdf['Broker'].apply(lambda x: f"TMS-{x}")
        
        mdf['Net Holding'] = mdf['Buy Vol'] - mdf['Sell Vol']
        mdf['Total Vol (Buy+Sell)'] = mdf['Buy Vol'] + mdf['Sell Vol']
        mdf['Avg Buy Price'] = (mdf['Buy Amt'] / mdf['Buy Vol']).fillna(0).round(2)
        mdf['Avg Sell Price'] = (mdf['Sell Amt'] / mdf['Sell Vol']).fillna(0).round(2)
        
        accumulators = mdf[mdf['Net Holding'] > 0].sort_values('Net Holding', ascending=False)
        distributors = mdf[mdf['Net Holding'] < 0].sort_values('Net Holding', ascending=True)

        st.success(f"✅ Deep Scan Complete: Identified behavior across {len(mdf)} active brokers.")

        # --- VISUALIZATION MATRIX ---
        
        # 🟢 CHART 1: The Whale Radar
        st.markdown("### 📡 The Whale Radar")
        st.markdown("*Find outliers. X-axis = Buys, Y-axis = Sells. Color indicates Net holding position.*")
        
        fig_radar = px.scatter(
            mdf, x="Buy Vol", y="Sell Vol", size="Total Vol (Buy+Sell)", color="Net Holding",
            hover_name="Broker", hover_data=["Avg Buy Price", "Avg Sell Price"],
            color_continuous_scale=px.colors.diverging.RdYlGn,
            title="Broker Activity Matrix (Size = Total Volume)",
            labels={"Buy Vol": "Total Shares Bought", "Sell Vol": "Total Shares Sold"}
        )
        # 45-degree line represents perfect balance
        max_val = max(mdf['Buy Vol'].max(), mdf['Sell Vol'].max())
        fig_radar.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val, line=dict(color="rgba(255,255,255,0.3)", dash="dash"))
        st.plotly_chart(fig_radar, use_container_width=True)

        colA, colB = st.columns(2)
        
        with colA:
            st.markdown("### 🟢 Top 10 Accumulators")
            if not accumulators.empty:
                fig_acc = px.bar(
                    accumulators.head(10), x="Net Holding", y="Broker", orientation='h',
                    color="Avg Buy Price", color_continuous_scale="Greens",
                    title="Biggest Net Buyers"
                )
                fig_acc.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_acc, use_container_width=True)
            else:
                st.info("No net accumulation.")
                
        with colB:
            st.markdown("### 🔴 Top 10 Distributors")
            if not distributors.empty:
                fig_dist = px.bar(
                    distributors.head(10), x="Net Holding", y="Broker", orientation='h',
                    color="Avg Sell Price", color_continuous_scale="Reds",
                    title="Biggest Net Sellers"
                )
                fig_dist.update_layout(yaxis={'categoryorder':'total descending'})
                st.plotly_chart(fig_dist, use_container_width=True)
            else:
                st.info("No net distribution.")

        # 🟢 CHART 4: Market Share Treemap
        st.markdown("### 🗺️ Liquidity Dominance (Treemap)")
        fig_tree = px.treemap(
            mdf, path=[px.Constant("All Brokers"), "Broker"], values='Total Vol (Buy+Sell)',
            color='Net Holding', hover_data=['Buy Vol', 'Sell Vol', 'Avg Buy Price'],
            color_continuous_scale=px.colors.diverging.RdYlGn,
            color_continuous_midpoint=0
        )
        fig_tree.update_traces(root_color="black")
        st.plotly_chart(fig_tree, use_container_width=True)
        
        # 🟢 DATA MATRIX
        st.markdown("### 🗃️ Master Broker Leaderboard")
        st.dataframe(mdf.sort_values("Total Vol (Buy+Sell)", ascending=False).reset_index(drop=True), use_container_width=True)
