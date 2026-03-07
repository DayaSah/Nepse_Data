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
    # V2: Single distinct call on master collection
    return sorted(db["market_trades"].distinct("stock"))

def run():
    st.header("🏢 TMS Holdings & Market Share (V2)")
    st.markdown("Surgically analyze exactly which brokers control the liquidity and float of a specific stock.")

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

    if st.button("🔍 Map Market Control", type="primary"):
        # --- 2. BUILD THE QUERY & PIPELINE ---
        match_query = {"stock": target_stock}
        
        # Add date filtering to the query
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

        # Aggregation 1: Broker Summaries
        summary_pipeline = [
            {"$match": match_query},
            {"$group": {
                "_id": "$broker",
                "Buy Vol": {"$sum": "$b_qty"},
                "Sell Vol": {"$sum": "$s_qty"},
                "Buy Amt": {"$sum": "$b_amt"},
                "Sell Amt": {"$sum": "$s_amt"}
            }}
        ]
        
        raw_summary = list(master_col.aggregate(summary_pipeline))
        
        if not raw_summary:
            st.warning("No trading activity found for this stock in this specific timeframe.")
            return

        # Aggregation 2: Time Series Data for the "Race" chart
        timeline_pipeline = [
            {"$match": match_query},
            {"$project": {
                "date": 1, "broker": 1, 
                "net": {"$subtract": ["$b_qty", "$s_qty"]}
            }},
            {"$sort": {"date": 1}}
        ]
        raw_timeline = list(master_col.aggregate(timeline_pipeline))

        # --- 3. DATA PROCESSING ---
        mdf = pd.DataFrame(raw_summary)
        mdf.rename(columns={"_id": "Broker"}, inplace=True)
        mdf['Broker'] = mdf['Broker'].apply(lambda x: f"TMS-{x}")
        
        mdf['Net Holding'] = mdf['Buy Vol'] - mdf['Sell Vol']
        mdf['Total Vol'] = mdf['Buy Vol'] + mdf['Sell Vol']
        mdf['Avg Buy Price'] = (mdf['Buy Amt'] / mdf['Buy Vol']).fillna(0).round(2)
        
        # Prepare Timeline Data
        tdf = pd.DataFrame(raw_timeline)
        tdf['date'] = pd.to_datetime(tdf['date'])
        tdf['Broker'] = tdf['broker'].apply(lambda x: f"TMS-{x}")
        
        accumulators = mdf[mdf['Net Holding'] > 0].sort_values('Net Holding', ascending=False).copy()
        distributors = mdf[mdf['Net Holding'] < 0].sort_values('Net Holding', ascending=True).copy()
        
        st.success(f"✅ Market Control Scanned for {target_stock}")

        # --- 4. VISUALIZATION MATRIX ---
        
        # Donut Charts
        st.markdown("### 🥧 Market Control Breakdown")
        colA, colB = st.columns(2)
        with colA:
            if not accumulators.empty:
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

        # The Accumulation Race
        st.markdown("### 🏎️ The Accumulation Race")
        st.markdown("*Tracking the build-up of the Top 5 Net Buyers.*")
        if not accumulators.empty and not tdf.empty:
            top_5_brokers_ids = [b.replace("TMS-", "") for b in accumulators.head(5)['Broker'].tolist()]
            race_df = tdf[tdf['broker'].isin(top_5_brokers_ids)].copy()
            
            # Calculate cumulative holding per broker
            race_df = race_df.sort_values(['Broker', 'date'])
            race_df['Net Accumulation'] = race_df.groupby('Broker')['net'].cumsum()
            
            fig_race = px.line(
                race_df, x="date", y="Net Accumulation", color="Broker",
                markers=True, title="Cumulative Net Holding Over Time"
            )
            st.plotly_chart(fig_race, use_container_width=True)

        # VWAP Risk Scatter
        st.markdown("### 🎯 Broker Positioning & Risk (VWAP Map)")
        valid_acc = accumulators[accumulators['Avg Buy Price'] > 0]
        if not valid_acc.empty:
            fig_risk = px.scatter(
                valid_acc, x="Net Holding", y="Avg Buy Price", size="Total Vol", color="Broker",
                hover_data=["Buy Vol", "Sell Vol"],
                title="Broker Entry Pricing vs Retention Volume",
                labels={"Net Holding": "Total Shares Retained", "Avg Buy Price": "Average Entry Price (Rs)"}
            )
            st.plotly_chart(fig_risk, use_container_width=True)

        # Data Ledger
        st.markdown("### 🗃️ Market Share Ledger")
        display_df = mdf.sort_values("Net Holding", ascending=False).reset_index(drop=True)
        # Clean formatting
        display_df['Avg Buy Price'] = display_df['Avg Buy Price'].map('{:,.2f}'.format)
        st.dataframe(display_df[['Broker', 'Net Holding', 'Buy Vol', 'Sell Vol', 'Total Vol', 'Avg Buy Price']], use_container_width=True)
