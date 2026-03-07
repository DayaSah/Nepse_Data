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
    st.header("🐋 Leviathan Protocol (Whale Tracker)")
    st.markdown("Scan the entire multiversal database to uncover which broker nodes are secretly hoarding or dumping massive volume.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed.")
        return

    db = client["StockHoldingByTMS"]
    collections = db.list_collection_names()

    if not collections:
        st.warning("⚠️ No data found in the Database.")
        return

    # 1. Parse Available Data to find all unique stocks
    unique_stocks = set()
    for coll in collections:
        if "_" in coll:
            stock = coll.split("_")[0]
            unique_stocks.add(stock)
            
    stocks_available = sorted(list(unique_stocks))

    # 2. Dynamic Inputs
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

    # 3. Execution - The Cross-Collection Scanner
    if st.button("🌊 Initiate Deep Scan"):
        # Find all collections belonging to this stock
        target_collections = [c for c in collections if c.startswith(f"{target_stock}_")]
        
        if not target_collections:
            st.error(f"No broker data found for {target_stock}.")
            return
            
        st.info(f"Scanning {len(target_collections)} different Broker Nodes for {target_stock}...")
        
        # Aggregate data from ALL brokers
        master_data = []
        
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
                
            # Calculate Broker's specific stats for this time window
            total_b_qty = df['b_qty'].sum()
            total_s_qty = df['s_qty'].sum()
            total_b_amt = df['b_amt'].sum()
            total_s_amt = df['s_amt'].sum()
            
            net_qty = total_b_qty - total_s_qty
            avg_buy = (total_b_amt / total_b_qty) if total_b_qty > 0 else 0
            avg_sell = (total_s_amt / total_s_qty) if total_s_qty > 0 else 0
            
            master_data.append({
                "Broker": f"TMS-{tms_id}",
                "Buy Vol": total_b_qty,
                "Sell Vol": total_s_qty,
                "Net Holding": net_qty,
                "Total Vol (Buy+Sell)": total_b_qty + total_s_qty,
                "Avg Buy Price": round(avg_buy, 2),
                "Avg Sell Price": round(avg_sell, 2)
            })
            
        if not master_data:
            st.warning("No trading activity found across any brokers in this specific timeframe.")
            return
            
        # Convert aggregated data into a Master DataFrame
        mdf = pd.DataFrame(master_data)
        
        # Separating the Accumulators (Net > 0) and Distributors (Net < 0)
        accumulators = mdf[mdf['Net Holding'] > 0].sort_values('Net Holding', ascending=False)
        distributors = mdf[mdf['Net Holding'] < 0].sort_values('Net Holding', ascending=True)

        st.success(f"✅ Deep Scan Complete: Identified behavior across {len(mdf)} active brokers.")

        # --- VISUALIZATION MATRIX ---
        
        # 🟢 CHART 1: The Whale Radar (Scatter Plot)
        st.markdown("### 📡 The Whale Radar")
        st.markdown("*Find outliers. Brokers sitting perfectly on the X-axis are pure buyers (no selling). Brokers high on the Y-axis are massive dumpers.*")
        
        fig_radar = px.scatter(
            mdf, x="Buy Vol", y="Sell Vol", size="Total Vol (Buy+Sell)", color="Net Holding",
            hover_name="Broker", hover_data=["Avg Buy Price", "Avg Sell Price"],
            color_continuous_scale=px.colors.diverging.RdYlGn,
            title="Broker Activity Matrix (Size = Total Volume)",
            labels={"Buy Vol": "Total Shares Bought", "Sell Vol": "Total Shares Sold"}
        )
        # Add a diagonal line for Break-Even (Buy = Sell)
        max_val = max(mdf['Buy Vol'].max(), mdf['Sell Vol'].max())
        fig_radar.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val, line=dict(color="White", dash="dash"))
        st.plotly_chart(fig_radar, use_container_width=True)

        colA, colB = st.columns(2)
        
        with colA:
            # 🟢 CHART 2: Top 10 Accumulators
            st.markdown("### 🟢 Top 10 Accumulators")
            if not accumulators.empty:
                top_acc = accumulators.head(10)
                fig_acc = px.bar(
                    top_acc, x="Net Holding", y="Broker", orientation='h',
                    color="Avg Buy Price", color_continuous_scale="Greens",
                    hover_data=["Buy Vol", "Sell Vol"],
                    title="Biggest Net Buyers (Color = Avg Entry Price)"
                )
                fig_acc.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_acc, use_container_width=True)
            else:
                st.info("No brokers accumulated net positive volume in this window.")
                
        with colB:
            # 🟢 CHART 3: Top 10 Distributors
            st.markdown("### 🔴 Top 10 Distributors")
            if not distributors.empty:
                top_dist = distributors.head(10)
                # Convert negative holding to positive for visual bar length, but keep label accurate
                fig_dist = px.bar(
                    top_dist, x="Net Holding", y="Broker", orientation='h',
                    color="Avg Sell Price", color_continuous_scale="Reds",
                    hover_data=["Buy Vol", "Sell Vol"],
                    title="Biggest Net Sellers (Color = Avg Exit Price)"
                )
                fig_dist.update_layout(yaxis={'categoryorder':'total descending'})
                st.plotly_chart(fig_dist, use_container_width=True)
            else:
                st.info("No brokers dumped net negative volume in this window.")

        # 🟢 CHART 4: Market Share Treemap (The Leviathan View)
        st.markdown("### 🗺️ Liquidity Dominance (Treemap)")
        st.markdown("*Which brokers control the most total volume (Buy + Sell)? Larger blocks indicate higher market control.*")
        
        fig_tree = px.treemap(
            mdf, path=[px.Constant("All Brokers"), "Broker"], values='Total Vol (Buy+Sell)',
            color='Net Holding', hover_data=['Buy Vol', 'Sell Vol', 'Avg Buy Price'],
            color_continuous_scale=px.colors.diverging.RdYlGn,
            color_continuous_midpoint=0
        )
        fig_tree.update_traces(root_color="black")
        fig_tree.update_layout(margin = dict(t=50, l=25, r=25, b=25))
        st.plotly_chart(fig_tree, use_container_width=True)
        
        # 🟢 DATA MATRIX: The Raw Output
        st.markdown("### 🗃️ Master Broker Leaderboard")
        st.dataframe(mdf.sort_values("Total Vol (Buy+Sell)", ascending=False).reset_index(drop=True), use_container_width=True)
