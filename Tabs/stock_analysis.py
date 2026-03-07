import streamlit as st
import pandas as pd
import plotly.express as px
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

@st.cache_data(ttl=600)
def get_stock_list():
    client = get_db_connection()
    if not client: return []
    db = client["StockHoldingByTMS"]
    return sorted(db["market_trades"].distinct("stock"))

def run():
    st.title("📈 Stock-Centric Quantum Scanner")
    st.markdown("Analyze a specific asset to identify which Broker Nodes are controlling the supply.")
    
    client = get_db_connection()
    if not client:
        st.error("❌ Database Connection Offline.")
        return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]

    # 1. Search Interface
    available_stocks = get_stock_list()
    symbol = st.selectbox("Select or Type Symbol:", [""] + available_stocks).upper()
    
    if symbol:
        st.info(f"🔍 Analyzing Multiversal Ledgers for {symbol}...")
        
        # 2. V2 Aggregation Pipeline
        # We calculate the total performance of every broker for this specific stock
        pipeline = [
            {"$match": {"stock": symbol}},
            {"$group": {
                "_id": "$broker",
                "Total_Buy": {"$sum": "$b_qty"},
                "Total_Sell": {"$sum": "$s_qty"},
                "Net_Holding": {"$sum": {"$subtract": ["$b_qty", "$s_qty"]}}
            }},
            {"$sort": {"Net_Holding": -1}}
        ]
        
        results = list(master_col.aggregate(pipeline))
        
        if not results:
            st.warning(f"No trading signatures found for {symbol} in the master matrix.")
            return

        # 3. Data Processing
        df = pd.DataFrame(results)
        df.rename(columns={"_id": "TMS_ID"}, inplace=True)
        df['TMS_ID'] = df['TMS_ID'].apply(lambda x: f"TMS-{x}")
        
        # Separate Accumulators and Distributors
        acc = df[df['Net_Holding'] > 0].copy()
        dist = df[df['Net_Holding'] < 0].copy()

        # 4. UI Layout
        st.subheader(f"📊 {symbol} Broker Leaderboard")
        
        col1, col2 = st.columns(2)
        with col1:
            st.success("🎯 Top Accumulators (Buyers)")
            if not acc.empty:
                st.dataframe(acc[['TMS_ID', 'Net_Holding']].head(10), use_container_width=True, hide_index=True)
            else:
                st.write("No net buyers found.")
                
        with col2:
            st.error("📉 Top Distributors (Sellers)")
            if not dist.empty:
                # Convert to positive for better display in table if preferred, 
                # but we'll keep negative to show outflow
                st.dataframe(dist[['TMS_ID', 'Net_Holding']].sort_values('Net_Holding').head(10), 
                             use_container_width=True, hide_index=True)
            else:
                st.write("No net sellers found.")

        # 5. Visual Distribution
        st.subheader("📦 Supply Concentration Graph")
        # Show top 15 brokers by absolute trade volume
        df['Total_Vol'] = df['Total_Buy'] + df['Total_Sell']
        top_15 = df.sort_values('Total_Vol', ascending=False).head(15)
        
        fig = px.bar(
            top_15, 
            x='TMS_ID', 
            y='Net_Holding',
            color='Net_Holding',
            color_continuous_scale='RdYlGn',
            title=f"Net Position of Top 15 Most Active Brokers in {symbol}",
            labels={'Net_Holding': 'Net Quantity (Buy - Sell)'}
        )
        st.plotly_chart(fig, use_container_width=True)

        # 6. Summary Stats
        st.divider()
        total_market_vol = df['Total_Vol'].sum()
        st.caption(f"Total Market Volume Tracked for {symbol}: {total_market_vol:,} shares across {len(df)} brokers.")
