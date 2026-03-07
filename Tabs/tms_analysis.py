import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
    st.title("📘 Broker Intelligence Profiler (TMS Focus)")
    st.markdown("Deep-dive into specific Broker behavior and cross-market inventory.")

    client = get_db_connection()
    if not client:
        st.error("❌ Database Offline.")
        return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]

    # 1. Global Broker Selector
    all_brokers = sorted(master_col.distinct("broker"), key=lambda x: int(x) if str(x).isdigit() else 0)
    
    col_a, col_b = st.columns([1, 2])
    with col_a:
        target_tms = st.selectbox("🕵️ Select Broker Node (TMS ID)", all_brokers, index=all_brokers.index("58") if "58" in all_brokers else 0)
    with col_b:
        lookback = st.slider("Lookback Period (Days)", 7, 365, 30)

    # --- DATA ENGINE ---
    cutoff = (datetime.now() - timedelta(days=lookback)).strftime('%Y-%m-%d')
    
    # Fetch all activity for this broker
    data = list(master_col.find({"broker": str(target_tms), "date": {"$gte": cutoff}}))
    
    if not data:
        st.warning(f"No recent activity detected for TMS-{target_tms} in the last {lookback} days.")
        return

    df = pd.DataFrame(data)
    df['Net_Qty'] = df['b_qty'] - df['s_qty']
    df['Total_Vol'] = df['b_qty'] + df['s_qty']
    df['Turnover'] = df['b_amt'] + df['s_amt']

    # --- BRAIN: BROKER PERSONALITY ---
    total_buy = df['b_qty'].sum()
    total_sell = df['s_qty'].sum()
    net_position = total_buy - total_sell
    
    if abs(net_position) < (total_buy * 0.05):
        style = "🔄 Day Trader / Scalper"
        color = "orange"
    elif net_position > 0:
        style = "🐋 Aggressive Accumulator"
        color = "green"
    else:
        style = "🔻 Liquidation / Exit Mode"
        color = "red"

    # --- UI LAYOUT ---
    st.markdown(f"### 🛡️ Profile: TMS-{target_tms} | <span style='color:{color}'>{style}</span>", unsafe_allow_html=True)
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Stocks Traded", len(df['stock'].unique()))
    m2.metric("Net Inventory Change", f"{net_position:,}")
    m3.metric("Market Turnover", f"Rs. {df['Turnover'].sum():,.0f}")
    m4.metric("Avg Trade Size", f"{(df['Total_Vol'].mean()):,.0f}")

    # --- TABBED ANALYSIS ---
    tab1, tab2, tab3 = st.tabs(["📊 Inventory DNA", "📈 Timeline Flow", "🔍 Cross-Broker Correlation"])

    with tab1:
        st.subheader(f"Top Holdings / Activities for TMS-{target_tms}")
        
        # Aggregate by stock
        stock_agg = df.groupby('stock').agg({
            'b_qty': 'sum',
            's_qty': 'sum',
            'Net_Qty': 'sum',
            'Turnover': 'sum'
        }).reset_index().sort_values('Turnover', ascending=False)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Most Traded Stocks (By Turnover)**")
            fig_pie = px.pie(stock_agg.head(10), values='Turnover', names='stock', hole=0.4, 
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig_pie, use_container_width=True)

        with col2:
            st.markdown("**Net Position by Asset**")
            # Bar chart showing what they are buying vs selling the most
            fig_bar = px.bar(stock_agg.head(15), x='stock', y='Net_Qty', 
                             color='Net_Qty', color_continuous_scale='RdYlGn')
            st.plotly_chart(fig_bar, use_container_width=True)

    with tab2:
        st.subheader("Daily Activity Pulse")
        # Daily flow of Buy vs Sell
        daily_flow = df.groupby('date').agg({'b_qty': 'sum', 's_qty': 'sum'}).reset_index()
        
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(x=daily_flow['date'], y=daily_flow['b_qty'], name="Daily Buying", line=dict(color='green')))
        fig_line.add_trace(go.Scatter(x=daily_flow['date'], y=daily_flow['s_qty'], name="Daily Selling", line=dict(color='red')))
        fig_line.update_layout(title="Volume Flow Timeline", template="plotly_dark")
        st.plotly_chart(fig_line, use_container_width=True)

    with tab3:
        st.subheader("Sync-Scanner (Experimental)")
        st.markdown("Who is this broker trading with? (Based on same-day activity in same stocks)")
        
        # Pick a stock this broker is heavy in
        top_stock = stock_agg.iloc[0]['stock']
        st.write(f"Analyzing Market Sync for **{top_stock}**...")
        
        sync_data = list(master_col.find({"stock": top_stock, "date": {"$gte": cutoff}}))
        sdf = pd.DataFrame(sync_data)
        
        if not sdf.empty:
            sync_agg = sdf.groupby('broker').agg({'b_qty': 'sum', 's_qty': 'sum'}).reset_index()
            sync_agg['Net'] = sync_agg['b_qty'] - sync_agg['s_qty']
            sync_agg = sync_agg[sync_agg['broker'] != str(target_tms)] # Remove target
            
            # Find the "Opposite" broker (Who sells when Target buys)
            opposite = sync_agg.sort_values('Net').iloc[0] if net_position > 0 else sync_agg.sort_values('Net', ascending=False).iloc[0]
            
            st.info(f"💡 Potential Counterparty: **TMS-{opposite['broker']}** has the exact opposite net position in {top_stock}. They might be the ones supplying/absorbing the liquidity for TMS-{target_tms}.")
            
            fig_sync = px.scatter(sync_agg, x='b_qty', y='s_qty', hover_name='broker', size='b_qty',
                                  title=f"All Brokers active in {top_stock}")
            st.plotly_chart(fig_sync, use_container_width=True)

    # Final Raw Data view
    with st.expander("View Full Ledger for this Broker"):
        st.dataframe(df.sort_values('date', ascending=False), use_container_width=True)
