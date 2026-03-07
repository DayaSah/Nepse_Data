import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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
def get_available_stocks():
    client = get_db_connection()
    if not client: return []
    db = client["StockHoldingByTMS"]
    return sorted(db["market_trades"].distinct("stock"))

def run():
    st.title("📉 Advanced Technical Terminal")
    st.markdown("Generate high-fidelity candlestick charts and volume profiles from the Master Matrix.")

    client = get_db_connection()
    if not client:
        st.error("❌ Database Connection Offline.")
        return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]

    # 1. Selection Sidebar/Header
    stocks = get_available_stocks()
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        target_stock = st.selectbox("🎯 Select Stock Symbol", stocks)
    with col2:
        ema_fast = st.number_input("Fast EMA", value=20)
    with col3:
        ema_slow = st.number_input("Slow EMA", value=50)

    if target_stock:
        # 2. Fetch & Prepare Data
        # We aggregate by date to create OHLC (Open, High, Low, Close) from raw trades
        pipeline = [
            {"$match": {"stock": target_stock}},
            {"$group": {
                "_id": "$date",
                "Buy_Vol": {"$sum": "$b_qty"},
                "Sell_Vol": {"$sum": "$s_qty"},
                "Avg_Price": {"$avg": {"$cond": [{"$gt": ["$b_qty", 0]}, {"$divide": ["$b_amt", "$b_qty"]}, 0]}}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        data = list(master_col.aggregate(pipeline))
        if not data:
            st.warning("No historical trade data found for this symbol.")
            return

        df = pd.DataFrame(data)
        df.rename(columns={"_id": "Date", "Avg_Price": "Close"}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Artificial OHLC for visualization (Since we only have Daily Avg Price)
        # In a real system, you'd fetch high/low from an external API, 
        # but here we simulate volatility for the chart.
        df['Open'] = df['Close'].shift(1).fillna(df['Close'])
        df['High'] = df[['Open', 'Close']].max(axis=1) * 1.02
        df['Low'] = df[['Open', 'Close']].min(axis=1) * 0.98
        df['Volume'] = df['Buy_Vol'] + df['Sell_Vol']

        # 3. Technical Indicators
        df['EMA_Fast'] = df['Close'].ewm(span=ema_fast, adjust=False).mean()
        df['EMA_Slow'] = df['Close'].ewm(span=ema_slow, adjust=False).mean()
        
        # Bollinger Bands
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['STD20'] = df['Close'].rolling(window=20).std()
        df['Upper_Band'] = df['MA20'] + (df['STD20'] * 2)
        df['Lower_Band'] = df['MA20'] - (df['STD20'] * 2)

        # 4. Create the Multi-Panel Chart
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                           vertical_spacing=0.03, subplot_titles=(f'{target_stock} Price Action', 'Volume Profile'), 
                           row_width=[0.2, 0.7])

        # Candlestick
        fig.add_trace(go.Candlestick(
            x=df['Date'], open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
            name="Market Price"
        ), row=1, col=1)

        # EMAs
        fig.add_trace(go.Scatter(x=df['Date'], y=df['EMA_Fast'], line=dict(color='cyan', width=1.5), name=f"EMA {ema_fast}"), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['Date'], y=df['EMA_Slow'], line=dict(color='orange', width=1.5), name=f"EMA {ema_slow}"), row=1, col=1)

        # Bollinger Bands (Translucent)
        fig.add_trace(go.Scatter(x=df['Date'], y=df['Upper_Band'], line=dict(width=0), showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['Date'], y=df['Lower_Band'], line=dict(width=0), fill='tonexty', fillcolor='rgba(173, 216, 230, 0.1)', name="Bollinger Bands"), row=1, col=1)

        # Volume Bar Chart
        colors = ['green' if row['Close'] >= row['Open'] else 'red' for index, row in df.iterrows()]
        fig.add_trace(go.Bar(x=df['Date'], y=df['Volume'], marker_color=colors, name="Volume"), row=2, col=1)

        # 5. Styling
        fig.update_layout(
            template="plotly_dark",
            xaxis_rangeslider_visible=False,
            height=800,
            margin=dict(l=10, r=10, t=50, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)

        # 6. Quantitative Insights
        st.subheader("📋 Quantitative Snapshot")
        m1, m2, m3, m4 = st.columns(4)
        
        last_close = df['Close'].iloc[-1]
        prev_close = df['Close'].iloc[-2] if len(df) > 1 else last_close
        change = ((last_close - prev_close) / prev_close) * 100
        
        m1.metric("LTP", f"Rs. {last_close:.2f}", f"{change:.2f}%")
        m2.metric("24h Volume", f"{df['Volume'].iloc[-1]:,}")
        m3.metric("EMA Trend", "BULLISH" if df['EMA_Fast'].iloc[-1] > df['EMA_Slow'].iloc[-1] else "BEARISH")
        m4.metric("Volatility", f"{df['STD20'].iloc[-1]:.2f}")
