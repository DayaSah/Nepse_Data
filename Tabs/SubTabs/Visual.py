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
def get_stock_list_v2():
    client = get_db_connection()
    if not client: return []
    db = client["StockHoldingByTMS"]
    # Efficiently fetch unique stocks from the master collection
    return sorted(db["market_trades"].distinct("stock"))

def run():
    st.header("👁️ Omniscient Visual Matrix (V2)")
    st.markdown("A multi-dimensional view of broker behavior. Hover over charts for exact telemetry.")

    client = get_db_connection()
    if not client:
        st.error("❌ MongoDB connection failed.")
        return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]

    # 1. Dynamic Asset & Node Selection
    stocks_available = get_stock_list_v2()

    if not stocks_available:
        st.warning("⚠️ No data found in the Master Matrix.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        selected_stock = st.selectbox("Select Target Stock", stocks_available, key="vis_stock")
    
    with col2:
        # V2: Context-aware broker list based on selected stock
        brokers_available = sorted(master_col.distinct("broker", {"stock": selected_stock}), key=lambda x: int(x))
        selected_tms = st.selectbox("Select TMS Node", brokers_available, key="vis_tms")
        
    with col3:
        time_horizon = st.selectbox(
            "Temporal Window", 
            ["All Time", "Last 7 Days", "Last 15 Days", "Last 30 Days", "Last 3 Months", "Custom Range"],
            key="vis_time"
        )

    custom_dates = []
    if time_horizon == "Custom Range":
        custom_dates = st.date_input("Select Range", [])

    # 2. Execution & Processing
    if st.button("🌌 Render Holographics", type="primary"):
        # V2 Optimized Query
        query = {"stock": selected_stock, "broker": str(selected_tms)}
        
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
            st.warning("No trace signatures found in this specific temporal window.")
            return

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Numeric Sanitization
        for col in ['b_qty', 's_qty', 'b_amt', 's_amt']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 3. Enhanced Calculations
        df = df.sort_values('date', ascending=True)
        df['Net Qty'] = df['b_qty'] - df['s_qty']
        df['Cum Net Qty'] = df['Net Qty'].cumsum() 
        
        df['Buy Rate'] = (df['b_amt'] / df['b_qty']).replace([np.inf, -np.inf], np.nan).fillna(0)
        df['Sell Rate'] = (df['s_amt'] / df['s_qty']).replace([np.inf, -np.inf], np.nan).fillna(0)
        
        df['Day of Week'] = df['date'].dt.day_name()

        st.success(f"✅ Rendered: {selected_stock} @ TMS-{selected_tms}")

        # --- 4. VISUALIZATION MATRIX ---
        
        # 🟢 CHART 1: THE ACCUMULATION CURVE
        st.markdown("### 🌊 Inventory Accumulation Curve")
        curve_color = "#00ff00" if df['Cum Net Qty'].iloc[-1] >= 0 else "#ff0000"
        st.area_chart(df.set_index('date')['Cum Net Qty'], color=curve_color)

        # Create tabs for multi-dimensional analysis
        v_tab1, v_tab2, v_tab3 = st.tabs(["📊 Volume Dynamics", "💰 Price & Capital", "🧠 Behavioral Patterns"])

        with v_tab1:
            st.markdown("#### Buy vs Sell Volume Delta")
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(x=df['date'], y=df['b_qty'], name='Bought', marker_color='#00CC96'))
            fig_vol.add_trace(go.Bar(x=df['date'], y=-df['s_qty'], name='Sold', marker_color='#EF553B'))
            fig_vol.update_layout(
                barmode='relative', 
                hovermode="x unified", 
                title="Daily Volume Delta (Magnitude of Trade)",
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="Timeline",
                yaxis_title="Quantity"
            )
            st.plotly_chart(fig_vol, use_container_width=True)

        with v_tab2:
            colA, colB = st.columns(2)
            with colA:
                st.markdown("#### Capital Distribution")
                total_b = df['b_amt'].sum()
                total_s = df['s_amt'].sum()
                fig_pie = px.pie(
                    names=['Capital Spent', 'Capital Realized'], 
                    values=[total_b, total_s],
                    hole=0.4,
                    color_discrete_sequence=['#636EFA', '#FECB52']
                )
                fig_pie.update_traces(textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)
                
            with colB:
                st.markdown("#### Buy Price Intensity")
                buy_df = df[df['b_qty'] > 0]
                if not buy_df.empty:
                    fig_scatter = px.scatter(
                        buy_df, x='date', y='Buy Rate', size='b_qty', 
                        color='Buy Rate', color_continuous_scale='Viridis',
                        title="Price Execution vs Volume Bubble"
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)
                else:
                    st.info("No buy transactions to plot.")

            st.markdown("#### Execution Price Density")
            # Combine non-zero rates for a full picture of trading zones
            valid_rates = pd.concat([df[df['b_qty']>0]['Buy Rate'], df[df['s_qty']>0]['Sell Rate']])
            if not valid_rates.empty:
                fig_hist = px.histogram(
                    valid_rates, nbins=30, 
                    title="Volume Weighting at Specific Price Points",
                    color_discrete_sequence=['#AB63FA']
                )
                fig_hist.update_layout(xaxis_title="Price (Rs.)", yaxis_title="Trade Frequency", bargap=0.1)
                st.plotly_chart(fig_hist, use_container_width=True)

        with v_tab3:
            st.markdown("#### Weekday Aggression Profile")
            st.markdown("*Determining if this broker is more aggressive on specific market days.*")
            day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
            day_df = df.groupby('Day of Week')[['b_qty', 's_qty']].sum().reindex(day_order).fillna(0).reset_index()
            
            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(
                r=day_df['b_qty'], theta=day_df['Day of Week'], fill='toself', name='Buy Bias', line_color='#00CC96'
            ))
            fig_radar.add_trace(go.Scatterpolar(
                r=day_df['s_qty'], theta=day_df['Day of Week'], fill='toself', name='Sell Bias', line_color='#EF553B'
            ))
            fig_radar.update_layout(
                polar=dict(radialaxis=dict(visible=True, showticklabels=False)),
                showlegend=True,
                title="Aggression Radar"
            )
            st.plotly_chart(fig_radar, use_container_width=True)
