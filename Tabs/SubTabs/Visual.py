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
    st.header("👁️ Omniscient Visual Matrix")
    st.markdown("A multi-dimensional view of broker behavior. Hover over charts for exact telemetry.")

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
        selected_stock = st.selectbox("Select Target Stock", stocks_available, key="vis_stock")
    with col2:
        tms_available = sorted(stock_tms_map.get(selected_stock, []))
        selected_tms = st.selectbox("Select TMS Node", tms_available, key="vis_tms")
    with col3:
        time_horizon = st.selectbox(
            "Temporal Window", 
            ["All Time", "Last 7 Days", "Last 15 Days", "Last 30 Days", "Last 3 Months", "Custom Range"],
            key="vis_time"
        )

    custom_dates = []
    if time_horizon == "Custom Range":
        custom_dates = st.date_input("Select Custom Dates", [])

    # 3. Execution & Processing
    if st.button("🌌 Render Holographics"):
        collection_name = f"{selected_stock}_{selected_tms}"
        data = list(db[collection_name].find().sort("date", 1))
        
        if not data:
            st.warning("Data anomaly: Node is empty.")
            return

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        for col in ['b_qty', 's_qty', 'b_amt', 's_amt']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 4. STRICT TEMPORAL FILTERING
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
            st.warning("⚠️ No trace signatures found in this specific temporal window.")
            return

        # 5. Windowed Calculations
        df = df.sort_values('date', ascending=True)
        df['Net Qty'] = df['b_qty'] - df['s_qty']
        df['Cum Net Qty'] = df['Net Qty'].cumsum() 
        
        df['Buy Rate'] = (df['b_amt'] / df['b_qty']).replace([np.inf, -np.inf], np.nan).fillna(0)
        df['Sell Rate'] = (df['s_amt'] / df['s_qty']).replace([np.inf, -np.inf], np.nan).fillna(0)
        
        # Add Day of Week for behavioral analysis
        df['Day of Week'] = df['date'].dt.day_name()

        st.success(f"✅ Visualizing Multiversal Behavior for {selected_stock} @ TMS-{selected_tms}")

        # --- VISUALIZATION MATRIX ---
        
        # 🟢 CHART 1: The Accumulation Curve (Streamlit Native Area)
        st.markdown("### 🌊 Isolated Accumulation Curve (Net Inventory)")
        st.area_chart(df.set_index('date')['Cum Net Qty'], color="#00ff00" if df['Cum Net Qty'].iloc[-1] >= 0 else "#ff0000")

        # Create tabs for different analytical dimensions
        v_tab1, v_tab2, v_tab3 = st.tabs(["📊 Volume Dynamics", "💰 Price & Capital", "🧠 Behavioral Patterns"])

        with v_tab1:
            # 🟢 CHART 2: Interactive Dual Bar Chart (Plotly)
            st.markdown("#### Buy vs Sell Volume Delta")
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(x=df['date'], y=df['b_qty'], name='Bought', marker_color='#1f77b4'))
            fig_vol.add_trace(go.Bar(x=df['date'], y=-df['s_qty'], name='Sold (Negative)', marker_color='#d62728'))
            fig_vol.update_layout(barmode='relative', hovermode="x unified", title="Daily Volume Delta (Above 0 = Net Buy)")
            st.plotly_chart(fig_vol, use_container_width=True)

        with v_tab2:
            colA, colB = st.columns(2)
            with colA:
                # 🟢 CHART 3: Capital Deployment Donut
                st.markdown("#### Capital Distribution")
                total_b = df['b_amt'].sum()
                total_s = df['s_amt'].sum()
                fig_pie = px.pie(
                    names=['Capital Spent (Buy)', 'Capital Gained (Sell)'], 
                    values=[total_b, total_s],
                    hole=0.4,
                    color_discrete_sequence=['#2ca02c', '#ff7f0e']
                )
                st.plotly_chart(fig_pie, use_container_width=True)
                
            with colB:
                # 🟢 CHART 4: Price Execution Bubble Scatter
                st.markdown("#### Buy Price Intensity (Bubble = Volume)")
                # Filter out days with 0 buy rate
                buy_df = df[df['Buy Rate'] > 0]
                if not buy_df.empty:
                    fig_scatter = px.scatter(
                        buy_df, x='date', y='Buy Rate', size='b_qty', 
                        color='Buy Rate', color_continuous_scale='Viridis',
                        hover_data=['b_qty']
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)
                else:
                    st.info("No buy data to plot.")

            # 🟢 CHART 5: Price Density Histogram
            st.markdown("#### Execution Price Density (Where do they trade most?)")
            valid_rates = pd.concat([df[df['Buy Rate']>0]['Buy Rate'], df[df['Sell Rate']>0]['Sell Rate']])
            if not valid_rates.empty:
                fig_hist = px.histogram(
                    valid_rates, nbins=20, 
                    title="Frequency of Trades at Specific Price Levels",
                    color_discrete_sequence=['#9467bd']
                )
                fig_hist.update_layout(xaxis_title="Price (Rs.)", yaxis_title="Number of Days Traded at this Price")
                st.plotly_chart(fig_hist, use_container_width=True)

        with v_tab3:
            # 🟢 CHART 6: Behavioral Day-of-Week Radar
            st.markdown("#### Weekday Aggression Profile")
            st.markdown("*Which days of the week is this broker most active?*")
            day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
            day_df = df.groupby('Day of Week')[['b_qty', 's_qty']].sum().reindex(day_order).fillna(0).reset_index()
            
            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(
                r=day_df['b_qty'], theta=day_df['Day of Week'], fill='toself', name='Buy Volume', line_color='blue'
            ))
            fig_radar.add_trace(go.Scatterpolar(
                r=day_df['s_qty'], theta=day_df['Day of Week'], fill='toself', name='Sell Volume', line_color='red'
            ))
            fig_radar.update_layout(polar=dict(radialaxis=dict(visible=True, showticklabels=False)))
            st.plotly_chart(fig_radar, use_container_width=True)
