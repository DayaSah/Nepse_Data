import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pymongo import MongoClient
import google.generativeai as genai

# --- 1. MONGODB CONNECTION ---
@st.cache_resource
def init_connection():
    try:
        return MongoClient(st.secrets["mongo"]["uri"])
    except Exception as e:
        st.error(f"🔴 Database Connection Failed: {e}")
        return None

client = init_connection()
if client is not None:
    db = client["StockHoldingByTMS"]
else:
    db = None

# --- 2. GEMINI AI CONFIGURATION ---
try:
    genai.configure(api_key=st.secrets["gemini"]["api_key"])
    model = genai.GenerativeModel('gemini-2.0-flash')
except Exception as e:
    model = None

# --- 3. DATA FETCHING & PREPARATION ---
@st.cache_data(ttl=600)
def fetch_and_clean_data(collection_name):
    if db is None:
        return pd.DataFrame()
        
    cursor = db[collection_name].find({}, {"_id": 0})
    df = pd.DataFrame(list(cursor))
    
    if df.empty:
        return df
        
    df["Date"] = pd.to_datetime(df["date"], errors='coerce')
    df["Buy_Qty"] = pd.to_numeric(df.get("b_qty", 0))
    df["Sell_Qty"] = pd.to_numeric(df.get("s_qty", 0))
    df["Buy_Amount"] = pd.to_numeric(df.get("b_amt", 0))
    df["Sell_Amount"] = pd.to_numeric(df.get("s_amt", 0))
    
    df["Net_Qty"] = df["Buy_Qty"] - df["Sell_Qty"]
    df["Net_Amount"] = df["Buy_Amount"] - df["Sell_Amount"]
    df["Total_Vol"] = df["Buy_Qty"] + df["Sell_Qty"]
    df["Daily_VWAP"] = np.where(df["Total_Vol"] > 0, (df["Buy_Amount"] + df["Sell_Amount"]) / df["Total_Vol"], 0)
    
    df = df.sort_values(by="Date").reset_index(drop=True)
    df["Cum_Net_Qty"] = df["Net_Qty"].cumsum()
    df["Avg_30D_Vol"] = df["Total_Vol"].rolling(window=30, min_periods=1).mean()
    
    return df


@st.cache_data(ttl=600)
def fetch_broker_race_data(stock_symbol, valid_collections):
    """Fetches individual cumulative inventory for every single broker for a specific stock."""
    if db is None: return pd.DataFrame()
    target_cols = [c for c in valid_collections if c.split("_")[0] == stock_symbol]
    
    all_dfs = []
    for col in target_cols:
        broker_id = col.split("_")[1]
        cursor = db[col].find({}, {"_id": 0, "date": 1, "b_qty": 1, "s_qty": 1})
        df = pd.DataFrame(list(cursor))
        if not df.empty:
            df["Date"] = pd.to_datetime(df["date"], errors='coerce')
            df["Net_Qty"] = pd.to_numeric(df.get("b_qty", 0)) - pd.to_numeric(df.get("s_qty", 0))
            df = df.sort_values("Date").reset_index(drop=True)
            df["Cum_Net_Qty"] = df["Net_Qty"].cumsum()
            df["Broker"] = f"Broker {broker_id}"
            all_dfs.append(df[["Date", "Broker", "Cum_Net_Qty", "Net_Qty"]])
            
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


@st.cache_data(ttl=600)
def fetch_collective_data(stock_symbol, valid_collections):
    """Aggregates data for a stock across ALL brokers in the database."""
    if db is None: return pd.DataFrame()
    target_cols = [c for c in valid_collections if c.split("_")[0] == stock_symbol]
    
    all_dfs = []
    for col in target_cols:
        cursor = db[col].find({}, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if not df.empty:
            all_dfs.append(df)
            
    if not all_dfs: return pd.DataFrame()
        
    combined = pd.concat(all_dfs, ignore_index=True)
    combined["Date"] = pd.to_datetime(combined["date"], errors='coerce')
    combined["Buy_Qty"] = pd.to_numeric(combined.get("b_qty", 0))
    combined["Sell_Qty"] = pd.to_numeric(combined.get("s_qty", 0))
    combined["Buy_Amount"] = pd.to_numeric(combined.get("b_amt", 0))
    combined["Sell_Amount"] = pd.to_numeric(combined.get("s_amt", 0))
    
    agg_df = combined.groupby("Date", as_index=False).agg({
        "Buy_Qty": "sum", "Sell_Qty": "sum", "Buy_Amount": "sum", "Sell_Amount": "sum"
    }).sort_values(by="Date").reset_index(drop=True)
    
    agg_df["Net_Qty"] = agg_df["Buy_Qty"] - agg_df["Sell_Qty"]
    agg_df["Total_Vol"] = agg_df["Buy_Qty"] + agg_df["Sell_Qty"]
    agg_df["Daily_VWAP"] = np.where(agg_df["Total_Vol"] > 0, (agg_df["Buy_Amount"] + agg_df["Sell_Amount"]) / agg_df["Total_Vol"], 0)
    agg_df["Cum_Net_Qty"] = agg_df["Net_Qty"].cumsum()
    
    return agg_df

# ==========================================
# 🚀 MAIN APP EXECUTOR
# ==========================================
def run():
    st.title("📈 NEPSE Pro Terminal (Quantum Edition)")
    st.markdown("Deep dive into a specific Asset-Broker relationship using MongoDB + AI.")
    
    if db is None:
        st.error("Cannot connect to MongoDB. Check secrets and network.")
        return

    collections = db.list_collection_names()
    valid_collections = sorted([c for c in collections if "_" in c])

    if not valid_collections:
        st.info("No stock data found in MongoDB. Run the Auto-Fetcher first!")
        return

    selected_col = st.selectbox("Select Target (Format: STOCK_BROKER):", valid_collections)

    if selected_col:
        stock_symbol = selected_col.split("_")[0]
        raw_df = fetch_and_clean_data(selected_col)
        
        if raw_df.empty:
            st.warning(f"No data found for {selected_col}.")
            return
            
        # Global Date Filter
        min_date, max_date = raw_df["Date"].min().date(), raw_df["Date"].max().date()
        date_range = st.date_input("🗓️ Select Date Range:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        
        if len(date_range) == 2:
            mask = (raw_df["Date"].dt.date >= date_range[0]) & (raw_df["Date"].dt.date <= date_range[1])
            df = raw_df.loc[mask].copy().reset_index(drop=True)
            df["Cum_Net_Qty"] = df["Net_Qty"].cumsum()
        else:
            df = raw_df.copy()

        # --- SETUP 6 MEGA TABS ---
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "📂 Ledger & Matrix", 
            "🚀 Advanced Metrics", 
            "📊 Optics & Trends", 
            "🐳 Volume & Whales", 
            "🤖 AI Advisor",
            "🌐 Collective TMS (Market Macro)",
            "📦 Broker Inventory Race"
        ])
        
        # ------------------------------------------
        # TAB 1: RAW DATA (Color Coded Aggression)
        # ------------------------------------------
        with tab1:
            st.subheader(f"🧮 Data Matrix: {selected_col}")
            st.markdown("Color-coded by Aggression (Dark Green = Heavy Accumulation, Dark Red = Heavy Dumping)")
            
            def apply_color_strength(row):
                net, avg_vol = row["Net_Qty"], row["Avg_30D_Vol"]
                if avg_vol == 0 or pd.isna(avg_vol): return [''] * len(row)
                alpha = min(max((abs(net) / avg_vol) * 0.5, 0.15), 0.85)
                color = f"rgba(0, 200, 0, {alpha})" if net > 0 else f"rgba(255, 0, 0, {alpha})" if net < 0 else "rgba(128, 128, 128, 0.2)"
                return [f"background-color: {color}; color: white;"] * len(row)

            display_df = df.copy()
            display_df["Date"] = display_df["Date"].dt.strftime('%Y-%m-%d')
            fmt_df = display_df[["Date", "Buy_Qty", "Sell_Qty", "Net_Qty", "Buy_Amount", "Sell_Amount", "Net_Amount", "Daily_VWAP", "Avg_30D_Vol"]].copy()
            
            styled_df = fmt_df.style.apply(apply_color_strength, axis=1).format({
                "Buy_Qty": "{:,.0f}", "Sell_Qty": "{:,.0f}", "Net_Qty": "{:,.0f}",
                "Buy_Amount": "{:,.0f}", "Sell_Amount": "{:,.0f}", "Net_Amount": "{:,.0f}",
                "Daily_VWAP": "{:.2f}", "Avg_30D_Vol": "{:,.0f}"
            })
            
            st.dataframe(styled_df, use_container_width=True, height=500)

        # ------------------------------------------
        # TAB 2: ADVANCED QUANTITATIVE ANALYSIS
        # ------------------------------------------
        with tab2:
            st.markdown("### 💰 Profitability & WACC Metrics")
            total_buy_qty = df["Buy_Qty"].sum()
            total_buy_amt = df["Buy_Amount"].sum()
            total_sell_qty = df["Sell_Qty"].sum()
            total_sell_amt = df["Sell_Amount"].sum()
            current_inventory = df["Cum_Net_Qty"].iloc[-1] if not df.empty else 0
            
            buy_wacc = (total_buy_amt / total_buy_qty) if total_buy_qty > 0 else 0
            sell_wacc = (total_sell_amt / total_sell_qty) if total_sell_qty > 0 else 0
            realized_pl = total_sell_qty * (sell_wacc - buy_wacc)
            
            net_capital_flow = total_buy_amt - total_sell_amt
            break_even = (net_capital_flow / current_inventory) if current_inventory > 0 else 0

            m1, m2, m3 = st.columns(3)
            m1.metric("Average Buy WACC", f"Rs {buy_wacc:,.2f}")
            m2.metric("Average Sell WACC", f"Rs {sell_wacc:,.2f}")
            m3.metric("Inventory Left", f"{current_inventory:,.0f} Units")

            m4, m5, m6 = st.columns(3)
            m4.metric("Realized P/L (Cleared Trades)", f"Rs {realized_pl:,.2f}", delta="Profit" if realized_pl > 0 else "Loss")
            
            if current_inventory > 0:
                if break_even < 0:
                    m5.metric("Remaining Break-Even", "Risk Free!", delta="Fully Recovered Initial Capital", delta_color="normal")
                else:
                    m5.metric("Remaining Break-Even", f"Rs {break_even:,.2f}", delta="Target Price to Recover Money", delta_color="off")
            else:
                m5.metric("Remaining Break-Even", "N/A", delta="No inventory left")

            st.write("---")
            st.markdown("### 🗓️ Improved Day-of-Week Heatmap")
            with st.expander("📖 How to Read this Heatmap", expanded=False):
                st.info("""
                **What it tells you:**
                This chart shows exactly which days of the week the broker is most active, broken down by month.
                - 🟩 **Green Boxes:** Aggressive buying.
                - 🟥 **Red Boxes:** Aggressive dumping.
                - **Pro Tip:** Look for patterns. E.g., If they always show deep red on Thursdays, they might be systematically booking profits before the weekend.
                """)
            
            df_heat = df.copy()
            df_heat['Day'] = df_heat['Date'].dt.day_name()
            df_heat['Month'] = df_heat['Date'].dt.strftime('%b %Y')
            
            nepse_days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday"]
            df_heat = df_heat[df_heat['Day'].isin(nepse_days)]
            df_heat['Day'] = pd.Categorical(df_heat['Day'], categories=nepse_days, ordered=True)
            
            try:
                heat_pivot = df_heat.groupby(['Month', 'Day'], observed=False)['Net_Qty'].sum().unstack().fillna(0)
                fig_heat = px.imshow(heat_pivot, color_continuous_scale="RdYlGn", color_continuous_midpoint=0, text_auto=".0f", aspect="auto")
                fig_heat.update_layout(height=450, margin=dict(t=20, b=20))
                st.plotly_chart(fig_heat, use_container_width=True)
            except Exception:
                st.warning("Not enough variance to build heatmap.")

        # ------------------------------------------
        # TAB 3: VISUAL OPTICS & TRENDS
        # ------------------------------------------
        with tab3:
            st.markdown("### 1. Volume & Inventory Accumulation")
            fig1 = make_subplots(specs=[[{"secondary_y": True}]])
            fig1.add_trace(go.Bar(x=df["Date"], y=df["Buy_Qty"], name="Buy Qty", marker_color="rgba(39, 174, 96, 0.7)"), secondary_y=False)
            fig1.add_trace(go.Bar(x=df["Date"], y=-df["Sell_Qty"], name="Sell Qty", marker_color="rgba(231, 76, 60, 0.7)"), secondary_y=False)
            fig1.add_trace(go.Scatter(x=df["Date"], y=df["Cum_Net_Qty"], name="Cum. Inventory", line=dict(color="#2980b9", width=4)), secondary_y=True)
            fig1.update_layout(barmode='relative', height=500, hovermode="x unified", margin=dict(t=30))
            st.plotly_chart(fig1, use_container_width=True)

            st.write("---")
            c_gauge, c_pie = st.columns(2)
            with c_gauge:
                st.markdown("### 2. Market Sentiment Gauge")
                total_net = df["Net_Qty"].sum()
                max_vol = df["Total_Vol"].sum() if df["Total_Vol"].sum() > 0 else 1
                fig4 = go.Figure(go.Indicator(
                    mode="gauge+number+delta", value=total_net, title={'text': "Total Net Accumulation"},
                    gauge={'axis': {'range': [-max_vol, max_vol]}, 'bar': {'color': "rgba(0,0,0,0.5)"},
                           'steps': [{'range': [-max_vol, 0], 'color': "rgba(231, 76, 60, 0.4)"}, 
                                     {'range': [0, max_vol], 'color': "rgba(39, 174, 96, 0.4)"}]}
                ))
                fig4.update_layout(height=350, margin=dict(t=50, b=0))
                st.plotly_chart(fig4, use_container_width=True)

            with c_pie:
                st.markdown("### 3. Dynamic Breakdown (Pie)")
                pie_type = st.selectbox("Select Metric:", ["Total Buy vs Sell Qty", "Net Accumulation by Month"])
                if pie_type == "Total Buy vs Sell Qty":
                    fig5 = px.pie(values=[total_buy_qty, total_sell_qty], names=["Buy Volume", "Sell Volume"], hole=0.4, color_discrete_sequence=["#27ae60", "#e74c3c"])
                else:
                    df_month = df.copy()
                    df_month["Month"] = df_month["Date"].dt.strftime('%b %Y')
                    month_group = df_month[df_month["Net_Qty"] > 0].groupby("Month")["Net_Qty"].sum().reset_index()
                    fig5 = px.pie(month_group, values="Net_Qty", names="Month", hole=0.4)
                fig5.update_layout(height=350, margin=dict(t=30, b=0))
                st.plotly_chart(fig5, use_container_width=True)

            st.write("---")
            st.markdown("### 4. Trading Behavior (Price vs Net Quantity)")
            st.caption("Are they buying when the price is low (Smart Money) or high (Dumb Money/FOMO)?")
            try:
                fig6 = px.scatter(df, x="Daily_VWAP", y="Net_Qty", color="Net_Qty", color_continuous_scale="RdYlGn", color_continuous_midpoint=0, hover_data=["Date"], trendline="ols")
                fig6.add_hline(y=0, line_width=2, line_dash="dash", line_color="black")
                fig6.update_layout(height=500, xaxis_title="Daily Average Price (VWAP)", yaxis_title="Net Quantity (Buy - Sell)")
                st.plotly_chart(fig6, use_container_width=True)
            except Exception:
                st.warning("Trendline failed. Displaying simple scatter.")
                st.plotly_chart(px.scatter(df, x="Daily_VWAP", y="Net_Qty", color="Net_Qty", color_continuous_scale="RdYlGn", color_continuous_midpoint=0), use_container_width=True)

        # ------------------------------------------
        # TAB 4: WHALE ACTION & VOLUME PROFILE
        # ------------------------------------------
        with tab4:
            st.markdown("### 1. Volume by Price (Support & Resistance Zones)")
            vp_df = df[df["Daily_VWAP"] > 0].copy()
            if not vp_df.empty and vp_df["Daily_VWAP"].nunique() > 1:
                bins = np.linspace(vp_df["Daily_VWAP"].min(), vp_df["Daily_VWAP"].max(), 15)
                vp_df['Price_Zone'] = pd.cut(vp_df['Daily_VWAP'], bins=bins)
                profile = vp_df.groupby('Price_Zone', observed=False).agg({'Buy_Qty': 'sum', 'Sell_Qty': 'sum'}).reset_index()
                profile['Price_Level'] = profile['Price_Zone'].apply(lambda x: f"Rs {int(x.mid)}" if pd.notnull(x) else "Unknown")
                
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(y=profile['Price_Level'], x=profile['Buy_Qty'], name='Buy Vol', orientation='h', marker_color='rgba(39, 174, 96, 0.8)'))
                fig2.add_trace(go.Bar(y=profile['Price_Level'], x=-profile['Sell_Qty'], name='Sell Vol', orientation='h', marker_color='rgba(231, 76, 60, 0.8)'))
                fig2.update_layout(barmode='relative', yaxis=dict(autorange="reversed"), height=500, hovermode="y unified", margin=dict(t=30))
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("Not enough price variation for a Volume Profile.")

            st.write("---")
            st.markdown("### 2. Whale Action Bubble Chart")
            st.caption("X=Date | Y=Average Price | Size=Total Volume | Color=Net Accumulation vs Distribution")
            fig3 = px.scatter(df, x="Date", y="Daily_VWAP", size=df["Total_Vol"].abs(), color="Net_Qty", color_continuous_scale="RdYlGn", color_continuous_midpoint=0, hover_data=["Buy_Qty", "Sell_Qty"])
            fig3.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')), selector=dict(mode='markers'))
            fig3.update_layout(height=500, yaxis_title="Daily VWAP (Rs)", xaxis_title="Date")
            st.plotly_chart(fig3, use_container_width=True)

        # ------------------------------------------
        # TAB 5: AI ADVISOR (GEMINI)
        # ------------------------------------------
        with tab5:
            st.subheader("🤖 Wall Street AI Analyst")
            st.markdown("Your personal AI analyst powered by Google Gemini.")
            if model is None:
                st.error("Gemini API not configured. Check secrets.toml.")
            else:
                total_days = len(df)
                recent_trend = df.tail(5)[["Date", "Net_Qty"]].to_string(index=False)
                
                user_question = st.text_input("Ask the AI a specific question, or leave blank for a general report:", 
                                              placeholder="E.g., Are they accumulating or distributing? What is their WACC?")
                
                if st.button("🧠 Generate AI Analysis", type="primary"):
                    with st.spinner("🧠 Gemini is analyzing the order flow..."):
                        prompt = f"""
                        You are an elite quantitative analyst for the Nepal Stock Exchange (NEPSE). 
                        I am providing you with the trading summary of {selected_col}.
                        
                        DATA SUMMARY:
                        - Trading Days Logged: {total_days}
                        - Current Holding Inventory (Net Qty): {current_inventory}
                        - Estimated Weighted Average Cost (WACC): Rs {buy_wacc:.2f}
                        
                        RECENT 5-DAY MOMENTUM (Net Qty):
                        {recent_trend}
                        
                        Based on this data, provide a professional, highly analytical response. 
                        If the Net Inventory is highly positive, they are accumulating. If negative, they are dumping.
                        Look at the recent 5 days to see if their behavior changed recently.
                        Keep the response concise, formatted with bullet points, and act like a Wall Street advisor.
                        
                        User's specific question: {user_question if user_question else "Provide a general accumulation/distribution analysis and strategic advice."}
                        """
                        try:
                            response = model.generate_content(prompt)
                            st.write("---")
                            st.markdown("### 🤖 AI Analyst Report")
                            st.write(response.text)
                        except Exception as e:
                            st.error(f"AI Generation Failed: {e}")

        # ------------------------------------------
        # TAB 6: COLLECTIVE TMS (Whole Market Analysis)
        # ------------------------------------------
        with tab6:
            st.subheader(f"🌐 Macro Market View: {stock_symbol}")
            st.markdown(f"Aggregating all TMS broker data to show true market volume and macro support/resistance for **{stock_symbol}**.")
            
            agg_df = fetch_collective_data(stock_symbol, valid_collections)
            
            if agg_df.empty:
                st.warning("Could not aggregate market data for this stock.")
            else:
                # Filter aggregate data using the same global date range
                if len(date_range) == 2:
                    mask = (agg_df["Date"].dt.date >= date_range[0]) & (agg_df["Date"].dt.date <= date_range[1])
                    agg_df = agg_df.loc[mask].copy().reset_index(drop=True)
                
                colA, colB = st.columns(2)
                
                with colA:
                    st.markdown("### 📈 Total Market Volume Trend")
                    fig_agg1 = go.Figure()
                    fig_agg1.add_trace(go.Bar(x=agg_df["Date"], y=agg_df["Total_Vol"], name="Market Vol", marker_color="#8e44ad"))
                    fig_agg1.update_layout(height=400, hovermode="x unified", margin=dict(t=30, b=0))
                    st.plotly_chart(fig_agg1, use_container_width=True)

                with colB:
                    st.markdown("### 🌪️ Market Volatility Bubble")
                    st.caption("X=Date | Y=Market VWAP | Size=Total Market Volume")
                    fig_agg2 = px.scatter(agg_df, x="Date", y="Daily_VWAP", size=agg_df["Total_Vol"].abs(), color="Total_Vol", color_continuous_scale="Plasma")
                    fig_agg2.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')), selector=dict(mode='markers'))
                    fig_agg2.update_layout(height=400, margin=dict(t=30, b=0))
                    st.plotly_chart(fig_agg2, use_container_width=True)

                st.write("---")
                st.markdown("### 🧱 True Market Volume Profile (Macro Support/Resistance)")
                vp_agg = agg_df[agg_df["Daily_VWAP"] > 0].copy()
                if not vp_agg.empty and vp_agg["Daily_VWAP"].nunique() > 1:
                    bins = np.linspace(vp_agg["Daily_VWAP"].min(), vp_agg["Daily_VWAP"].max(), 20)
                    vp_agg['Price_Zone'] = pd.cut(vp_agg['Daily_VWAP'], bins=bins)
                    profile_agg = vp_agg.groupby('Price_Zone', observed=False)['Total_Vol'].sum().reset_index()
                    profile_agg['Price_Level'] = profile_agg['Price_Zone'].apply(lambda x: f"Rs {int(x.mid)}" if pd.notnull(x) else "Unknown")
                    
                    fig_agg3 = go.Figure()
                    fig_agg3.add_trace(go.Bar(y=profile_agg['Price_Level'], x=profile_agg['Total_Vol'], orientation='h', marker_color='#2c3e50'))
                    fig_agg3.update_layout(yaxis=dict(autorange="reversed"), height=500, hovermode="y unified", margin=dict(t=30, b=0))
                    st.plotly_chart(fig_agg3, use_container_width=True)
                else:
                    st.info("Not enough price variation to plot Macro Volume Profile.")

        with tab7:
                st.markdown("### 🏎️ The Great Broker Accumulation Race")
                st.markdown("Tracks the cumulative inventory of **every single broker** simultaneously.")
                st.info("💡 **Pro Tip:** Double-click any broker in the legend to isolate their line, or single-click to turn them off.")
                
                race_df = fetch_broker_race_data(selected_stock, valid_collections)
                
                if not race_df.empty:
                    if len(date_range) == 2:
                        mask = (race_df["Date"].dt.date >= date_range[0]) & (race_df["Date"].dt.date <= date_range[1])
                        race_df = race_df.loc[mask].copy()
                    
                    # Get the final standing of each broker to find the top accumulators/dumpers
                    final_standings = race_df.groupby("Broker")["Cum_Net_Qty"].last().sort_values(ascending=False)
                    
                    # Smart Filter Toggle
                    show_top_only = st.checkbox("🎯 Show Top 5 Accumulators & Top 5 Dumpers Only (Remove Noise)")
                    
                    if show_top_only:
                        top_brokers = list(final_standings.head(5).index) + list(final_standings.tail(5).index)
                        plot_df = race_df[race_df["Broker"].isin(top_brokers)]
                    else:
                        plot_df = race_df
                        
                    # Plot the Spaghetti Chart
                    fig_race = px.line(
                        plot_df, x="Date", y="Cum_Net_Qty", color="Broker", 
                        hover_data=["Net_Qty"],
                        color_discrete_sequence=px.colors.qualitative.Alphabet
                    )
                    fig_race.update_layout(height=650, hovermode="closest", yaxis_title="Cumulative Net Quantity")
                    # Make lines slightly transparent to handle the noise, unless hovered
                    fig_race.update_traces(line=dict(width=2), opacity=0.7)
                    st.plotly_chart(fig_race, use_container_width=True)
                    
                    # Mini Leaderboard below the chart
                    st.markdown("### 🏆 Current Holdings Leaderboard (End of Selected Period)")
                    ld1, ld2 = st.columns(2)
                    with ld1:
                        st.success("**Top 5 Accumulators (Holding the most)**")
                        st.dataframe(final_standings.head(5), use_container_width=True)
                    with ld2:
                        st.error("**Top 5 Dumpers (Sold the most short/cleared out)**")
                        st.dataframe(final_standings.tail(5), use_container_width=True)
