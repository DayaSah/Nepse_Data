import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from pymongo import MongoClient
import google.generativeai as genai

# --- 1. MONGODB CONNECTION ---
@st.cache_resource
def init_connection():
    try:
        # Pulls URI from your Streamlit secrets
        return MongoClient(st.secrets["mongo"]["uri"])
    except Exception as e:
        st.error(f"🔴 Database Connection Failed: {e}")
        return None

client = init_connection()
if client is not None:
    db = client["StockHoldingByTMS"]
else:
    st.stop()

# --- 2. GEMINI AI CONFIGURATION ---
try:
    genai.configure(api_key=st.secrets["gemini"]["api_key"])
    model = genai.GenerativeModel('gemini-2.0-flash')
except Exception as e:
    st.warning("⚠️ Gemini API not configured. AI Advisor will be disabled.")
    model = None

# --- 3. DATA FETCHING LOGIC ---
@st.cache_data(ttl=600) # Caches the data for 10 minutes to speed up the app
def fetch_and_clean_data(collection_name):
    """Fetches data from Mongo and calculates VWAP and Net Qty."""
    cursor = db[collection_name].find({}, {"_id": 0})
    df = pd.DataFrame(list(cursor))
    
    if df.empty:
        return df
        
    # Standardize columns
    df["Date"] = pd.to_datetime(df["date"], errors='coerce')
    df["Buy_Qty"] = pd.to_numeric(df.get("b_qty", 0))
    df["Sell_Qty"] = pd.to_numeric(df.get("s_qty", 0))
    df["Buy_Amount"] = pd.to_numeric(df.get("b_amt", 0))
    df["Sell_Amount"] = pd.to_numeric(df.get("s_amt", 0))
    
    # Advanced Calculations
    df["Net_Qty"] = df["Buy_Qty"] - df["Sell_Qty"]
    df["Total_Qty"] = df["Buy_Qty"] + df["Sell_Qty"]
    df["Total_Amount"] = df["Buy_Amount"] + df["Sell_Amount"]
    
    # Calculate VWAP (Volume Weighted Average Price) safely to avoid Division by Zero
    df["Daily_VWAP"] = np.where(df["Total_Qty"] > 0, df["Total_Amount"] / df["Total_Qty"], 0)
    
    return df.sort_values(by="Date").reset_index(drop=True)

# ==========================================
# MAIN UI: NEPSE PRO TERMINAL
# ==========================================
st.title("📈 NEPSE Pro Terminal (Quantum Edition)")
st.markdown("Deep dive into a specific Asset-Broker relationship using MongoDB + AI.")

# Dynamically list all collections in MongoDB
collections = db.list_collection_names()
valid_collections = sorted([c for c in collections if "_" in c])

if not valid_collections:
    st.info("No stock data found in MongoDB. Run the Auto-Fetcher first!")
    st.stop()

# Collection Selector
selected_col = st.selectbox("Select Target (Format: STOCK_BROKER):", valid_collections)

if selected_col:
    raw_df = fetch_and_clean_data(selected_col)
    
    if raw_df.empty:
        st.warning(f"No data found for {selected_col}.")
    else:
        # Global Date Filter for all tabs
        min_date, max_date = raw_df["Date"].min().date(), raw_df["Date"].max().date()
        date_range = st.date_input("🗓️ Select Date Range:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        
        if len(date_range) == 2:
            mask = (raw_df["Date"].dt.date >= date_range[0]) & (raw_df["Date"].dt.date <= date_range[1])
            df = raw_df.loc[mask]
        else:
            df = raw_df

        # --- SETUP 4 TABS ---
        tab1, tab2, tab3, tab4 = st.tabs(["📂 Raw Database", "🚀 Advanced Heatmap", "📊 Visual Optics", "🤖 AI Advisor"])
        
        # ------------------------------------------
        # TAB 1: RAW DATA
        # ------------------------------------------
        with tab1:
            st.subheader(f"Raw Data Ledger: {selected_col}")
            st.dataframe(df[["Date", "Buy_Qty", "Sell_Qty", "Net_Qty", "Daily_VWAP"]].sort_values("Date", ascending=False), use_container_width=True)

        # ------------------------------------------
        # TAB 2: ADVANCED HEATMAP
        # ------------------------------------------
        with tab2:
            st.subheader("🗓️ Behavioral Heatmap")
            st.markdown("Identify which days of the week this broker systematically buys or dumps.")
            
            df_heat = df.copy()
            df_heat['Day'] = df_heat['Date'].dt.day_name()
            df_heat['Month'] = df_heat['Date'].dt.strftime('%b %Y')
            
            # Filter strictly for NEPSE trading days
            nepse_days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday"]
            df_heat = df_heat[df_heat['Day'].isin(nepse_days)]
            df_heat['Day'] = pd.Categorical(df_heat['Day'], categories=nepse_days, ordered=True)
            
            try:
                heat_pivot = df_heat.groupby(['Month', 'Day'], observed=False)['Net_Qty'].sum().unstack().fillna(0)
                fig_heat = px.imshow(
                    heat_pivot, 
                    color_continuous_scale="RdYlGn", 
                    color_continuous_midpoint=0, 
                    text_auto=".0f", 
                    aspect="auto",
                    title=f"Net Quantity Heatmap ({selected_col})"
                )
                fig_heat.update_layout(height=450, margin=dict(t=40, b=20))
                st.plotly_chart(fig_heat, use_container_width=True)
            except Exception as e:
                st.error(f"Not enough data to generate Heatmap: {e}")

        # ------------------------------------------
        # TAB 3: VISUAL OPTICS
        # ------------------------------------------
        with tab3:
            st.subheader("🎨 Accumulation & Price Optics")
            colA, colB = st.columns(2)
            
            with colA:
                st.markdown("**Months with Heaviest Buying**")
                df_month = df.copy()
                df_month["Month"] = df_month["Date"].dt.strftime('%b %Y')
                buy_months = df_month[df_month["Net_Qty"] > 0].groupby("Month")["Net_Qty"].sum().reset_index()
                if not buy_months.empty:
                    fig_pie = px.pie(buy_months, values="Net_Qty", names="Month", hole=0.4)
                    fig_pie.update_layout(height=350, margin=dict(t=30, b=0))
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.info("No net buying months found.")

            with colB:
                st.markdown("**Months with Heaviest Dumping**")
                sell_months = df_month[df_month["Net_Qty"] < 0].copy()
                sell_months["Net_Qty"] = sell_months["Net_Qty"].abs()
                sell_group = sell_months.groupby("Month")["Net_Qty"].sum().reset_index()
                if not sell_group.empty:
                    fig_pie2 = px.pie(sell_group, values="Net_Qty", names="Month", hole=0.4)
                    fig_pie2.update_layout(height=350, margin=dict(t=30, b=0))
                    st.plotly_chart(fig_pie2, use_container_width=True)
                else:
                    st.info("No net selling months found.")

            st.write("---")
            st.markdown("### 📈 Price vs Net Quantity (Smart Money Tracker)")
            st.caption("Are they buying when the price is low (Smart Money) or high (FOMO)?")
            
            # Scatter Plot with OLS Trendline
            fig_scatter = px.scatter(
                df, x="Daily_VWAP", y="Net_Qty", 
                color="Net_Qty", color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                hover_data=["Date"], trendline="ols"
            )
            fig_scatter.add_hline(y=0, line_width=2, line_dash="dash", line_color="black")
            fig_scatter.update_layout(height=500, margin=dict(t=30, b=0))
            st.plotly_chart(fig_scatter, use_container_width=True)

        # ------------------------------------------
        # TAB 4: AI ADVISOR (GEMINI)
        # ------------------------------------------
        with tab4:
            st.subheader("🤖 Wall Street AI Analyst")
            if model is None:
                st.error("Gemini API not configured. Check secrets.toml.")
            else:
                total_days = len(df)
                net_inventory = df["Net_Qty"].sum()
                
                # Calculate True WACC safely
                total_buy_amt = df["Buy_Amount"].sum()
                total_buy_qty = df["Buy_Qty"].sum()
                wacc = (total_buy_amt / total_buy_qty) if total_buy_qty > 0 else 0
                
                # 5-Day Momentum
                recent_5_days = df.sort_values(by="Date", ascending=False).head(5)
                recent_trend = recent_5_days[["Date", "Net_Qty", "Daily_VWAP"]].to_string(index=False)
                
                # Dynamic User Prompt
                user_question = st.text_input("Ask the AI a specific question about this broker's strategy:", 
                                              placeholder="E.g., Are they accumulating or just trading ranges?")
                
                if st.button("Generate Quant Analysis", type="primary"):
                    with st.spinner("🧠 Gemini is analyzing the order flow..."):
                        prompt = f"""
                        You are a highly advanced Quantitative Analyst for the Nepal Stock Exchange (NEPSE).
                        I am providing you with the trading summary of {selected_col}.
                        
                        DATA SUMMARY:
                        - Trading Days Logged: {total_days}
                        - Current Holding Inventory (Net Qty): {net_inventory}
                        - Estimated Weighted Average Cost (WACC): Rs {wacc:.2f}
                        
                        RECENT 5-DAY MOMENTUM:
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
                            st.markdown("### 🤖 Analyst Report")
                            st.write(response.text)
                        except Exception as e:
                            st.error(f"AI Generation Failed: {e}")
