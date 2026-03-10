import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import os
import json

# --- SECURE DATABASE SETUP ---
try:
    MONGO_URI = st.secrets["MONGO_URI"]
except Exception:
    MONGO_URI = os.getenv("MONGO_URI", "")

COLLECTION_NAME = "Stock_Price_Volume"

@st.cache_resource
def get_db():
    if not MONGO_URI:
        st.error("⚠️ MONGO_URI not found! Please set it in `.streamlit/secrets.toml` or as an environment variable.")
        return None
    try:
        client = MongoClient(MONGO_URI)
        return client["StockHoldingByTMS"]
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return None

db = get_db()

def run():
    st.title("📈 Stock Price & Volume Matrix")
    
    # Create the two sub-tabs
    tab_graph, tab_injector = st.tabs(["📉 Price & Volume Graph", "💉 Data Injector"])

    # ==========================================
    # SUB-TAB 1: GRAPH VISUALIZATION
    # ==========================================
    with tab_graph:
        st.subheader("Interactive Price & Volume Analysis")
        
        if db is not None:
            records = list(db[COLLECTION_NAME].find({}, {"_id": 0}))
            
            if records:
                df = pd.DataFrame(records)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date')
                
                available_stocks = df['Stock'].unique()
                selected_stock = st.selectbox("Select a Stock to Analyze:", available_stocks)
                
                stock_df = df[df['Stock'] == selected_stock]
                
                fig = go.Figure()

                # Add Volume
                fig.add_trace(
                    go.Bar(
                        x=stock_df['Date'],
                        y=stock_df['Volume'],
                        name="Volume",
                        marker_color='rgba(158, 202, 225, 0.6)',
                        yaxis='y2'
                    )
                )

                # Add Price
                fig.add_trace(
                    go.Scatter(
                        x=stock_df['Date'],
                        y=stock_df['Close'],
                        name="Closing Price",
                        mode='lines+markers',
                        line=dict(color='orange', width=2),
                        marker=dict(size=6)
                    )
                )

                fig.update_layout(
                    title=f"{selected_stock} - Price vs Volume Over Time",
                    xaxis=dict(title="Date"),
                    yaxis=dict(title="Closing Price (Rs)", side="left", showgrid=False),
                    yaxis2=dict(title="Volume", side="right", overlaying="y", showgrid=False),
                    legend=dict(x=0.01, y=0.99),
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Raw Data"):
                    st.dataframe(stock_df.sort_values("Date", ascending=False).reset_index(drop=True))
            else:
                st.info("No data found in the database. Use the 'Data Injector' tab to add some.")

    # ==========================================
    # SUB-TAB 2: DATA INJECTOR (JSON FILE UPLOAD)
    # ==========================================
    with tab_injector:
        st.subheader("Inject TradingView Chart Data")
        st.markdown("""
        Upload your raw **JSON/TXT file** (like `temp_data - Copy.txt`) containing the TradingView data.
        *Uploading a file bypasses browser limits and processes instantly.*
        """)

        # Inputs
        stock_name = st.text_input("Enter Stock Symbol (e.g., NHPC):", "").strip().upper()
        
        # USE A FILE UPLOADER INSTEAD OF A TEXT AREA
        uploaded_file = st.file_uploader("Upload your data file (TXT or JSON)", type=["txt", "json"])

        # The Button Action
        if st.button("Convert & Inject Data", type="primary"):
            
            # 1. Validate inputs immediately
            if not stock_name:
                st.error("⚠️ Please enter the Stock Symbol first!")
                st.stop()
            if uploaded_file is None:
                st.error("⚠️ Please upload a file first!")
                st.stop()

            # 2. Process data
            with st.spinner(f"Reading file and processing data for {stock_name}..."):
                try:
                    import json
                    from pymongo import UpdateOne
                    
                    # Read the file directly into memory
                    file_content = uploaded_file.read().decode("utf-8")
                    data = json.loads(file_content)
                    
                    if "t" not in data or "c" not in data or "v" not in data:
                        st.error("❌ Invalid JSON format! Missing 't', 'c', or 'v' arrays.")
                        st.stop()
                    
                    # Create DataFrame (converting timestamps directly)
                    dates = [pd.to_datetime(ts, unit='s').strftime('%Y-%m-%d') for ts in data["t"]]
                    
                    df_new = pd.DataFrame({
                        "Date": dates,
                        "Stock": stock_name,
                        "Open": data.get("o", [0] * len(dates)),
                        "High": data.get("h", [0] * len(dates)),
                        "Low": data.get("l", [0] * len(dates)),
                        "Close": data["c"],
                        "Volume": data["v"]
                    })
                    
                    records_to_insert = df_new.to_dict('records')
                    
                    if db is not None and records_to_insert:
                        # Batch operations
                        operations = [
                            UpdateOne(
                                {"Date": r["Date"], "Stock": r["Stock"]},
                                {"$set": r},
                                upsert=True
                            ) for r in records_to_insert
                        ]
                        
                        result = db[COLLECTION_NAME].bulk_write(operations)
                        
                        st.success(f"✅ Successfully processed {len(records_to_insert)} days of market data for {stock_name}!")
                        st.info(f"Inserted: {result.upserted_count} | Updated: {result.modified_count}")
                        
                        # Show a preview
                        st.dataframe(df_new.head(10))
                        
                except json.JSONDecodeError:
                    st.error("❌ Failed to parse file. Make sure it contains clean JSON data.")
                except Exception as e:
                    st.error(f"❌ An error occurred: {e}")
