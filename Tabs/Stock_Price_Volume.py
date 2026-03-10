import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pymongo import MongoClient
import os
import json

# --- ULTRA-RESILIENT DATABASE SETUP ---
def get_mongo_uri():
    try:
        # Check if it is under the [mongo] section in secrets.toml
        if "mongo" in st.secrets and "uri" in st.secrets["mongo"]:
            return st.secrets["mongo"]["uri"]
        # Check if it is just a plain MONGO_URI in secrets.toml
        elif "MONGO_URI" in st.secrets:
            return st.secrets["MONGO_URI"]
    except Exception:
        pass
    
    # Fallback to system environment variable
    return os.getenv("MONGO_URI", "")

MONGO_URI = get_mongo_uri()
COLLECTION_NAME = "Stock_Price_Volume"

@st.cache_resource
def get_db():
    if not MONGO_URI:
        st.error("⚠️ MONGO_URI is empty! Streamlit cannot find your password.")
        return None
    try:
        client = MongoClient(MONGO_URI)
        # Test the connection to force an error if it fails
        client.admin.command('ping')
        return client["StockHoldingByTMS"]
    except Exception as e:
        st.error(f"❌ Connection Error Details: {e}")
        return None

db = get_db()

def run():
    st.title("📈 Stock Price & Volume Matrix")
    
    # Create the two sub-tabs
    tab_graph, tab_injector = st.tabs(["📉 Price & Volume Graph", "💉 Data Injector"])

    # ==========================================
    # SUB-TAB 1: GRAPH VISUALIZATION (TRADINGVIEW STYLE)
    # ==========================================
    with tab_graph:
        st.subheader("Interactive Candlestick & Volume Analysis")
        
        if db is not None:
            records = list(db[COLLECTION_NAME].find({}, {"_id": 0}))
            
            if records:
                df = pd.DataFrame(records)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date')
                
                available_stocks = df['Stock'].unique()
                selected_stock = st.selectbox("Select a Stock to Analyze:", available_stocks)
                
                stock_df = df[df['Stock'] == selected_stock]
                
                # Create subplots: 2 rows (Top for Price, Bottom for Volume)
                fig = make_subplots(
                    rows=2, cols=1, 
                    shared_xaxes=True, 
                    vertical_spacing=0.03, 
                    row_heights=[0.8, 0.2], # 80% price, 20% volume
                )

                # 1. Add Candlestick (TradingView Colors)
                fig.add_trace(
                    go.Candlestick(
                        x=stock_df['Date'],
                        open=stock_df['Open'],
                        high=stock_df['High'],
                        low=stock_df['Low'],
                        close=stock_df['Close'],
                        name="Price",
                        increasing_line_color='#26a69a', # TradingView Green
                        decreasing_line_color='#ef5350'  # TradingView Red
                    ),
                    row=1, col=1
                )

                # Determine colors for volume bars (Green if Close >= Open, else Red)
                volume_colors = ['#26a69a' if row['Close'] >= row['Open'] else '#ef5350' for index, row in stock_df.iterrows()]

                # 2. Add Volume Bar Chart
                fig.add_trace(
                    go.Bar(
                        x=stock_df['Date'],
                        y=stock_df['Volume'],
                        name="Volume",
                        marker_color=volume_colors
                    ),
                    row=2, col=1
                )

                # Layout formatting for clean TradingView look
                fig.update_layout(
                    title=f"{selected_stock} - Advanced Chart",
                    yaxis_title="Price (Rs)",
                    yaxis2_title="Volume",
                    xaxis_rangeslider_visible=False, # Hides the bulky default slider for pure mouse zooming
                    height=650,
                    margin=dict(l=20, r=20, t=50, b=20),
                    showlegend=False,
                    dragmode="zoom" # Default mouse behavior
                )
                
                # Hide NEPSE weekends (Friday & Saturday) from the chart to prevent empty gaps
                fig.update_xaxes(
                    rangebreaks=[
                        dict(bounds=["fri", "sun"]) 
                    ]
                )
                
                # Render the chart
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
        """)

        # 🔒 Wrap everything in an st.form to prevent Streamlit from dropping the button click
        with st.form("data_injector_form"):
            stock_name = st.text_input("Enter Stock Symbol (e.g., NHPC):", "").strip().upper()
            
            uploaded_file = st.file_uploader("Upload your data file (TXT or JSON)", type=["txt", "json"])

            # The Submit Button MUST be inside the form
            submit_button = st.form_submit_button("Convert & Inject Data", type="primary")

        # Process exactly when the form is submitted
        if submit_button:
            if not stock_name:
                st.error("⚠️ Please enter the Stock Symbol first!")
            elif uploaded_file is None:
                st.error("⚠️ Please upload a file first!")
            elif db is None:
                st.error("❌ Database connection failed. Please check your MONGO_URI.")
            else:
                with st.spinner(f"Reading file and processing data for {stock_name}..."):
                    try:
                        import json
                        from pymongo import UpdateOne
                        
                        # Read the file directly into memory
                        file_content = uploaded_file.read().decode("utf-8")
                        data = json.loads(file_content)
                        
                        if "t" not in data or "c" not in data or "v" not in data:
                            st.error("❌ Invalid JSON format! Missing 't', 'c', or 'v' arrays.")
                        else:
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
                            
                            if records_to_insert:
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
                            else:
                                st.warning("No data found to insert.")
                                
                    except json.JSONDecodeError:
                        st.error("❌ Failed to parse file. Make sure it contains clean JSON data.")
                    except Exception as e:
                        st.error(f"❌ An error occurred: {e}")
