import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import os

# --- SECURE DATABASE SETUP ---
# It will check Streamlit Secrets first, and fallback to OS Environment Variables
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
            # Fetch available stocks from the database
            records = list(db[COLLECTION_NAME].find({}, {"_id": 0}))
            
            if records:
                df = pd.DataFrame(records)
                # Ensure date is a datetime object and sort it
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date')
                
                available_stocks = df['Stock'].unique()
                selected_stock = st.selectbox("Select a Stock to Analyze:", available_stocks)
                
                # Filter data for the selected stock
                stock_df = df[df['Stock'] == selected_stock]
                
                # Create a dual-axis chart (Price as Line, Volume as Bar)
                fig = go.Figure()

                # Add Volume (Bar Chart) - Assigned to secondary Y-axis
                fig.add_trace(
                    go.Bar(
                        x=stock_df['Date'],
                        y=stock_df['Volume'],
                        name="Volume",
                        marker_color='rgba(158, 202, 225, 0.6)',
                        yaxis='y2'
                    )
                )

                # Add Price (Line Chart)
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

                # Layout for dual-axis
                fig.update_layout(
                    title=f"{selected_stock} - Price vs Volume Over Time",
                    xaxis=dict(title="Date"),
                    yaxis=dict(
                        title="Closing Price (Rs)",
                        side="left",
                        showgrid=False
                    ),
                    yaxis2=dict(
                        title="Volume",
                        side="right",
                        overlaying="y",
                        showgrid=False
                    ),
                    legend=dict(x=0.01, y=0.99),
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Show raw data below the graph
                with st.expander("View Raw Data"):
                    st.dataframe(stock_df.sort_values("Date", ascending=False).reset_index(drop=True))
            else:
                st.info("No data found in the database. Use the 'Data Injector' tab to add some.")

    # ==========================================
    # SUB-TAB 2: DATA INJECTOR (JSON & CSV)
    # ==========================================
    with tab_injector:
        st.subheader("Inject TradingView Chart Data")
        st.markdown("""
        Paste the raw **JSON** data from NepseAlpha/TradingView (containing `"t"`, `"c"`, `"v"` arrays). 
        The system will automatically convert the timestamps to dates!
        """)

        # Ask for the Stock Name before injecting
        stock_name = st.text_input("Enter Stock Symbol for this data (e.g., NHPC, ULHC):", "").strip().upper()
        
        pasted_data = st.text_area("Paste Raw JSON Data here:", height=250)

        if st.button("Convert & Inject Data"):
            if not stock_name:
                st.warning("⚠️ Please enter the Stock Symbol first!")
            elif not pasted_data.strip():
                st.warning("⚠️ Please paste the JSON data.")
            else:
                try:
                    import json
                    from pymongo import UpdateOne
                    
                    # 1. Parse the JSON text
                    data = json.loads(pasted_data)
                    
                    # 2. Extract the arrays
                    if "t" not in data or "c" not in data or "v" not in data:
                        st.error("❌ Invalid JSON format! The data must contain 't' (time), 'c' (close), and 'v' (volume) arrays.")
                    else:
                        # 3. Use Pandas to convert everything instantly
                        df_new = pd.DataFrame({
                            # Convert Unix timestamps to YYYY-MM-DD
                            "Date": pd.to_datetime(data["t"], unit='s').strftime('%Y-%m-%d'),
                            "Stock": stock_name,
                            "Open": data.get("o", [0] * len(data["t"])),
                            "High": data.get("h", [0] * len(data["t"])),
                            "Low": data.get("l", [0] * len(data["t"])),
                            "Close": data["c"],
                            "Volume": data["v"]
                        })
                        
                        # 4. Prepare for MongoDB
                        records_to_insert = df_new.to_dict('records')
                        
                        if db is not None and records_to_insert:
                            # Use bulk upsert to prevent duplicating data if you paste the same thing twice
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
                            
                            # Show a preview of the clean data it just injected
                            st.dataframe(df_new.head(10))
                            
                except json.JSONDecodeError:
                    st.error("❌ Failed to parse JSON. Please make sure you copied the entire `{ ... }` block properly.")
                except Exception as e:
                    st.error(f"❌ An error occurred: {e}")
