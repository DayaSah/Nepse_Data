import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import os

# --- DATABASE SETUP ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://sah255472_db_user:HelloWorld@dataanalysis.rqfvczw.mongodb.net/?appName=DataAnalysis")
COLLECTION_NAME = "Stock_Price_Volume"

@st.cache_resource
def get_db():
    try:
        client = MongoClient(MONGO_URI)
        return client["StockHoldingByTMS"]
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return None

db = get_db()

def show_stock_price_tab():
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
    # SUB-TAB 2: DATA INJECTOR
    # ==========================================
    with tab_injector:
        st.subheader("Inject Bulk Data via Copy-Paste")
        st.markdown("""
        **Format required:** Paste tab-separated or comma-separated data. 
        It must include these columns: `Date`, `Stock`, `Close`, `Volume`.
        *Example:*
        ```
        Date,Stock,Close,Volume
        2024-03-10,NHPC,245.50,150000
        2024-03-11,NHPC,248.00,180000
        ```
        """)

        pasted_data = st.text_area("Paste your data here:", height=250)

        if st.button("Inject Data into MongoDB"):
            if pasted_data.strip() == "":
                st.warning("Please paste some data first.")
            else:
                try:
                    from io import StringIO
                    
                    # Detect if it's tab separated (like from Excel) or comma separated
                    separator = '\t' if '\t' in pasted_data else ','
                    
                    # Read the pasted text into a pandas DataFrame
                    df_new = pd.read_csv(StringIO(pasted_data), sep=separator)
                    
                    # Clean up column names (strip whitespace)
                    df_new.columns = df_new.columns.str.strip()
                    
                    # Verify required columns exist
                    required_cols = ['Date', 'Stock', 'Close', 'Volume']
                    missing_cols = [col for col in required_cols if col not in df_new.columns]
                    
                    if missing_cols:
                        st.error(f"Missing required columns: {', '.join(missing_cols)}")
                    else:
                        # Clean the data before injecting
                        df_new['Date'] = pd.to_datetime(df_new['Date']).dt.strftime('%Y-%m-%d')
                        df_new['Stock'] = df_new['Stock'].astype(str).str.strip().str.upper()
                        df_new['Close'] = pd.to_numeric(df_new['Close'], errors='coerce')
                        df_new['Volume'] = pd.to_numeric(df_new['Volume'], errors='coerce')
                        
                        # Drop any rows where parsing failed
                        df_new = df_new.dropna(subset=['Date', 'Stock', 'Close', 'Volume'])
                        
                        records_to_insert = df_new.to_dict('records')
                        
                        if db is not None and records_to_insert:
                            # Use bulk upsert to avoid duplicates based on Date and Stock
                            from pymongo import UpdateOne
                            operations = [
                                UpdateOne(
                                    {"Date": r["Date"], "Stock": r["Stock"]},
                                    {"$set": r},
                                    upsert=True
                                ) for r in records_to_insert
                            ]
                            
                            result = db[COLLECTION_NAME].bulk_write(operations)
                            
                            st.success(f"✅ Successfully processed {len(records_to_insert)} records!")
                            st.info(f"Inserted: {result.upserted_count} | Updated: {result.modified_count}")
                            
                            # Give a little preview of what was saved
                            st.dataframe(df_new.head())
                            
                except Exception as e:
                    st.error(f"Error parsing data. Please ensure it matches the correct format. Details: {e}")
