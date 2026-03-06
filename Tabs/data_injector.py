import streamlit as st
import json
import pandas as pd
from pymongo import MongoClient

# --- DATABASE CONNECTION ---
# We use st.cache_resource so it only connects to MongoDB once and doesn't crash the app
@st.cache_resource
def init_connection():
    # You will need to add your MongoDB URI to Streamlit's secrets later!
    # For local testing, you can temporarily replace st.secrets["mongo"]["uri"] 
    # with your actual string: "mongodb+srv://username:password@cluster..."
    try:
        # Default placeholder. We'll guide you on how to set this up properly.
        uri = st.secrets.get("mongo", {}).get("uri", "mongodb://localhost:27017") 
        client = MongoClient(uri)
        return client
    except Exception as e:
        st.error(f"Database Connection Failed: {e}")
        return None

def run():
    st.title("💉 Multiversal Data Injector")
    st.markdown("Inject raw API dumps or Floorsheet data directly into the Quantum Database (MongoDB).")
    
    # Establish connection
    client = init_connection()
    
    # Create the Sub-Tabs
    tab1, tab2 = st.tabs(["📊 Stock Holding By TMS", "⚙️ Other Injections"])
    
    with tab1:
        st.header("Upload TMS-Specific Stock Data")
        st.markdown("This will merge daily Buy/Sell records into the Database without duplicating dates.")
        
        # 1. User Inputs
        col1, col2 = st.columns(2)
        with col1:
            stock_symbol = st.text_input("Stock Symbol (e.g., NHPC):").upper().strip()
        with col2:
            tms_id = st.text_input("Broker TMS ID (e.g., 58):").strip()
            
        # 2. File Upload
        uploaded_file = st.file_uploader("Upload Data File (.txt or .json format)", type=["txt", "json"])
        
        # 3. Inject Button
        if st.button("🚀 Inject Data to MongoDB"):
            if not stock_symbol or not tms_id:
                st.error("❌ Please enter both Stock Symbol and TMS ID.")
            elif uploaded_file is None:
                st.error("❌ Please upload a data file.")
            elif client is None:
                st.error("❌ MongoDB connection is not active.")
            else:
                try:
                    # Read and parse the uploaded file
                    file_content = uploaded_file.read().decode("utf-8")
                    json_data = json.loads(file_content)
                    
                    # Extract the list of records (assuming format {"data": [...]})
                    records = json_data.get("data", [])
                    
                    if not records:
                        st.error("No valid 'data' array found in the file.")
                    else:
                        st.info(f"File parsed successfully. Found {len(records)} daily records.")
                        
                        # Database Setup
                        db = client["StockHoldingByTMS"] # This acts like the "Folder"
                        collection_name = f"{stock_symbol}_{tms_id}" # e.g., NHPC_58
                        collection = db[collection_name]
                        
                        # MERGE LOGIC: Iterate and Upsert (Update if exists, Insert if new)
                        progress_bar = st.progress(0)
                        updated_count = 0
                        inserted_count = 0
                        
                        for i, record in enumerate(records):
                            date_val = record.get("date")
                            
                            if date_val:
                                # Define the query: Check if this date already exists
                                query = {"date": date_val}
                                
                                # Define the new values to set
                                new_values = {"$set": {
                                    "b_qty": int(record.get("b_qty", 0)),
                                    "s_qty": int(record.get("s_qty", 0)),
                                    "b_amt": float(record.get("b_amt", 0)),
                                    "s_amt": float(record.get("s_amt", 0))
                                }}
                                
                                # Upsert to MongoDB
                                result = collection.update_one(query, new_values, upsert=True)
                                
                                if result.matched_count > 0:
                                    updated_count += 1
                                else:
                                    inserted_count += 1
                                    
                            # Update progress bar
                            progress_bar.progress((i + 1) / len(records))
                            
                        # Success Message!
                        st.success(f"✅ Injection Complete for **{collection_name}**!")
                        st.markdown(f"- **New Records Inserted:** {inserted_count}")
                        st.markdown(f"- **Existing Records Updated:** {updated_count}")
                        
                        # Show a preview of the DB to prove it worked
                        st.subheader("Current Database Preview (First 5 Entries)")
                        cursor = collection.find().sort("date", 1).limit(5)
                        df_preview = pd.DataFrame(list(cursor))
                        if not df_preview.empty:
                            df_preview = df_preview.drop(columns=["_id"]) # Hide MongoDB object ID
                            st.dataframe(df_preview)
                            
                except json.JSONDecodeError:
                    st.error("❌ Failed to parse JSON. Ensure the file format is perfectly structured.")
                except Exception as e:
                    st.error(f"❌ An error occurred during injection: {e}")

    with tab2:
        st.info("Other database injection modules will be built here (e.g., Global Market Data).")
