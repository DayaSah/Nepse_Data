import streamlit as st

def run():
    st.title("💉 Data Injector (MongoDB)")
    st.markdown("Push local data into the MongoDB Cloud Cluster.")
    
    data_type = st.selectbox("Select Data Type to Inject:", ["TMS Data", "Historical Stock Data", "API Dump"])
    if st.button("Inject Data"):
        st.warning(f"Connecting to MongoDB... (Placeholder logic for {data_type})")
