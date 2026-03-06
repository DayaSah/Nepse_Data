import streamlit as st

def run():
    st.title("📘 TMS-Centric Surveillance")
    st.markdown("Input a Broker (TMS) ID to reveal their entire trading wave-function (Buy/Sell/Holdings).")
    
    tms_id = st.text_input("Enter Broker TMS ID (e.g., 58, 34):", "")
    
    if tms_id:
        st.warning(f"Intercepting order flow for Broker TMS-{tms_id}...")
        
        st.subheader(f"Current Portfolio Holdings for TMS-{tms_id}")
        # Simulated Portfolio Data
        st.dataframe({
            "Stock": ["NHPC", "NICA", "SHIVM", "HDHPC"],
            "Total Bought": [100000, 5000, 25000, 80000],
            "Total Sold": [20000, 4500, 10000, 80000],
            "Net Holding": [80000, 500, 15000, 0]
        })
        
        st.subheader("Recent Market Activity")
        st.line_chart([5, 10, 8, 15, 20, 18, 25]) # Represents trading volume over time
