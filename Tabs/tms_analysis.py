import streamlit as st
import os

# Important: We need to import the SubTabs module
try:
    from Tabs.SubTabs import Stock_Hold
except ImportError:
    st.error("❌ Failed to load SubTabs. Make sure Tabs/SubTabs/Stock_Hold.py exists and __init__.py is in both folders.")

def run():
    st.title("📘 Broker Telemetry (TMS Analysis)")
    st.markdown("Analyze specific TMS data flows and broker inventory.")
    
    # Define SubTabs
    tab1, tab2, tab3 = st.tabs([
        "📊 TMS Stock Holding", 
        "🔄 Trade Flow Matrix", 
        "🐳 Whale Tracker"
    ])
    
    with tab1:
        # We call the run() function from the Stock_Hold module
        try:
            Stock_Hold.run()
        except Exception as e:
            st.warning("⚠️ Module 'Stock_Hold' is currently under construction or missing.")
            
    with tab2:
        st.info("The Trade Flow Matrix will be implemented in Step 2.")
        
    with tab3:
        st.info("The Whale Tracker will be implemented in Step 3.")
