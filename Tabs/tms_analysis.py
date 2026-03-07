import streamlit as st
import os

# Import the SubTabs
try:
    from Tabs.SubTabs import Stock_Hold
    from Tabs.SubTabs import Visual  # <--- NEW IMPORT
except ImportError:
    st.error("❌ Failed to load SubTabs. Check your __init__.py files.")

def run():
    st.title("📘 Broker Telemetry (TMS Analysis)")
    st.markdown("Analyze specific TMS data flows and broker inventory.")
    
    # Define SubTabs
    tab1, tab2, tab3 = st.tabs([
        "🗃️ Ledger Matrix", 
        "👁️ Visual Flow", 
        "🐳 Whale Tracker"
    ])
    
    with tab1:
        try:
            Stock_Hold.run()
        except Exception as e:
            st.error(f"🛑 CRITICAL SYSTEM FAILURE: {str(e)}")
            st.exception(e)
            
    with tab2: # <--- INJECTING VISUAL.PY HERE
        try:
            Visual.run()
        except Exception as e:
            st.error(f"🛑 CRITICAL SYSTEM FAILURE: {str(e)}")
            st.exception(e)
        
    with tab3:
        st.info("The Whale Tracker will be implemented next.")
