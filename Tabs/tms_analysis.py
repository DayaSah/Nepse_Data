import streamlit as st
import importlib  


# Import the SubTabs
try:
    from Tabs.SubTabs import Stock_Hold
    from Tabs.SubTabs import Visual
    from Tabs.SubTabs import Whale
    from Tabs.SubTabs import TMS_Holdings
    importlib.reload(Stock_Hold)
    importlib.reload(Visual)
    importlib.reload(Whale)
    importlib.reload(TMS_Holdings)
      
except ImportError:
    st.error("❌ Failed to load SubTabs. Check your __init__.py files.")

def run():
    st.title("📘 Broker Telemetry (TMS Analysis)")
    st.markdown("Analyze specific TMS data flows and broker inventory.")
   

# Import the SubTabs


    
    # Define SubTabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🗃️ Ledger Matrix", 
        "👁️ Visual Flow", 
        "🐳 Whale Tracker",
        "🏢 TMS Holdings"
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
        
    with tab3: # <--- INJECTING WHALE.PY HERE
        try:
            Whale.run()
        except Exception as e:
            st.error(f"🛑 CRITICAL SYSTEM FAILURE: {str(e)}")
            st.exception(e)
    with tab4: 
        try:
            TMS_Holdings.run()
        except Exception as e:
            st.error(f"🛑 CRITICAL SYSTEM FAILURE: {str(e)}")
            st.exception(e)
