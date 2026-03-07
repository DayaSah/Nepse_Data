import streamlit as st
import os
from pymongo import MongoClient

# --- 1. GLOBAL CONFIGURATION ---
st.set_page_config(
    page_title="Quantum Matrix V2 | NEPSE Intelligence",
    page_icon="🌌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for the "Quantum Matrix" vibe
st.markdown("""
    <style>
    .stApp { background-color: #0E1117; color: #E0E0E0; }
    [data-testid="stSidebar"] { background-color: #161B22; border-right: 1px solid #30363D; }
    .stButton>button { width: 100%; border-radius: 5px; background-color: #238636; color: white; }
    .status-box { padding: 10px; border-radius: 5px; border: 1px solid #30363D; background-color: #0D1117; }
    </style>
    """, unsafe_allow_html=True)

# Import the tab modules
from Tabs import (
    dashboard, 
    stock_analysis, 
    tms_analysis, 
    data_injector, 
    stock_graph, 
    predictor, 
    Nepse_Terminal
)

# --- 2. AUTHENTICATION SYSTEM ---
def check_credentials():
    if "credentials_correct" not in st.session_state:
        st.session_state["credentials_correct"] = False
    
    if st.session_state["credentials_correct"]:
        return True

    # Login UI
    st.markdown("<h1 style='text-align: center;'>🌌 QUANTUM MATRIX V2</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #8B949E;'>Terminal Authentication Required</p>", unsafe_allow_html=True)
    
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("---")
            user = st.text_input("Identity (Username)")
            key = st.text_input("Encryption Key", type="password")
            
            if st.button("Initiate Link"):
                if user == st.secrets["credentials"]["username"] and key == st.secrets["credentials"]["password"]:
                    st.session_state["credentials_correct"] = True
                    st.rerun()
                else:
                    st.error("❌ Invalid Frequency. Connection Refused.")
    return False

# --- 3. SYSTEM DIAGNOSTICS (Sidebar) ---
def sidebar_diagnostics():
    st.sidebar.title("🪐 Operations Menu")
    
    with st.sidebar.expander("📡 System Status", expanded=True):
        try:
            client = MongoClient(st.secrets["mongo"]["uri"], serverSelectionTimeoutMS=2000)
            client.server_info()
            st.markdown("● **Database:** <span style='color: #238636;'>ONLINE</span>", unsafe_allow_html=True)
        except:
            st.markdown("● **Database:** <span style='color: #D73A49;'>OFFLINE</span>", unsafe_allow_html=True)
        
        st.markdown("● **Matrix Version:** 2.0.4 (Master)")
        st.markdown("● **Security:** AES-256 Encrypted")

# --- 4. MAIN APP LOGIC ---
if check_credentials():
    # Render Sidebar
    sidebar_diagnostics()
    st.sidebar.markdown("---")
    
    # Navigation Matrix
    tabs = {
        "📊 Command Dashboard": dashboard,
        "📈 Stock Scanner": stock_analysis,
        "📘 TMS Intelligence": tms_analysis,
        "📉 Technical Terminal": stock_graph,
        "🔮 AI Predictor": predictor,
        "💉 Data Injector": data_injector,
        "📡 Nepse Terminal": Nepse_Terminal
    }
    
    selection = st.sidebar.radio("Select Interface Layer:", list(tabs.keys()))
    
    # Global Header (Visible on every tab)
    st.markdown(f"""
        <div style='background-color: #161B22; padding: 10px; border-radius: 10px; border-left: 5px solid #238636; margin-bottom: 20px;'>
            <span style='color: #8B949E;'>CURRENT LAYER:</span> <b style='color: #58A6FF;'>{selection.upper()}</b> | 
            <span style='color: #8B949E;'>SYNC STATUS:</span> <b style='color: #238636;'>LIVE DATA STREAM</b>
        </div>
    """, unsafe_allow_html=True)

    # Run the selected module
    try:
        tabs[selection].run()
    except Exception as e:
        st.error(f"⚠️ MODULE CRASH: {e}")
        st.info("Attempting to re-establish connection to the Master Matrix...")
        if st.button("Re-Sync System"):
            st.rerun()

    # Footer
    st.sidebar.markdown("---")
    if st.sidebar.button("🔓 Terminate Session"):
        st.session_state["credentials_correct"] = False
        st.rerun()
