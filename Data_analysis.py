import streamlit as st
import os

# Set page config for the Quantum Terminal vibe
st.set_page_config(page_title="NEPSE Quantum Matrix", page_icon="🌌", layout="wide")

# Import the tab modules (we will create these next)
from Tabs import dashboard, stock_analysis, tms_analysis, data_injector, stock_graph, predictor

# --- 1. LOGIN SYSTEM ---
def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
        if st.session_state["password"] == "quantum2026": # Change this password later!
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show inputs for password.
        st.markdown("## 🌌 Multiversal Market Matrix | Authentication")
        st.text_input("Enter Encryption Key (Password)", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input + error.
        st.markdown("## 🌌 Multiversal Market Matrix | Authentication")
        st.text_input("Enter Encryption Key (Password)", type="password", on_change=password_entered, key="password")
        st.error("❌ Access Denied. Invalid Frequency.")
        return False
    else:
        # Password correct.
        return True

# --- 2. MAIN APP NAVIGATION ---
if check_password():
    st.sidebar.title("🪐 Operations Menu")
    st.sidebar.markdown("---")
    
    # Define the tabs
    tabs = {
        "📊 Dashboard": dashboard,
        "📈 Stock Analysis": stock_analysis,
        "📘 TMS Ledger Analysis": tms_analysis,
        "💉 Data Injector": data_injector,
        "📉 Stock Graphing": stock_graph,
        "🔮 AI Predictor": predictor
    }
    
    # Sidebar selection
    selection = st.sidebar.radio("Select Interface:", list(tabs.keys()))
    
    # Run the selected module
    module = tabs[selection]
    module.run() # Every file in Tabs/ will have a run() function
