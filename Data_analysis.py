import streamlit as st
import os

# Set page config for the Quantum Terminal vibe
st.set_page_config(page_title="NEPSE Quantum Matrix", page_icon="🌌", layout="wide")

# Import the tab modules (we will create these next)
from Tabs import dashboard, stock_analysis, tms_analysis, data_injector, stock_graph, predictor

# --- 1. LOGIN SYSTEM ---
def check_credentials():
    """Returns True if the user enters the correct username and password."""
    
    # Initialize session state variables if they don't exist
    if "credentials_correct" not in st.session_state:
        st.session_state["credentials_correct"] = False

    def verify_login():
        # Check if the inputs match the secrets
        expected_user = st.secrets["credentials"]["username"]
        expected_pass = st.secrets["credentials"]["password"]
        
        if st.session_state["user_input"] == expected_user and st.session_state["pass_input"] == expected_pass:
            st.session_state["credentials_correct"] = True
            # Clear passwords from memory for security
            del st.session_state["pass_input"]
        else:
            st.session_state["credentials_correct"] = False
            st.session_state["login_failed"] = True

    # If already logged in, let them through
    if st.session_state["credentials_correct"]:
        return True

    # If not logged in, show the login screen
    st.markdown("## 🌌 Multiversal Market Matrix | Authentication")
    st.text_input("Username", key="user_input")
    st.text_input("Encryption Key (Password)", type="password", key="pass_input")
    
    st.button("Initiate Link", on_click=verify_login)
    
    # Show error if they tried and failed
    if st.session_state.get("login_failed"):
        st.error("❌ Access Denied. Invalid Frequency or Identity.")
        
    return False

# --- 2. MAIN APP NAVIGATION ---
if check_credentials():  
    st.sidebar.title("🪐 Operations Menu")
    
    st.sidebar.markdown("---")
    
    # Define the tabs
    tabs = {
        "📊 Dashboard": dashboard,
        "📈 Stock Analysis": stock_analysis,
        "📘 TMS Data Analysis": tms_analysis,
        "💉 Data Injector": data_injector,
        "📉 Stock Graphing": stock_graph,
        "🔮 AI Predictor": predictor
    }
    
    # Sidebar selection
    selection = st.sidebar.radio("Select Interface:", list(tabs.keys()))
    
    # Run the selected module
    module = tabs[selection]
    module.run() # Every file in Tabs/ will have a run() function
