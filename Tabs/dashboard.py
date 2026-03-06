import streamlit as st

def run():
    st.title("📊 Master Dashboard")
    st.markdown("Welcome to the Prime Node. High-level NEPSE metrics will appear here.")
    
    # Placeholder for future metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("NEPSE Index", "2,145.30", "+12.5")
    col2.metric("Portfolio Value", "Rs. ---", "+---")
    col3.metric("Quantum State", "Stable", "")
