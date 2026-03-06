import streamlit as st

def run():
    st.title("📈 Stock Analysis Interface")
    st.markdown("Deep dive into individual company fundamentals and technicals.")
    symbol = st.text_input("Enter NEPSE Symbol (e.g., NHPC, NICA):")
    if symbol:
        st.info(f"Retrieving multi-dimensional data for {symbol.upper()}...")
