import streamlit as st

def run():
    st.title("📘 TMS Ledger Analysis")
    st.markdown("Upload broker statements to calculate true P&L and capital flow.")
    uploaded_file = st.file_uploader("Upload TMS Statement (CSV or PDF)", type=["csv", "pdf"])
    if uploaded_file is not None:
        st.success("File ingested. Initiating ledger parsing sequence...")
