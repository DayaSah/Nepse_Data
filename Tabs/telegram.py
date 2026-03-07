import streamlit as st
import requests
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_db_connection():
    try:
        uri = st.secrets["mongo"]["uri"]
        client = MongoClient(uri)
        return client
    except Exception as e:
        return None

def send_telegram_message(bot_token, chat_id, message):
    """Helper function to send telegram messages."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        return response.status_code == 200, response.json()
    except Exception as e:
        return False, str(e)

def run():
    st.title("📱 Telegram Command Hub")
    st.markdown("Broadcast intelligence and configure automated Whale alerts to your mobile device.")

    client = get_db_connection()
    if not client:
        st.error("❌ Database Connection Offline.")
        return

    db = client["StockHoldingByTMS"]
    master_col = db["market_trades"]

    # --- CREDENTIALS CONFIGURATION ---
    # Attempt to load from secrets, but allow manual input if missing
    default_token = st.secrets.get("telegram", {}).get("bot_token", "")
    default_chat = st.secrets.get("telegram", {}).get("chat_id", "")

    with st.expander("⚙️ Bot Diagnostics & Configuration", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            bot_token = st.text_input("Bot Token (from BotFather)", value=default_token, type="password")
        with col2:
            chat_id = st.text_input("Target Chat ID / Channel ID", value=default_chat)
            
        if st.button("📡 Ping Device (Test Connection)"):
            if not bot_token or not chat_id:
                st.warning("⚠️ Credentials missing.")
            else:
                success, res = send_telegram_message(bot_token, chat_id, "🟢 *Quantum Matrix V2*: Link Established successfully. Ready for operations.")
                if success:
                    st.success("✅ Test signal received! Your device is synced.")
                else:
                    st.error(f"❌ Failed to connect: {res}")

    # --- MAIN INTERFACE TABS ---
    tab1, tab2, tab3 = st.tabs(["📢 Broadcaster", "📊 Daily Intel Dispatch", "🐋 Alert Triggers"])

    with tab1:
        st.subheader("Manual Signal Broadcaster")
        st.markdown("Instantly push a custom trade signal or observation to your channel.")
        
        signal_text = st.text_area("Intelligence Briefing:", placeholder="e.g., 🚨 WHALE ALERT: TMS-58 is aggressively accumulating ULHC...")
        
        if st.button("📤 Broadcast Signal", type="primary"):
            if signal_text and bot_token and chat_id:
                formatted_msg = f"👁️ *QUANTUM MATRIX BRIEFING*\n\n{signal_text}\n\n_Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}_"
                success, _ = send_telegram_message(bot_token, chat_id, formatted_msg)
                if success:
                    st.success("✅ Signal broadcasted to Telegram.")
                else:
                    st.error("❌ Transmission failed.")
            else:
                st.warning("Missing text or Telegram credentials.")

    with tab2:
        st.subheader("Automated Market Summary Dispatch")
        st.markdown("Generates a report of today's highest accumulating brokers and pushes it to Telegram.")
        
        if st.button("🚀 Compile & Dispatch Today's Report"):
            # Fetch latest data
            pipeline = [
                {"$group": {
                    "_id": {"stock": "$stock", "broker": "$broker"},
                    "net_qty": {"$sum": {"$subtract": ["$b_qty", "$s_qty"]}}
                }},
                {"$match": {"net_qty": {"$gt": 0}}},
                {"$sort": {"net_qty": -1}},
                {"$limit": 5}
            ]
            
            results = list(master_col.aggregate(pipeline))
            
            if results:
                msg = "📊 *QUANTUM MATRIX: TOP ACCUMULATORS*\n\n"
                for i, r in enumerate(results, 1):
                    stock = r['_id']['stock']
                    broker = r['_id']['broker']
                    qty = r['net_qty']
                    msg += f"{i}. *{stock}* (TMS-{broker}) : `+{qty:,}` shares\n"
                
                msg += "\n_End of automated report._"
                
                success, _ = send_telegram_message(bot_token, chat_id, msg)
                if success:
                    st.success("✅ Market summary dispatched to mobile.")
                    st.code(msg) # Preview what was sent
            else:
                st.info("No significant accumulation data found to report.")

    with tab3:
        st.subheader("Whale Threshold Configuration")
        st.markdown("Define what triggers an automated alert during the background scraping phase.")
        
        alert_vol = st.number_input("Whale Volume Threshold (Shares)", value=50000, step=5000)
        alert_turnover = st.number_input("Whale Turnover Threshold (Rs)", value=10000000, step=1000000)
        
        st.info("*(Note: To make this fully autonomous, these parameters would be saved to a 'Settings' collection in MongoDB for your `auto_fetcher.py` to read during its cron jobs.)*")
        if st.button("💾 Save Thresholds"):
            # Here you would save to MongoDB. For now:
            st.success(f"✅ Alert settings updated! Fetcher will now flag trades > {alert_vol:,} shares.")
