import streamlit as st
import requests
import json
import time
from github import Github
from datetime import datetime

# --- CONFIGURATION & TARGETS ---
STOCKS = ["NHPC", "SGHC", "ULHC", "KKHC", "AKJCL", "RIDI", "SHPC"]
BROKERS = ["58", "41", "48", "52", "55"]

# Headers from Secrets
NA_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "cookie": st.secrets["nepse_alpha"]["cookie"],
    "user-agent": st.secrets["nepse_alpha"]["user_agent"],
    "x-requested-with": "XMLHttpRequest"
}

NAVYA_HEADERS = {
    "authorization": st.secrets["navya"]["auth_bearer"],
    "cookie": st.secrets["navya"]["cookie"],
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

# --- GITHUB STORAGE ENGINE ---
def save_to_github(data, filename):
    try:
        g = Github(st.secrets["github"]["token"])
        repo = g.get_repo(st.secrets["github"]["repo"])
        content = json.dumps(data, indent=2)
        
        try:
            # Update existing file
            file = repo.get_contents(filename)
            repo.update_file(file.path, f"Weekly Update: {datetime.now()}", content, file.sha)
        except:
            # Create new file
            repo.create_file(filename, "Initial Upload", content)
        return True
    except Exception as e:
        st.error(f"GitHub Error for {filename}: {e}")
        return False

# --- API FETCHERS ---
def run_batch_update():
    fsk = st.secrets["nepse_alpha"]["fsk"]
    progress_bar = st.progress(0)
    status = st.empty()
    
    # API 1 & 2: Nepse Alpha Symbol & History
    for i, stock in enumerate(STOCKS):
        status.text(f"Fetching NepseAlpha: {stock}")
        # Symbol Details
        res1 = requests.get(f"https://www.nepsealpha.com/trading/1/symbols?fsk={fsk}&symbol={stock}", headers=NA_HEADERS)
        if res1.status_code == 200:
            save_to_github(res1.json(), f"api1/details_{stock}.json")
            
        # Price History
        res2 = requests.get(f"https://www.nepsealpha.com/trading/1/history?fsk={fsk}&symbol={stock}&resolution=1&frame=1", headers=NA_HEADERS)
        if res2.status_code == 200:
            save_to_github(res2.json(), f"api2/history_{stock}.json")
        time.sleep(1) # Safety delay
    
    # API 3 & 4: Navya Trading Data & HighLow
    status.text("Fetching Navya Global Data...")
    res3 = requests.get("https://navyaadvisors.com/api_endpoint/stocks/highlow", headers=NAVYA_HEADERS)
    if res3.status_code == 200:
        save_to_github(res3.json(), "api4/highlow.json")

    # API 5: Market Cap (Macro/Mid/Small)
    for cap in ["macro", "mid", "small"]:
        res4 = requests.get(f"https://navyaadvisors.com/api_endpoint/market_cap_valuation/{cap}", headers=NAVYA_HEADERS)
        if res4.status_code == 200:
            save_to_github(res4.json(), f"api5/market_cap_{cap}.json")

    # API 6: Broker Based Floorsheet (Target Brokers)
    for broker in BROKERS:
        status.text(f"Fetching Broker Floorsheet: {broker}")
        res5 = requests.get(f"https://live.navyaadvisors.com/api/broker-based-floorsheet/{broker}/?period=1w", headers=NAVYA_HEADERS)
        if res5.status_code == 200:
            save_to_github(res5.json(), f"api6/broker_{broker}.json")
        time.sleep(1)

    # API 7: Stock Based Floorsheet (Target Stocks)
    for stock in STOCKS:
        status.text(f"Fetching Stock Floorsheet: {stock}")
        res6 = requests.get(f"https://live.navyaadvisors.com/api/stock-based-floorsheet/{stock}/?period=1w", headers=NAVYA_HEADERS)
        if res6.status_code == 200:
            save_to_github(res6.json(), f"api7/stock_{stock}.json")

    status.success("Weekly Update Complete!")
    progress_bar.progress(100)

# --- STREAMLIT UI ---
st.title("NEPSE Manipulation - Weekly Data Fetcher")
st.warning("Ensure your Bearer Tokens and Cookies in secrets.toml are fresh before running.")

if st.button("🚀 Run Weekly Batch Update"):
    with st.spinner("Processing APIs and syncing to GitHub..."):
        run_batch_update()

st.sidebar.header("Target Assets")
st.sidebar.write("**Stocks:**", ", ".join(STOCKS))
st.sidebar.write("**Brokers:**", ", ".join(BROKERS))
