import streamlit as st
import requests
import pandas as pd
import json
import os
import base64
from datetime import datetime

# =========================
# CONFIG & SECRETS
# =========================

GITHUB_TOKEN = st.secrets["github"]["token"]
GITHUB_REPO = st.secrets["github"]["repo"]

NEPSE_FSK = st.secrets["nepse_alpha"]["fsk"]
NEPSE_COOKIE = st.secrets["nepse_alpha"]["cookie"]
NEPSE_UA = st.secrets["nepse_alpha"]["user_agent"]

NAVYA_AUTH = st.secrets["navya"]["auth_bearer"]
NAVYA_COOKIE = st.secrets["navya"]["cookie"]

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# =========================
# COMMON HELPERS
# =========================

def save_csv(df, filename):
    path = os.path.join(DATA_DIR, filename)
    df.to_csv(path, index=False)
    return path

def save_json(data, filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path

def github_upload(file_path):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    with open(file_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    payload = {
        "message": f"Add {file_path}",
        "content": content
    }

    r = requests.put(url, headers=headers, json=payload)
    return r.status_code == 201

# =========================
# NAVYA APIs
# =========================

def fetch_navya_macro(category="macro"):
    url = f"https://navyaadvisors.com/api_endpoint/market_cap_valuation/{category}"
    headers = {
        "Authorization": NAVYA_AUTH,
        "Cookie": NAVYA_COOKIE,
        "Accept": "application/json"
    }
    r = requests.get(url, headers=headers)
    return r.json()

def fetch_navya_highlow():
    url = "https://navyaadvisors.com/api_endpoint/stocks/highlow"
    headers = {
        "Authorization": NAVYA_AUTH,
        "Cookie": NAVYA_COOKIE
    }
    r = requests.get(url, headers=headers)
    return r.json()

# =========================
# NEPSEALPHA APIs
# =========================

def fetch_nepse_history(symbol, resolution=1, frame=1):
    url = (
        "https://www.nepsealpha.com/trading/1/history"
        f"?fsk={NEPSE_FSK}&symbol={symbol}&resolution={resolution}&frame={frame}"
    )
    headers = {
        "User-Agent": NEPSE_UA,
        "Cookie": NEPSE_COOKIE,
        "Accept": "application/json"
    }
    r = requests.get(url, headers=headers)
    return r.json()

def fetch_floorsheet_live(symbol, buyer="", seller=""):
    url = (
        "https://nepsealpha.com/floorsheet-live-today/filter"
        f"?fsk={NEPSE_FSK}&stockSymbol={symbol}&buyer={buyer}&seller={seller}&itemsPerPage=500"
    )
    headers = {
        "User-Agent": NEPSE_UA,
        "Cookie": NEPSE_COOKIE
    }
    r = requests.get(url, headers=headers)
    return r.json()

# =========================
# STREAMLIT UI
# =========================

st.title("📊 Pro Stock Tracker – Data Engine")

st.subheader("Navya Advisors")

if st.button("Fetch Market Cap (Macro)"):
    data = fetch_navya_macro("macro")
    df = pd.DataFrame(data)
    file = save_csv(df, "navya_macro.csv")
    github_upload(file)
    st.success("Navya Macro data saved & uploaded")

if st.button("Fetch 52 Week High/Low"):
    data = fetch_navya_highlow()
    df = pd.DataFrame(data)
    file = save_csv(df, "navya_highlow.csv")
    github_upload(file)
    st.success("High/Low data saved & uploaded")

st.subheader("NepseAlpha")

symbol = st.text_input("Stock Symbol", "ULHC")

if st.button("Fetch Trading History"):
    data = fetch_nepse_history(symbol)
    file = save_json(data, f"history_{symbol}.json")
    github_upload(file)
    st.success("Trading history saved & uploaded")

if st.button("Fetch Floorsheet Live"):
    data = fetch_floorsheet_live(symbol)
    file = save_json(data, f"floorsheet_live_{symbol}.json")
    github_upload(file)
    st.success("Floorsheet saved & uploaded")
