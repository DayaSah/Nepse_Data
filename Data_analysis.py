import requests
import json
import time
import pandas as pd
from github import Github
from io import StringIO

# --- CONFIGURATION ---
GITHUB_TOKEN = "your_github_token_here"
REPO_NAME = "your_username/your_repo_name"
STOCKS = ["NHPC", "SGHC", "ULHC", "KKHC", "AKJCL", "RIDI", "SHPC"]
BROKERS = ["58", "41", "48", "52", "55"]

# EXTRACTED HEADERS FROM API.TXT
# Note: You MUST update these tokens regularly or the requests will fail.
NEPSE_ALPHA_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "cookie": "_ga=GA1.1.1825060222.1770908279; cf_clearance=N3y0esBIRHC9BbpH8CkFDarnTUBqevPVXrDy.9YbUE0-1771498720...; nepsealpha_session=eyJpdiI6Ii9BL2daYzFscFVkOVluUmQwNk9NWHc9PSIsInZhbHVlIjoialNlcmZLVzg1QkxYcU5lTjdIZEVqU1hoTDlhY1UxMS9CMFQvMXJsNzRkdVZaRzZ0cENHb3FHcnNLTG1OUWlYcG9jTi9pVzNTR1h0YThXa0NpRzlVTDA3d0phVzBHUWNqV1BuRWVkNGc3eW9FbnRxTWkvTjNJY2VlYXp5SFZzRmciLCJtYWMiOiI0NGIyMWU0ZDc3MmVlOGY5MTNiZmY4NDg4MjY3Mjk4NDNlNjNlMTdjNWJiMzAyYzk2NjIyMzQ3MGIyZWM1MjQyIiwidGFnIjoiIn0%3D", # UPDATE THIS
    "x-requested-with": "XMLHttpRequest"
}

NAVYA_HEADERS = {
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzcxNTcxNzI5LCJpYXQiOjE3NzE0ODUzMjksImp0aSI6ImM5ZjI5ZjU2MWQ4NDQ3NmY4MTNmNWIyMzYyNDM5MWRmIiwidXNlcl9pZCI6NDM5MTR9.NhxJliiOtcrcsu5sUd3zNjDI42Tdn6WIsbGC8ATV0Jw", # UPDATE THIS
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

# --- GITHUB HELPER ---
def upload_to_github(data, filename):
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(REPO_NAME)
    content = json.dumps(data, indent=4) if isinstance(data, dict) else data
    
    try:
        contents = repo.get_contents(filename)
        repo.update_file(contents.path, f"Update {filename}", content, contents.sha)
    except:
        repo.create_file(filename, f"Initial {filename}", content)
    print(f"Successfully stored: {filename}")

# --- FETCHERS ---

def fetch_nepse_alpha_data():
    """Fetches trading history and symbols for target stocks"""
    fsk = "1771326338011" # Extracted from your file
    for stock in STOCKS:
        # 1. Trading History
        url = f"https://www.nepsealpha.com/trading/1/history?fsk={fsk}&symbol={stock}&resolution=1&frame=1"
        res = requests.get(url, headers=NEPSE_ALPHA_HEADERS)
        if res.status_code == 200:
            upload_to_github(res.json(), f"nepse_alpha/history_{stock}.json")
        
        # 2. Live Floorsheet Filter (Stock + Broker 58 example)
        # Note: You can loop through all broker combinations here
        fs_url = f"https://nepsealpha.com/floorsheet-live-today/filter?fsk={fsk}&stockSymbol={stock}&itemsPerPage=500"
        fs_res = requests.get(fs_url, headers=NEPSE_ALPHA_HEADERS)
        if fs_res.status_code == 200:
            upload_to_github(fs_res.json(), f"nepse_alpha/floorsheet_{stock}.json")

def fetch_navya_data():
    """Fetches stock-based and broker-based floorsheets from Navya"""
    # 1. Static/Global Endpoints
    endpoints = {
        "highlow": "https://navyaadvisors.com/api_endpoint/stocks/highlow",
        "market_cap_macro": "https://navyaadvisors.com/api_endpoint/market_cap_valuation/macro",
        "market_cap_mid": "https://navyaadvisors.com/api_endpoint/market_cap_valuation/mid",
        "market_cap_small": "https://navyaadvisors.com/api_endpoint/market_cap_valuation/small"
    }
    
    for name, url in endpoints.items():
        res = requests.get(url, headers=NAVYA_HEADERS)
        if res.status_code == 200:
            upload_to_github(res.json(), f"navya/{name}.json")

    # 2. Broker-Based (For the specific TMS list)
    for broker in BROKERS:
        url = f"https://live.navyaadvisors.com/api/broker-based-floorsheet/{broker}/?period=1d"
        res = requests.get(url, headers=NAVYA_HEADERS)
        if res.status_code == 200:
            upload_to_github(res.json(), f"navya/broker_{broker}_floorsheet.json")

    # 3. Stock-Based
    for stock in STOCKS:
        url = f"https://live.navyaadvisors.com/api/stock-based-floorsheet/{stock}/?period=1d"
        res = requests.get(url, headers=NAVYA_HEADERS)
        if res.status_code == 200:
            upload_to_github(res.json(), f"navya/stock_{stock}_floorsheet.json")

# --- EXECUTION ---
if __name__ == "__main__":
    print("Starting Fetching Cycle...")
    fetch_navya_data()
    fetch_nepse_alpha_data()
    print("Cycle Complete.")
