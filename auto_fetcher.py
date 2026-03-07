import asyncio
import os
import random
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright
from pymongo import MongoClient

# --- CONFIGURATION ---
# It now securely pulls the URI from GitHub Secrets instead of hardcoding it!
MONGO_URI = os.getenv("MONGO_URI") 

def get_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        db = client["StockHoldingByTMS"]
        
        # V2 OPTIMIZATION: Ensure the high-speed index exists on startup
        db["market_trades"].create_index([("stock", 1), ("date", -1), ("broker", 1)])
        return db
    except Exception as e:
        print(f"🔴 CONNECTION ERROR: {e}")
        return None

db = get_db()

# Exclude merged/suspended/inactive brokers
DEAD_BROKERS = [2, 9, 12, 15, 27, 30, 31, 54]
ACTIVE_BROKERS = [str(i) for i in range(1, 91) if i not in DEAD_BROKERS]

def get_tracked_stocks():
    """Scans the master market_trades collection to find which stock symbols are actively tracked."""
    if db is None: return []
    try:
        # V2 OPTIMIZATION: Use distinct() to instantly get all unique stocks from the master collection
        stocks = db["market_trades"].distinct("stock")
        return list(stocks)
    except Exception as e:
        print(f"⚠️ Error scanning for tracked stocks: {e}")
        return []

def safe_num(val, is_float=False):
    if val is None or str(val).strip() == "": return 0.0 if is_float else 0
    clean_val = str(val).replace(',', '').strip()
    try:
        return float(clean_val) if is_float else int(float(clean_val))
    except: return 0.0 if is_float else 0

async def global_network_radar(response):
    if "floorsheet-history/filter" in response.url and response.status == 200:
        try:
            parsed = urlparse(response.url)
            qs = parse_qs(parsed.query)
            symbol = qs.get('symbol', [None])[0]
            broker = qs.get('broker', [None])[0]
            
            if not symbol or not broker: return
            
            json_data = await response.json()
            records = json_data.get("data", [])
            
            if records:
                market_trades = db["market_trades"]
                for r in records:
                    # V2 OPTIMIZATION: Update the Master Collection with stock and broker fields
                    market_trades.update_one(
                        {
                            "stock": symbol, 
                            "broker": str(broker), 
                            "date": r.get("date")
                        },
                        {"$set": {
                            "b_qty": safe_num(r.get("b_qty")),
                            "s_qty": safe_num(r.get("s_qty")),
                            "b_amt": safe_num(r.get("b_amt"), True),
                            "s_amt": safe_num(r.get("s_amt"), True)
                        }},
                        upsert=True # Updates existing dates, adds new ones! No duplicates!
                    )
                print(f"   🎯 [UPDATED] {symbol} | Broker {broker}: {len(records)} records.")
        except Exception: pass

async def run_automation():
    stocks_to_update = get_tracked_stocks()
    if not stocks_to_update:
        print("⚠️ No existing stocks found in MongoDB. You must run the initial vacuum first.")
        return

    print("="*40)
    print(f"☁️ NEPSE GHOST FETCHER (V2 Master Architecture)")
    print(f"🔄 Updating stocks: {', '.join(stocks_to_update)}")
    print("="*40)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await page.add_init_script("delete navigator.__proto__.webdriver")
        
        page.on("response", global_network_radar)

        print(f"\n🛡️  Initial Handshake...")
        try:
            await page.goto("https://nepsealpha.com/floorsheet-history", timeout=60000)
            await asyncio.sleep(10) 
        except Exception as e:
            print(f"❌ Handshake failed: {e}")
            await browser.close()
            return

        for stock in stocks_to_update:
            print(f"\n📈 Starting Fetch for: {stock}")
            for b_id in ACTIVE_BROKERS:
                api_url = f"https://nepsealpha.com/floorsheet-history/filter?fsk=1772847797646&symbol={stock}&broker={b_id}&dateRangeType=1month"
                
                try:
                    await page.goto(api_url, timeout=30000)
                    await asyncio.sleep(random.uniform(3, 7)) 
                except Exception:
                    print(f"⚠️ Broker {b_id} timed out. Skipping...")
                    continue

        print(f"\n🏁 ALL STOCKS SUCCESSFULLY UPDATED IN MONGODB.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_automation())
