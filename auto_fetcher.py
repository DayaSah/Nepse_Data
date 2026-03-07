import asyncio
import os
import random
import urllib.request
import urllib.parse
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright
from pymongo import MongoClient

# --- CONFIGURATION & SECRETS ---
MONGO_URI = os.getenv("MONGO_URI") 
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- CUSTOM EXTRACTION TARGETS ---
TARGET_STOCKS = ['KKHC', 'RHGCL', 'SPL', 'RIDI', 'NGPL', 'RADHI']

# --- GLOBAL TRACKERS FOR FINAL REPORT ---
fetch_report = {
    "stocks_updated": set(),
    "whale_alerts": 0,
    "errors": 0
}

# --- TELEGRAM ALERT FUNCTION ---
def send_telegram_alert(message):
    """Sends a push notification to your Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram secrets not found. Skipping alert...")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({'chat_id': TELEGRAM_CHAT_ID, 'text': message}).encode('utf-8')
        urllib.request.urlopen(url, data=data, timeout=5)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

# --- MONGODB SETUP ---
def get_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        db = client["StockHoldingByTMS"]
        # V2 OPTIMIZATION: Ensure the high-speed index exists on startup
        db["market_trades"].create_index([("stock", 1), ("date", -1), ("broker", 1)])
        return db
    except Exception as e:
        print(f"🔴 CONNECTION ERROR: {e}")
        send_telegram_alert(f"🚨 DATABASE ERROR: Cannot connect to MongoDB. Error: {e}")
        return None

db = get_db()

# Exclude merged/suspended/inactive brokers (Scanning 1 to 100)
DEAD_BROKERS = [2, 9, 12, 15, 27, 30, 31, 54]
ACTIVE_BROKERS = [str(i) for i in range(1, 101) if i not in DEAD_BROKERS]

def safe_num(val, is_float=False):
    if val is None or str(val).strip() == "": return 0.0 if is_float else 0
    clean_val = str(val).replace(',', '').strip()
    try:
        return float(clean_val) if is_float else int(float(clean_val))
    except: return 0.0 if is_float else 0

# --- THE NETWORK INTERCEPTOR & WHALE RADAR ---
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
                daily_net_qty = 0 # Track accumulation for Whale Alert
                
                for r in records:
                    b_qty = safe_num(r.get("b_qty"))
                    s_qty = safe_num(r.get("s_qty"))
                    daily_net_qty += (b_qty - s_qty)
                    
                    market_trades.update_one(
                        {"stock": symbol, "broker": str(broker), "date": r.get("date")},
                        {"$set": {
                            "b_qty": b_qty, "s_qty": s_qty,
                            "b_amt": safe_num(r.get("b_amt"), True), "s_amt": safe_num(r.get("s_amt"), True)
                        }},
                        upsert=True
                    )
                
                print(f"   🎯 [UPDATED] {symbol} | Broker {broker}: {len(records)} records.")
                fetch_report["stocks_updated"].add(symbol)
                
                # 🐋 WHALE ALERT SYSTEM 🐋
                if daily_net_qty > 10000:
                    send_telegram_alert(f"🐋 WHALE BUY ALERT!\nBroker {broker} accumulated +{daily_net_qty:,} shares of {symbol}!")
                    fetch_report["whale_alerts"] += 1
                elif daily_net_qty < -10000:
                    send_telegram_alert(f"🩸 DUMP ALERT!\nBroker {broker} unloaded {daily_net_qty:,} shares of {symbol}!")
                    fetch_report["whale_alerts"] += 1
                    
        except Exception as e: 
            fetch_report["errors"] += 1

# --- THE MAIN ORCHESTRATOR ---
async def run_automation():
    stocks_to_update = TARGET_STOCKS
    
    print("="*40)
    print(f"☁️ NEPSE GHOST FETCHER STARTING...")
    print(f"🔄 Targets: {len(stocks_to_update)} stocks (2-Year Deep Scan)")
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
            error_msg = f"❌ Handshake failed (Cloudflare block or site down). Error: {str(e)[:100]}"
            print(error_msg)
            send_telegram_alert(f"🚨 SCRAPER CRASHED: {error_msg}")
            await browser.close()
            return

        for stock in stocks_to_update:
            print(f"\n📈 Scanning 2-Year Market Data for: {stock}")
            for b_id in ACTIVE_BROKERS:
                # V2 URL Injection: 2 Year Range & Custom FSK
                api_url = f"https://nepsealpha.com/floorsheet-history/filter?fsk=1772886222600&symbol={stock}&broker={b_id}&dateRangeType=2year"
                
                try:
                    await page.goto(api_url, timeout=30000)
                    # Random delay to prevent IP ban while deep-scanning
                    await asyncio.sleep(random.uniform(3, 7)) 
                except Exception:
                    print(f"⚠️ Broker {b_id} timed out. Skipping...")
                    fetch_report["errors"] += 1
                    continue

        await browser.close()

    # --- SEND FINAL TELEGRAM BRIEFING ---
    success_count = len(fetch_report["stocks_updated"])
    
    if success_count > 0:
        final_msg = (
            f"✅ **2-YEAR DEEP SCAN COMPLETE!**\n\n"
            f"📈 Stocks Successfully Passed: {success_count} / {len(stocks_to_update)}\n"
            f"🐋 Whale Alerts Fired: {fetch_report['whale_alerts']}\n"
            f"⚠️ Minor Errors/Timeouts: {fetch_report['errors']}\n\n"
            f"Database successfully updated. 🌌"
        )
    else:
        final_msg = "❌ **NEPSE Update Failed.** The scraper ran, but ZERO records were updated. NepseAlpha may have changed their API structure or blocked the IP entirely."

    print(f"\n🏁 SCRIPT FINISHED. Sending briefing to Telegram...")
    send_telegram_alert(final_msg)

if __name__ == "__main__":
    asyncio.run(run_automation())
