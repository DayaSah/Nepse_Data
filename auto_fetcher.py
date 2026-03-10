import asyncio
import os
import random
import urllib.request
import urllib.parse
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from pymongo import MongoClient

# --- CONFIGURATION & SECRETS ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017") 
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GITHUB_BACKUP_FOLDER = "Fetcher Data"

# --- GLOBAL TRACKERS ---
fetch_report = {
    "records_fetched": 0,
    "whale_alerts": 0,
    "errors": 0,
    "error_details": []
}
global_csv_buffer = []

# --- 1. TELEGRAM ALERT SYSTEM ---
def send_telegram_alert(message):
    """Sends a push notification to your Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram secrets not found. Outputting to console instead:\n", message)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({'chat_id': TELEGRAM_CHAT_ID, 'text': message}).encode('utf-8')
        urllib.request.urlopen(url, data=data, timeout=10)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

# --- 2. MONGODB & AUTO-TARGETING ---
def get_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        db = client["StockHoldingByTMS"]
        # V3 OPTIMIZATION: High-speed index
        db["market_trades"].create_index([("stock", 1), ("date", -1), ("broker", 1)])
        return db
    except Exception as e:
        msg = f"🔴 DATABASE CRASH: Cannot connect to MongoDB. Error: {e}"
        print(msg)
        send_telegram_alert(msg)
        return None

db = get_db()

def get_dynamic_targets():
    """Scans MongoDB to only fetch Stocks and TMS currently present."""
    if db is None:
        return ['NHPC', 'SGHC'], ['58', '41'] # Fallback
        
    print("🔍 Scanning MongoDB for existing targets...")
    stocks = db["market_trades"].distinct("stock")
    brokers = db["market_trades"].distinct("broker")
    
    # Clean up results
    stocks = [s for s in stocks if s]
    brokers = [str(b) for b in brokers if b and str(b) != '0']
    
    if not stocks: stocks = ['NHPC', 'SGHC', 'ULHC', 'KKHC'] # Fallback if empty DB
    if not brokers: brokers = [str(i) for i in range(1, 101)] # Fallback
    
    return stocks, brokers

def safe_num(val, is_float=False):
    if val is None or str(val).strip() == "": return 0.0 if is_float else 0
    clean_val = str(val).replace(',', '').strip()
    try:
        return float(clean_val) if is_float else int(float(clean_val))
    except: return 0.0 if is_float else 0

# --- 3. THE NETWORK INTERCEPTOR ---
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
                daily_net_qty = 0 
                
                for r in records:
                    b_qty = safe_num(r.get("b_qty"))
                    s_qty = safe_num(r.get("s_qty"))
                    b_amt = safe_num(r.get("b_amt"), True)
                    s_amt = safe_num(r.get("s_amt"), True)
                    r_date = r.get("date")
                    
                    daily_net_qty += (b_qty - s_qty)
                    fetch_report["records_fetched"] += 1
                    
                    # 1. Update MongoDB
                    if db is not None:
                        market_trades.update_one(
                            {"stock": symbol, "broker": str(broker), "date": r_date},
                            {"$set": {
                                "b_qty": b_qty, "s_qty": s_qty,
                                "b_amt": b_amt, "s_amt": s_amt
                            }},
                            upsert=True
                        )
                    
                    # 2. Add to CSV Buffer
                    global_csv_buffer.append({
                        "date": r_date,
                        "b_amt": b_amt, "b_qty": b_qty,
                        "s_amt": s_amt, "s_qty": s_qty,
                        "stock": symbol,
                        "broker": broker
                    })
                
                print(f"   🎯 [FETCHED] {symbol} | Broker {broker}: {len(records)} records.")
                
                # 🐋 WHALE ALERT
                if daily_net_qty > 15000:
                    fetch_report["whale_alerts"] += 1
                elif daily_net_qty < -15000:
                    fetch_report["whale_alerts"] += 1
                    
        except Exception as e: 
            fetch_report["errors"] += 1
            fetch_report["error_details"].append(str(e)[:100])

# --- 4. THE MAIN ORCHESTRATOR ---
async def run_automation():
    start_time = datetime.now()
    send_telegram_alert(f"🚀 **QUANTUM FETCHER INITIATED**\n\n⏰ Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n⚙️ Mode: Headless (GitHub Actions)")
    
    target_stocks, target_brokers = get_dynamic_targets()
    
    print("="*50)
    print(f"☁️ NEPSE GHOST FETCHER STARTING...")
    print(f"🔄 Dynamic Targets: {len(target_stocks)} Stocks | {len(target_brokers)} Brokers")
    print("="*50)

    try:
        async with async_playwright() as p:
            # 🛡️ GITHUB HEADLESS STEALTH
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            await Stealth().apply_page(page) # Activate anti-bot cloaking
            
            page.on("response", global_network_radar)

            print(f"\n🛡️ Initial Handshake & Token Theft...")
            await page.goto("https://nepsealpha.com/floorsheet-history", timeout=60000)
            await asyncio.sleep(5) 
            
            # Steal dynamic FSK token to bypass Cloudflare
            cookies = await context.cookies()
            fsk_token = next((c['value'] for c in cookies if c['name'] == 'fsk'), "1772886222600")
            print(f"🔑 Captured Dynamic Token: {fsk_token}")

            # Scan loop
            for stock in target_stocks:
                print(f"\n📈 Scanning Data for: {stock}")
                for b_id in target_brokers:
                    api_url = f"https://nepsealpha.com/floorsheet-history/filter?fsk={fsk_token}&symbol={stock}&broker={b_id}&dateRangeType=2year"
                    
                    try:
                        await page.goto(api_url, timeout=30000)
                        await asyncio.sleep(random.uniform(2, 4)) # Fast but safe delay
                    except Exception:
                        fetch_report["errors"] += 1
                        continue

            await browser.close()
            
    except Exception as critical_error:
        end_time = datetime.now()
        duration = end_time - start_time
        crash_msg = (
            f"❌ **FATAL SYSTEM CRASH** ❌\n\n"
            f"⏰ End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏱️ Duration: {duration}\n\n"
            f"⚠️ **Error Details:**\n{str(critical_error)[:300]}"
        )
        print(crash_msg)
        send_telegram_alert(crash_msg)
        return

    # --- 5. SAVE CSV BACKUP ---
    if global_csv_buffer:
        os.makedirs(GITHUB_BACKUP_FOLDER, exist_ok=True)
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"{GITHUB_BACKUP_FOLDER}/Nepse_Backup_{timestamp_str}.csv"
        
        df = pd.DataFrame(global_csv_buffer)
        df.to_csv(csv_filename, index=False)
        print(f"💾 CSV Backup saved: {csv_filename}")
    
    # --- SEND FINAL TELEGRAM BRIEFING ---
    end_time = datetime.now()
    duration = end_time - start_time
    success_count = fetch_report["records_fetched"]
    
    if success_count > 0:
        final_msg = (
            f"✅ **QUANTUM FETCH COMPLETE!**\n\n"
            f"⏰ End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏱️ Duration: {str(duration).split('.')[0]}\n"
            f"📊 Total Records Synced: {success_count:,}\n"
            f"🐋 Whale Alerts Fired: {fetch_report['whale_alerts']}\n"
            f"⚠️ Minor Errors: {fetch_report['errors']}\n\n"
            f"💾 CSV Backup Saved to /{GITHUB_BACKUP_FOLDER}\n"
            f"Database successfully updated. 🌌"
        )
    else:
        final_msg = (
            f"⚠️ **FETCH COMPLETED WITH 0 RECORDS**\n\n"
            f"⏱️ Duration: {duration}\n"
            f"NepseAlpha may have blocked the GitHub Actions IP, or no new trades occurred."
        )

    print(f"\n🏁 SCRIPT FINISHED. Sending briefing to Telegram...")
    send_telegram_alert(final_msg)

if __name__ == "__main__":
    asyncio.run(run_automation())
