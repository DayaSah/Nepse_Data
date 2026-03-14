import os
import json
import asyncio
import aiohttp
import requests
import traceback
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

class HeadlessFetcher:
    def __init__(self):
        # 1. Load Secrets from GitHub Environment
        self.mongo_uri = os.environ.get("MONGO_URI")
        self.tele_token = os.environ.get("TELEGRAM_TOKEN")
        self.tele_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        if not self.mongo_uri:
            raise ValueError("MONGO_URI secret is missing!")

        # 2. Connect to MongoDB
        self.client = MongoClient(self.mongo_uri, tlsAllowInvalidCertificates=True)
        self.db = self.client["StockHoldingByTMS"]
        self.target_collection = self.db["market_trades_temp"] # TEMP COLLECTION FOR TESTING
        self.price_collection = self.db["Stock_Price_Volume"]

        # 3. Setup Fetching config
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
        
        # Tracking for Telegram Report
        self.stats = {"SGHC": 0} # ONLY SGHC FOR NOW

    def send_telegram(self, message):
        """Sends a message to your Telegram Chat"""
        if not self.tele_token or not self.tele_chat_id:
            print("Telegram credentials missing, skipping message.")
            return
            
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        payload = {"chat_id": self.tele_chat_id, "text": message, "parse_mode": "Markdown"}
        try:
            requests.post(url, json=payload)
        except Exception as e:
            print(f"Failed to send Telegram message: {e}")

    def get_start_date(self, symbol):
        """Finds the very first trading day for the stock from your DB"""
        first_record = self.price_collection.find_one({"Stock": symbol}, sort=[("Date", 1)])
        if first_record and "Date" in first_record:
            return first_record["Date"]
        return "2020-01-01" 

    async def fetch_day(self, session, symbol, date_str, sem):
        """Fetches Buy and Sell data concurrently from the new API endpoints"""
        url_buy = f"https://chukul.com/api/data/top-buy/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        url_sell = f"https://chukul.com/api/data/top-sell/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        
        async with sem:
            try:
                # Fire BOTH requests at the exact same time for maximum speed
                buy_task = session.get(url_buy, headers=self.headers, timeout=15)
                sell_task = session.get(url_sell, headers=self.headers, timeout=15)
                
                # Wait for both to finish
                buy_res, sell_res = await asyncio.gather(buy_task, sell_task, return_exceptions=True)
                
                buy_data = []
                sell_data = []

                if not isinstance(buy_res, Exception) and buy_res.status == 200:
                    buy_data = await buy_res.json()
                if not isinstance(sell_res, Exception) and sell_res.status == 200:
                    sell_data = await sell_res.json()

                # --- MERGE BUY AND SELL DATA ---
                daily_brokers = {}
                
                # Helper function to extract lists from API response
                def extract_records(api_response):
                    if isinstance(api_response, list): return api_response
                    if isinstance(api_response, dict): return api_response.get("data", [])
                    return []

                # 1. Process Buys
                for r in extract_records(buy_data):
                    b_id = str(r.get("broker", "")).strip()
                    if not b_id or b_id.lower() == "none" or b_id == "0": continue
                    
                    # API might use 'quantity' instead of 'buyQty' in these specific endpoints
                    qty = int(float(r.get("buyQty", r.get("quantity", 0))))
                    amt = float(r.get("buyAmount", r.get("amount", 0.0)))
                    
                    daily_brokers[b_id] = {"broker": b_id, "b_qty": qty, "b_amt": amt, "s_qty": 0, "s_amt": 0.0}

                # 2. Process Sells
                for r in extract_records(sell_data):
                    b_id = str(r.get("broker", "")).strip()
                    if not b_id or b_id.lower() == "none" or b_id == "0": continue
                    
                    qty = int(float(r.get("sellQty", r.get("quantity", 0))))
                    amt = float(r.get("sellAmount", r.get("amount", 0.0)))

                    if b_id not in daily_brokers:
                        daily_brokers[b_id] = {"broker": b_id, "b_qty": 0, "b_amt": 0.0, "s_qty": qty, "s_amt": amt}
                    else:
                        daily_brokers[b_id]["s_qty"] = qty
                        daily_brokers[b_id]["s_amt"] = amt

                return date_str, list(daily_brokers.values())

            except Exception as e:
                # Fail silently for individual days to keep the script running
                pass
                
        return date_str, []

    async def run_sync(self):
        start_time = datetime.now()
        self.send_telegram(f"🚀 *NEPSE Sync Started*\n_Running Headless on GitHub Actions_\n\n*Target:* SGHC\n*Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            stocks_to_test = ["SGHC"] # TEMPORARY TARGET
            sem = asyncio.Semaphore(25) # SPEED BOOST: Increased from 10 to 25 concurrent connections
            
            async with aiohttp.ClientSession() as session:
                for symbol in stocks_to_test:
                    start_date_str = self.get_start_date(symbol)
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                    today = datetime.now()
                    
                    # Generate all valid dates
                    dates_to_fetch = []
                    current = start_date
                    while current <= today:
                        if current.weekday() < 5: 
                            dates_to_fetch.append(current.strftime("%Y-%m-%d"))
                        current += timedelta(days=1)

                    print(f"📡 Fetching {len(dates_to_fetch)} days for {symbol} (from {start_date_str})...")
                    
                    tasks = [self.fetch_day(session, symbol, d, sem) for d in dates_to_fetch]
                    operations = []
                    
                    for f in asyncio.as_completed(tasks):
                        date, daily_data = await f
                        
                        for record in daily_data:
                            # --- THE BOUNCER ---
                            # If they bought 0 AND sold 0, throw the record away
                            if record["b_qty"] == 0 and record["s_qty"] == 0:
                                continue
                            
                            doc = {
                                "stock": symbol,
                                "date": date,
                                "broker": record["broker"],
                                "b_qty": record["b_qty"],
                                "s_qty": record["s_qty"],
                                "b_amt": record["b_amt"],
                                "s_amt": record["s_amt"]
                            }
                            
                            operations.append(UpdateOne(
                                {"stock": doc["stock"], "date": doc["date"], "broker": doc["broker"]},
                                {"$set": doc},
                                upsert=True
                            ))
                            
                            if len(operations) >= 2000:
                                self.target_collection.bulk_write(operations, ordered=False)
                                self.stats[symbol] += len(operations)
                                operations = []

                    if operations:
                        self.target_collection.bulk_write(operations, ordered=False)
                        self.stats[symbol] += len(operations)

            # Success Report
            end_time = datetime.now()
            duration = end_time - start_time
            
            report = (
                f"✅ *NEPSE Sync Complete*\n"
                f"⏱️ *Duration:* {duration.total_seconds():.1f} seconds\n\n"
                f"📊 *Records Synced to `market_trades_temp`:*\n"
                f"• SGHC: {self.stats['SGHC']:,}"
            )
            self.send_telegram(report)
            print("Mission Accomplished.")

        except Exception as e:
            error_trace = traceback.format_exc()
            fail_msg = (
                f"❌ *NEPSE Sync FAILED*\n"
                f"⏱️ *Failed At:* {datetime.now().strftime('%H:%M:%S')}\n"
                f"⚠️ *Reason:* `{str(e)}`\n"
            )
            self.send_telegram(fail_msg)
            print(error_trace)
            raise e

if __name__ == "__main__":
    fetcher = HeadlessFetcher()
    asyncio.run(fetcher.run_sync())
