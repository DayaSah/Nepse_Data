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
        self.stats = {"KKHC": 0, "SGHC": 0}

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
        # Sort by Date ascending (1) to get the oldest record
        first_record = self.price_collection.find_one({"Stock": symbol}, sort=[("Date", 1)])
        if first_record and "Date" in first_record:
            return first_record["Date"]
        return "2020-01-01" # Fallback if not found

    async def fetch_day(self, session, symbol, date_str, sem):
        """Fetches a single day from Chukul"""
        url = f"https://chukul.com/api/data/historydata/?symbol={symbol}&date={date_str}"
        async with sem:
            try:
                async with session.get(url, headers=self.headers, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        return date_str, data
            except Exception:
                pass
        return date_str, None

    async def run_sync(self):
        start_time = datetime.now()
        self.send_telegram(f"🚀 *NEPSE Sync Started*\n_Running Headless on GitHub Actions_\n\n*Target:* KKHC, SGHC\n*Time:* {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            stocks_to_test = ["KKHC", "SGHC"]
            sem = asyncio.Semaphore(10) # Prevent overloading Chukul
            
            async with aiohttp.ClientSession() as session:
                for symbol in stocks_to_test:
                    start_date_str = self.get_start_date(symbol)
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                    today = datetime.now()
                    
                    # Generate all dates from start to today
                    dates_to_fetch = []
                    current = start_date
                    while current <= today:
                        if current.weekday() < 5: # Skip Saturday/Sunday
                            dates_to_fetch.append(current.strftime("%Y-%m-%d"))
                        current += timedelta(days=1)

                    print(f"📡 Fetching {len(dates_to_fetch)} days for {symbol} (from {start_date_str})...")
                    
                    tasks = [self.fetch_day(session, symbol, d, sem) for d in dates_to_fetch]
                    
                    operations = []
                    
                    # Process as they finish
                    for f in asyncio.as_completed(tasks):
                        date, daily_data = await f
                        if daily_data and isinstance(daily_data, list):
                            for record in daily_data:
                                doc = {
                                    "stock": symbol,
                                    "date": date,
                                    "broker": str(record.get("broker")),
                                    "b_qty": int(float(record.get("buyQty", 0))),
                                    "s_qty": int(float(record.get("sellQty", 0))),
                                    "b_amt": float(record.get("buyAmount", 0.0)),
                                    "s_amt": float(record.get("sellAmount", 0.0))
                                }
                                # Upsert to avoid duplicates in the Temp Collection
                                operations.append(UpdateOne(
                                    {"stock": doc["stock"], "date": doc["date"], "broker": doc["broker"]},
                                    {"$set": doc},
                                    upsert=True
                                ))
                                
                                # Batch write every 2000 records
                                if len(operations) >= 2000:
                                    self.target_collection.bulk_write(operations, ordered=False)
                                    self.stats[symbol] += len(operations)
                                    operations = []

                    # Final push for the stock
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
                f"• KKHC: {self.stats['KKHC']:,}\n"
                f"• SGHC: {self.stats['SGHC']:,}"
            )
            self.send_telegram(report)
            print("Mission Accomplished.")

        except Exception as e:
            # Failure Report
            error_trace = traceback.format_exc()
            fail_msg = (
                f"❌ *NEPSE Sync FAILED*\n"
                f"⏱️ *Failed At:* {datetime.now().strftime('%H:%M:%S')}\n"
                f"⚠️ *Reason:* `{str(e)}`\n"
            )
            self.send_telegram(fail_msg)
            print(error_trace)
            raise e # Fail the GitHub Action

if __name__ == "__main__":
    fetcher = HeadlessFetcher()
    asyncio.run(fetcher.run_sync())
