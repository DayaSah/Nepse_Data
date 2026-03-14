import os
import asyncio
import aiohttp
import requests
import traceback
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

class HeadlessFetcher:
    def __init__(self):
        self.mongo_uri = os.environ.get("MONGO_URI")
        self.tele_token = os.environ.get("TELEGRAM_TOKEN")
        self.tele_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        if not self.mongo_uri:
            raise ValueError("MONGO_URI secret is missing!")

        self.client = MongoClient(self.mongo_uri, tlsAllowInvalidCertificates=True)
        self.db = self.client["StockHoldingByTMS"]
        self.target_collection = self.db["market_trades_temp"]
        self.price_collection = self.db["Stock_Price_Volume"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://chukul.com/"
        }
        
        self.stats = {"SGHC": 0}

    def send_telegram(self, message):
        if not self.tele_token or not self.tele_chat_id: return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        payload = {"chat_id": self.tele_chat_id, "text": message, "parse_mode": "Markdown"}
        try: requests.post(url, json=payload)
        except: pass

    def get_start_date(self, symbol):
        # Finds the oldest date in your Price collection
        first_record = self.price_collection.find_one({"Stock": symbol}, sort=[("Date", 1)])
        if first_record and "Date" in first_record:
            return first_record["Date"]
        return "2024-01-01" # Fallback if no history found

    async def fetch_day(self, session, symbol, date_str, sem):
        url_buy = f"https://chukul.com/api/data/top-buy/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        url_sell = f"https://chukul.com/api/data/top-sell/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        
        async with sem:
            try:
                # Use a single session to fetch both buy and sell data for the day
                async with session.get(url_buy, headers=self.headers, timeout=10) as r_buy, \
                           session.get(url_sell, headers=self.headers, timeout=10) as r_sell:
                    
                    buy_json = await r_buy.json() if r_buy.status == 200 else []
                    sell_json = await r_sell.json() if r_sell.status == 200 else []

                    daily_brokers = {}

                    # 1. PROCESS BUYS (Uses 'buyer', 'quantity', 'amount')
                    if isinstance(buy_json, list):
                        for r in buy_json:
                            b_id = str(r.get("buyer") or r.get("broker") or "").strip()
                            if not b_id or b_id == "0": continue
                            
                            qty = int(float(r.get("quantity") or r.get("buyQty") or 0))
                            amt = float(r.get("amount") or r.get("buyAmount") or 0.0)
                            
                            if qty > 0:
                                daily_brokers[b_id] = {"broker": b_id, "b_qty": qty, "b_amt": amt, "s_qty": 0, "s_amt": 0.0}

                    # 2. PROCESS SELLS (Uses 'seller', 'quantity', 'amount')
                    if isinstance(sell_json, list):
                        for r in sell_json:
                            b_id = str(r.get("seller") or r.get("broker") or "").strip()
                            if not b_id or b_id == "0": continue
                            
                            qty = int(float(r.get("quantity") or r.get("sellQty") or 0))
                            amt = float(r.get("amount") or r.get("sellAmount") or 0.0)

                            if qty > 0:
                                if b_id not in daily_brokers:
                                    daily_brokers[b_id] = {"broker": b_id, "b_qty": 0, "b_amt": 0.0, "s_qty": qty, "s_amt": amt}
                                else:
                                    daily_brokers[b_id]["s_qty"] = qty
                                    daily_brokers[b_id]["s_amt"] = amt

                    return date_str, list(daily_brokers.values())
            except Exception:
                return date_str, []

    async def run_sync(self):
        start_time = datetime.now()
        target_symbol = "SGHC" # FETCH ONLY SGHC FOR NOW
        self.send_telegram(f"🚀 *SGHC Deep Sync Started*")

        try:
            sem = asyncio.Semaphore(20) # Speed: 20 parallel connections
            
            async with aiohttp.ClientSession() as session:
                start_date_str = self.get_start_date(target_symbol)
                current = datetime.strptime(start_date_str, "%Y-%m-%d")
                today = datetime.now()
                
                dates = []
                while current <= today:
                    if current.weekday() < 5: # Monday-Friday
                        dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)

                print(f"📡 Fetching {len(dates)} days for {target_symbol}...")
                tasks = [self.fetch_day(session, target_symbol, d, sem) for d in dates]
                
                operations = []
                total_synced = 0
                
                for f in asyncio.as_completed(tasks):
                    date, daily_data = await f
                    if not daily_data: continue

                    for record in daily_data:
                        # THE BOUNCER: Skip empty trades
                        if record["b_qty"] == 0 and record["s_qty"] == 0:
                            continue

                        doc = {
                            "stock": target_symbol,
                            "date": date,
                            "broker": record["broker"],
                            "b_qty": record["b_qty"],
                            "s_qty": record["s_qty"],
                            "b_amt": record["b_amt"],
                            "s_amt": record["s_amt"]
                        }
                        
                        operations.append(UpdateOne(
                            {"stock": target_symbol, "date": date, "broker": record["broker"]},
                            {"$set": doc},
                            upsert=True
                        ))

                    # Periodic bulk write
                    if len(operations) >= 1000:
                        res = self.target_collection.bulk_write(operations, ordered=False)
                        total_synced += (res.upserted_count + res.modified_count)
                        operations = []

                # Final bulk write
                if operations:
                    res = self.target_collection.bulk_write(operations, ordered=False)
                    total_synced += (res.upserted_count + res.modified_count)

            duration = (datetime.now() - start_time).total_seconds()
            self.send_telegram(f"✅ *SGHC Sync Complete*\n📊 Records Saved: {total_synced:,}\n⏱ Time: {duration:.1f}s")

        except Exception as e:
            self.send_telegram(f"❌ *Fatal Error*: `{str(e)[:100]}`")
            print(traceback.format_exc())
            raise e

if __name__ == "__main__":
    asyncio.run(HeadlessFetcher().run_sync())
