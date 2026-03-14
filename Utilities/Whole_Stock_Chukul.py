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
        # We look for the oldest date in your DB
        first_record = self.price_collection.find_one({"Stock": symbol}, sort=[("Date", 1)])
        if first_record and "Date" in first_record:
            return first_record["Date"]
        return "2024-01-01" # Start from 2024 if not found for testing speed

    async def fetch_day(self, session, symbol, date_str, sem):
        url_buy = f"https://chukul.com/api/data/top-buy/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        url_sell = f"https://chukul.com/api/data/top-sell/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        
        async with sem:
            try:
                async with session.get(url_buy, headers=self.headers, timeout=10) as r_buy, \
                           session.get(url_sell, headers=self.headers, timeout=10) as r_sell:
                    
                    buy_json = await r_buy.json() if r_buy.status == 200 else []
                    sell_json = await r_sell.json() if r_sell.status == 200 else []

                    daily_brokers = {}

                    # --- ENHANCED PARSING LOGIC ---
                    def get_list(data):
                        if isinstance(data, list): return data
                        if isinstance(data, dict):
                            # Chukul sometimes nests data under "data" or "results"
                            return data.get("data", data.get("results", []))
                        return []

                    # Process Buys
                    for r in get_list(buy_json):
                        # Chukul keys can vary: 'broker', 'broker_id', or 'brokerNum'
                        b_id = str(r.get("broker") or r.get("broker_id") or "")
                        if not b_id or b_id.lower() == "none": continue
                        
                        qty = int(float(r.get("buyQty") or r.get("quantity") or 0))
                        amt = float(r.get("buyAmount") or r.get("amount") or 0.0)
                        
                        if qty > 0:
                            daily_brokers[b_id] = {"broker": b_id, "b_qty": qty, "b_amt": amt, "s_qty": 0, "s_amt": 0.0}

                    # Process Sells
                    for r in get_list(sell_json):
                        b_id = str(r.get("broker") or r.get("broker_id") or "")
                        if not b_id or b_id.lower() == "none": continue
                        
                        qty = int(float(r.get("sellQty") or r.get("quantity") or 0))
                        amt = float(r.get("sellAmount") or r.get("amount") or 0.0)

                        if qty > 0:
                            if b_id not in daily_brokers:
                                daily_brokers[b_id] = {"broker": b_id, "b_qty": 0, "b_amt": 0.0, "s_qty": qty, "s_amt": amt}
                            else:
                                daily_brokers[b_id]["s_qty"] = qty
                                daily_brokers[b_id]["s_amt"] = amt

                    return date_str, list(daily_brokers.values())
            except:
                return date_str, []

    async def run_sync(self):
        start_time = datetime.now()
        self.send_telegram("🚀 *SGHC Test Sync Started*")

        try:
            symbol = "SGHC"
            sem = asyncio.Semaphore(15) 
            
            async with aiohttp.ClientSession() as session:
                start_date_str = self.get_start_date(symbol)
                current = datetime.strptime(start_date_str, "%Y-%m-%d")
                today = datetime.now()
                
                dates = []
                while current <= today:
                    if current.weekday() < 5: dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)

                tasks = [self.fetch_day(session, symbol, d, sem) for d in dates]
                operations = []
                
                for f in asyncio.as_completed(tasks):
                    date, daily_data = await f
                    for record in daily_data:
                        # Final Safety Check: No 0-value records
                        if record["b_qty"] == 0 and record["s_qty"] == 0:
                            continue

                        doc = {
                            "stock": symbol, "date": date, "broker": record["broker"],
                            "b_qty": record["b_qty"], "s_qty": record["s_qty"],
                            "b_amt": record["b_amt"], "s_amt": record["s_amt"]
                        }
                        operations.append(UpdateOne(
                            {"stock": symbol, "date": date, "broker": record["broker"]},
                            {"$set": doc}, upsert=True
                        ))

                    if len(operations) >= 500:
                        res = self.target_collection.bulk_write(operations, ordered=False)
                        self.stats[symbol] += res.upserted_count + res.modified_count
                        operations = []

                if operations:
                    res = self.target_collection.bulk_write(operations, ordered=False)
                    self.stats[symbol] += res.upserted_count + res.modified_count

            duration = (datetime.now() - start_time).total_seconds()
            self.send_telegram(f"✅ *Sync Complete*\n📊 SGHC: {self.stats[symbol]} records\n⏱ Time: {duration:.1f}s")

        except Exception as e:
            self.send_telegram(f"❌ *Error*: {str(e)}")
            raise e

if __name__ == "__main__":
    asyncio.run(HeadlessFetcher().run_sync())
