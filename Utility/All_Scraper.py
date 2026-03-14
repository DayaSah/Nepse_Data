import os
import asyncio
import aiohttp
import random
import requests
import traceback
import json
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

class AllScraper:
    def __init__(self):
        # 1. Database & Secrets Setup
        self.mongo_uri = os.environ.get("MONGO_URI")
        self.tele_token = os.environ.get("TELEGRAM_TOKEN")
        self.tele_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        if not self.mongo_uri:
            raise ValueError("MONGO_URI secret is missing!")

        self.client = MongoClient(self.mongo_uri, tlsAllowInvalidCertificates=True)
        self.db = self.client["StockHoldingByTMS"]
        self.trades_col = self.db["market_trades"]
        self.detail_col = self.db["Stock_Detail"]
        self.price_col = self.db["Stock_Price_Volume"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://chukul.com/"
        }

    def send_telegram(self, message):
        """Sends alerts and progress updates."""
        if not self.tele_token or not self.tele_chat_id: return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        payload = {"chat_id": self.tele_chat_id, "text": message, "parse_mode": "Markdown"}
        try: requests.post(url, json=payload, timeout=10)
        except: pass

    async def get_and_save_stock_info(self, session, symbol):
        """Fetches listed_date and saves full metadata to Stock_Detail."""
        url = f"https://chukul.com/api/stock/?search={symbol}"
        try:
            async with session.get(url, headers=self.headers, timeout=15) as res:
                if res.status == 200:
                    data = await res.json()
                    for stock in data:
                        if stock.get("symbol") == symbol:
                            listed_date = stock.get("listed_date")
                            # Save full info to Stock_Detail
                            self.detail_col.update_one(
                                {"symbol": symbol},
                                {"$set": {"symbol": symbol, "listed_date": listed_date, "metadata": stock}},
                                upsert=True
                            )
                            return listed_date
        except Exception as e:
            print(f"Error fetching info for {symbol}: {e}")
        return "2023-01-01" # Safe fallback

    async def fetch_day(self, session, symbol, date_str, sem):
        """Fetches Buy and Sell data for a single day concurrently."""
        url_buy = f"https://chukul.com/api/data/top-buy/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        url_sell = f"https://chukul.com/api/data/top-sell/?symbol={symbol}&from_date={date_str}&to_date={date_str}"
        
        async with sem:
            try:
                # Concurrent requests for Buy and Sell of the same day
                async with session.get(url_buy, headers=self.headers, timeout=12) as rb, \
                           session.get(url_sell, headers=self.headers, timeout=12) as rs:
                    
                    b_data = await rb.json() if rb.status == 200 else []
                    s_data = await rs.json() if rs.status == 200 else []

                    daily_map = {}

                    # Parse Buys
                    for r in b_data:
                        bid = str(r.get("buyer", "")).strip()
                        if not bid or bid == "0": continue
                        daily_map[bid] = {
                            "b_qty": int(float(r.get("quantity", 0))),
                            "b_amt": float(r.get("amount", 0.0)),
                            "s_qty": 0, "s_amt": 0.0
                        }

                    # Parse Sells
                    for r in s_data:
                        sid = str(r.get("seller", "")).strip()
                        if not sid or sid == "0": continue
                        if sid not in daily_map:
                            daily_map[sid] = {"b_qty": 0, "b_amt": 0.0, "s_qty": 0, "s_amt": 0.0}
                        daily_map[sid]["s_qty"] = int(float(r.get("quantity", 0)))
                        daily_map[sid]["s_amt"] = float(r.get("amount", 0.0))

                    return date_str, daily_map
            except:
                return date_str, None

    async def process_stock(self, session, symbol):
        """Logic for whole-life fetching, Smart Comparison, and Saving."""
        # 1. Get History Start & Existing Data
        listed_date_str = await self.get_and_save_stock_info(session, symbol)
        
        # Load current records from DB into memory for $O(1)$ comparison
        existing = {}
        cursor = self.trades_col.find({"stock": symbol}, {"date": 1, "broker": 1, "b_qty": 1, "s_qty": 1})
        for r in cursor:
            existing[f"{r['date']}_{r['broker']}"] = {"b": r.get("b_qty", 0), "s": r.get("s_qty", 0)}

        # 2. Prep Dates
        start = datetime.strptime(listed_date_str, "%Y-%m-%d")
        curr, end = start, datetime.now()
        dates = []
        while curr <= end:
            if curr.weekday() not in [4, 5]: # Skip Friday, Saturday (NEPSE)
                dates.append(curr.strftime("%Y-%m-%d"))
            curr += timedelta(days=1)

        # 3. Fetch Days
        sem = asyncio.Semaphore(25) # High speed fire
        tasks = [self.fetch_day(session, symbol, d, sem) for d in dates]
        
        ops = []
        saved_count = 0
        
        for f in asyncio.as_completed(tasks):
            date_str, daily_map = await f
            if not daily_map: continue

            for broker, data in daily_map.items():
                # Condition: Skip if both are 0
                if data["b_qty"] == 0 and data["s_qty"] == 0:
                    continue

                # --- SMART COMPARISON ---
                key = f"{date_str}_{broker}"
                if key in existing:
                    old = existing[key]
                    b_diff = abs(old["b"] - data["b_qty"])
                    s_diff = abs(old["s"] - data["s_qty"])

                    if b_diff > 1000 or s_diff > 1000:
                        self.send_telegram(f"🚨 *High Diff Alert*\n{symbol} | {date_str} | B{broker}\nDiff: B:{b_diff} S:{s_diff}\n_Data skipped to protect existing info._")
                        continue
                    else:
                        # Less than 1000 difference? Skip (Keep your existing data)
                        continue

                # If record doesn't exist, prepare for Insert
                doc = {
                    "stock": symbol, "date": date_str, "broker": broker,
                    "b_qty": data["b_qty"], "s_qty": data["s_qty"],
                    "b_amt": data["b_amt"], "s_amt": data["s_amt"]
                }
                ops.append(UpdateOne({"stock": symbol, "date": date_str, "broker": broker}, {"$set": doc}, upsert=True))

                if len(ops) >= 1000:
                    res = self.trades_col.bulk_write(ops, ordered=False)
                    saved_count += (res.upserted_count + res.modified_count)
                    ops = []

        if ops:
            res = self.trades_col.bulk_write(ops, ordered=False)
            saved_count += (res.upserted_count + res.modified_count)

        self.send_telegram(f"📊 *{symbol} Sync Complete*\nNew/Updated Records: {saved_count}")

    async def run(self):
        start_run = datetime.now()
        self.send_telegram("🚀 *Full Scraper Engine Started*")

        try:
            # Dynamically get all unique stocks already in your Price database
            stocks = self.price_col.distinct("Stock")
            if not stocks:
                self.send_telegram("⚠️ No stocks found in Price database!")
                return

            async with aiohttp.ClientSession() as session:
                for i, symbol in enumerate(stocks):
                    try:
                        await self.process_stock(session, symbol)
                    except Exception as e:
                        self.send_telegram(f"❌ *Error in {symbol}*: `{str(e)[:100]}`")

                    # RANDOM COOLDOWN between stocks
                    if i < len(stocks) - 1:
                        sleep_time = random.randint(10, 20)
                        await asyncio.sleep(sleep_time)

            duration = (datetime.now() - start_run).total_seconds() / 60
            self.send_telegram(f"🏁 *Scraper Finished All Stocks*\nTotal Duration: {duration:.1f} minutes")

        except Exception as e:
            self.send_telegram(f"❌ *Fatal Error*: `{str(e)}`")
            print(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(AllScraper().run())
