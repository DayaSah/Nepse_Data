import os
import asyncio
import aiohttp
import requests
import traceback
from datetime import datetime
from pymongo import MongoClient, UpdateOne

class DailyUpdater:
    def __init__(self):
        self.mongo_uri = os.environ.get("MONGO_URI")
        self.tele_token = os.environ.get("TELEGRAM_TOKEN")
        self.tele_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        self.client = MongoClient(self.mongo_uri, tlsAllowInvalidCertificates=True)
        self.db = self.client["StockHoldingByTMS"]
        self.trades_col = self.db["market_trades"]
        self.price_col = self.db["Stock_Price_Volume"]

        self.headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://chukul.com/"}
        self.today = datetime.now().strftime('%Y-%m-%d')

    def send_telegram(self, message):
        if not self.tele_token: return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        requests.post(url, json={"chat_id": self.tele_chat_id, "text": message, "parse_mode": "Markdown"})

    async def fetch_stock(self, session, symbol, sem):
        url_b = f"https://chukul.com/api/data/top-buy/?symbol={symbol}&from_date={self.today}&to_date={self.today}"
        url_s = f"https://chukul.com/api/data/top-sell/?symbol={symbol}&from_date={self.today}&to_date={self.today}"
        async with sem:
            try:
                async with session.get(url_b, headers=self.headers) as rb, \
                           session.get(url_s, headers=self.headers) as rs:
                    b_json = await rb.json() if rb.status == 200 else []
                    s_json = await rs.json() if rs.status == 200 else []
                    
                    merged = {}
                    for r in (b_json if isinstance(b_json, list) else []):
                        b = str(r.get("buyer", "")).strip()
                        if b and b != "0":
                            merged[b] = {"broker": b, "b_qty": int(float(r.get("quantity", 0))), "b_amt": float(r.get("amount", 0)), "s_qty": 0, "s_amt": 0.0}
                    
                    for r in (s_json if isinstance(s_json, list) else []):
                        s = str(r.get("seller", "")).strip()
                        if s and s != "0":
                            if s not in merged:
                                merged[s] = {"broker": s, "b_qty": 0, "b_amt": 0.0, "s_qty": int(float(r.get("quantity", 0))), "s_amt": float(r.get("amount", 0))}
                            else:
                                merged[s]["s_qty"] = int(float(r.get("quantity", 0)))
                                merged[s]["s_amt"] = float(r.get("amount", 0))
                    return symbol, list(merged.values())
            except: return symbol, []

    async def run(self):
        stocks = self.price_col.distinct("Stock")
        self.send_telegram(f"🚀 *Daily Update*: Processing {len(stocks)} stocks...")
        
        sem = asyncio.Semaphore(20)
        ops = []
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_stock(session, s, sem) for s in stocks]
            for f in asyncio.as_completed(tasks):
                symbol, results = await f
                for r in results:
                    if r["b_qty"] == 0 and r["s_qty"] == 0: continue
                    doc = {"stock": symbol, "date": self.today, **r}
                    ops.append(UpdateOne({"stock": symbol, "date": self.today, "broker": r["broker"]}, {"$set": doc}, upsert=True))
                
                if len(ops) >= 1000:
                    self.trades_col.bulk_write(ops, ordered=False)
                    ops = []
        
        if ops: self.trades_col.bulk_write(ops, ordered=False)
        self.send_telegram(f"✅ *Daily Update Complete* for {self.today}")

if __name__ == "__main__":
    asyncio.run(DailyUpdater().run())
