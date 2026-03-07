# 🌌 NEPSE Quantum Matrix V2

The **NEPSE Quantum Matrix** is a high-performance, multiversal stock market intelligence terminal designed to track broker behavior, detect market manipulation, and visualize liquidity flows within the Nepal Stock Exchange (NEPSE).

## 🚀 Key Features

- **📡 Master Matrix Sync:** All data (Auto-fetched or Manual) is consolidated into a single MongoDB collection for unified analysis.
- **🕵️ Broker Intelligence:** Deep-dive into specific TMS nodes (e.g., TMS 58) to see their total inventory, trading style, and "Matched Trade" counterparties.
- **📈 Technical Terminal:** Candlestick charts with EMA crossovers, Bollinger Bands, and Volume profiles powered by Plotly.
- **🔮 AI Predictor:** Algorithmic scanning for wash-trading detection and high-probability accumulation setups.
- **💉 Data Injector:** Multi-mode data entry (Auto-fetch, Manual JSON, or Cookie-Hijack) to bypass Cloudflare protection.

## 🛠️ System Architecture

```text
Nepse_Data/
├── Data_analysis.py       <-- Master Controller (Login & Nav)
├── Tabs/
│   ├── dashboard.py       <-- High-level Market Overview
│   ├── stock_analysis.py  <-- Leaderboard of Brokers per Stock
│   ├── tms_analysis.py    <-- Portfolio & Behavior of specific TMS
│   ├── stock_graph.py     <-- Advanced Candlestick Charts
│   ├── predictor.py       <-- Manipulation & Signal Detection
│   ├── data_injector.py   <-- The Data Entry Point
│   └── Nepse_Terminal.py  <-- Advanced Raw Data View
