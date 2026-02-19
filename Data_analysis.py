import json
import time
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ----------------------------
# CONFIG
# ----------------------------
DATA_PATH = "data/raw/ULHC.json"

st.set_page_config(
    page_title="NEPSE Chart Replay",
    layout="wide"
)

st.title("📈 NEPSE Chart Replay Engine")
st.caption("Price only. No hindsight. No excuses.")

# ----------------------------
# LOAD DATA
# ----------------------------
@st.cache_data
def load_data(path):
    with open(path, "r") as f:
        raw = json.load(f)

    df = pd.DataFrame({
        "time": pd.to_datetime(raw["t"], unit="s"),
        "open": raw["o"],
        "high": raw["h"],
        "low": raw["l"],
        "close": raw["c"],
        "volume": raw["v"]
    })

    return df.sort_values("time").reset_index(drop=True)

df = load_data(DATA_PATH)

# ----------------------------
# CONTROLS
# ----------------------------
col1, col2, col3 = st.columns(3)

with col1:
    max_candles = st.slider(
        "Visible candles",
        min_value=20,
        max_value=len(df),
        value=100,
        step=1
    )

with col2:
    speed = st.select_slider(
        "Replay speed (seconds per candle)",
        options=[0.1, 0.25, 0.5, 1.0, 2.0],
        value=0.5
    )

with col3:
    auto_play = st.toggle("▶ Auto Replay", value=False)

# ----------------------------
# REPLAY STATE
# ----------------------------
if "cursor" not in st.session_state:
    st.session_state.cursor = max_candles

if auto_play:
    if st.session_state.cursor < len(df):
        st.session_state.cursor += 1
        time.sleep(speed)
        st.rerun()
else:
    st.session_state.cursor = max_candles

visible_df = df.iloc[:st.session_state.cursor]

# ----------------------------
# CHART
# ----------------------------
fig = go.Figure()

fig.add_candlestick(
    x=visible_df["time"],
    open=visible_df["open"],
    high=visible_df["high"],
    low=visible_df["low"],
    close=visible_df["close"],
    name="Price"
)

fig.update_layout(
    height=600,
    xaxis_rangeslider_visible=False,
    template="plotly_dark",
    title=f"Candles shown: {len(visible_df)} / {len(df)}"
)

st.plotly_chart(fig, use_container_width=True)

# ----------------------------
# INFO PANEL
# ----------------------------
last = visible_df.iloc[-1]

st.markdown("### 📌 Current Candle")
st.write({
    "Date": str(last["time"]),
    "Open": last["open"],
    "High": last["high"],
    "Low": last["low"],
    "Close": last["close"],
    "Volume": last["volume"]
})
