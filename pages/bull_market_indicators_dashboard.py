import streamlit as st
from binance.client import Client

from market_indicators.registry import get_registry
from market_indicators.data import get_OHLCV
from market_indicators.styles import inject_base_css
from market_indicators.ui import status_badge, score_meter, sparkline, last_updated


st.set_page_config(page_title="Bull Market Peak – Dashboard", layout="wide")


inject_base_css()

st.header("Bull Market Peak Indicators - Sell At The Top")

st.title("Bull Market Peak Indicators")
st.caption("Consolidated view of cycle-top risk signals across multiple indicators.")


symbol = st.sidebar.selectbox("Symbol", ["BTCUSDT"], index=0)
lookback_days = st.sidebar.slider("Lookback (days)", 365, 4000, 2000, step=30)


ohlc = get_OHLCV(symbol=symbol,interval=Client.KLINE_INTERVAL_1DAY)


registry = get_registry()


st.markdown("<div class='grid'>", unsafe_allow_html=True)
for key, meta in registry.items():
    payload = meta.compute_fn(ohlc)
    with st.container():
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        cols = st.columns([3, 2])
        with cols[0]:
            st.markdown(f"<h3>{meta.name}</h3>", unsafe_allow_html=True)
            st.caption(meta.description)
            status_badge(payload["status"])
            score_meter(payload["score"]) # 0..1
        with cols[1]:
            sparkline(payload["df"], y="close")
            if payload.get("last_signal") is not None:
                st.caption(f"Last signal: {payload['last_signal'].date()}")
                last_updated(payload["last_updated"])

        link_cols = st.columns([1,1])
        with link_cols[0]:
           st.page_link("pages/pi_cycle_top.py", label="Open detail", icon="📄") if key=="pi_cycle_top" else None
        with link_cols[1]:
            if meta.source_url:
                st.link_button("Source", meta.source_url)


        st.markdown("</div>", unsafe_allow_html=True)


st.markdown("</div>", unsafe_allow_html=True)