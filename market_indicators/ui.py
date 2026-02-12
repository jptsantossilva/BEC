import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

STATUS_TO_COLOR = {
"SAFE": "#16a34a",
"NEUTRAL": "#f59e0b",
"RISK": "#ef4444",
}

def status_badge(status: str):
    color = STATUS_TO_COLOR.get(status, "#64748b")
    st.markdown(
        f"""
        <span style="background:{color};color:white;padding:4px 8px;border-radius:6px;font-weight:600;">
        {status}
        </span>
        """,
        unsafe_allow_html=True,
    )

def score_meter(score: float, label: str = "Risk Score"):
    st.progress(score, text=f"{label}: {int(score*100)}%")

def sparkline(df, y="close", height=80):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df[y], mode="lines", name=y))
    fig.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=height, xaxis_visible=False, yaxis_visible=False)
    st.plotly_chart(fig)

def last_updated(dt: datetime):
    st.caption(f"Last updated: {dt.strftime('%Y-%m-%d %H:%M UTC')}")