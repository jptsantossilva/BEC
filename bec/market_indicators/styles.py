import streamlit as st


def inject_base_css():
    st.markdown(
        """
        <style>
        .card {
        border: 1px solid #e5e7eb;
        border-radius: 14px;
        padding: 14px 16px;
        background: white;
        box-shadow: 0 1px 2px rgba(0,0,0,0.03);
        }
        .card h3 { margin: 0 0 6px 0; font-size: 1.05rem; }
        .muted { color: #6b7280; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 12px; }
        </style>
        """,
        unsafe_allow_html=True,
    )