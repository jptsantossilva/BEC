import streamlit as st


DEFAULT_PAGE_TITLE = "BEC App"
DEFAULT_PAGE_ICON = ":material/currency_bitcoin:"
DEFAULT_LAYOUT = "wide"
APP_LOGO = "static/bec-logo.svg"
DEFAULT_MENU_ITEMS = {
    "Get Help": "https://github.com/jptsantossilva/BEC#readme",
    "Report a bug": "https://github.com/jptsantossilva/BEC/issues/new",
    "About": """# My name is BEC \n I am a Trading Bot and I'm trying to be an *extremely* cool app! 
        \n This is my dad's 🐦 Twitter: [@jptsantossilva](https://twitter.com/jptsantossilva).
        """,
}
WIDE_MODE_CSS = """
<style>
    .block-container,
    [data-testid="stMainBlockContainer"] {
        max-width: none !important;
        width: 100% !important;
    }

    /* Keep the app details reachable at the bottom of the sidebar on both
       desktop and short mobile viewports. The sidebar itself remains the
       scroll container when the navigation is taller than the screen. */
    [data-testid="stSidebarContent"] {
        display: flex;
        flex-direction: column;
    }

    [data-testid="stSidebarUserContent"] {
        flex: 1 0 auto;
    }

    [data-testid="stSidebarUserContent"] > [data-testid="stVerticalBlock"] {
        min-height: 100%;
    }

    .st-key-bec_sidebar_footer {
        position: sticky;
        bottom: 0;
        z-index: 1;
        margin-top: auto;
        padding-top: 0.75rem;
        padding-bottom: max(0.75rem, env(safe-area-inset-bottom));
        background: var(--secondary-background-color);
    }
</style>
"""


def configure_page(page_title=DEFAULT_PAGE_TITLE):
    st.set_page_config(
        page_title=page_title,
        page_icon=DEFAULT_PAGE_ICON,
        layout=DEFAULT_LAYOUT,
        menu_items=DEFAULT_MENU_ITEMS,
    )
    # configure_page() runs for every selected page, so the logo is sent on
    # every Streamlit rerun instead of only when bec.dashboard is imported.
    st.logo(APP_LOGO, size="large", icon_image=DEFAULT_PAGE_ICON)
    st.markdown(WIDE_MODE_CSS, unsafe_allow_html=True)
