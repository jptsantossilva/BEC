import streamlit as st


DEFAULT_PAGE_TITLE = "BEC App"
DEFAULT_PAGE_ICON = "random"
DEFAULT_LAYOUT = "wide"
DEFAULT_MENU_ITEMS = {
    "Get Help": "https://github.com/jptsantossilva/BEC#readme",
    "Report a bug": "https://github.com/jptsantossilva/BEC/issues/new",
    "About": """# My name is BEC \n I am a Trading Bot and I'm trying to be an *extremely* cool app! 
        \n This is my dad's 🐦 Twitter: [@jptsantossilva](https://twitter.com/jptsantossilva).
        """,
}


def configure_page(page_title=DEFAULT_PAGE_TITLE):
    st.set_page_config(
        page_title=page_title,
        page_icon=DEFAULT_PAGE_ICON,
        layout=DEFAULT_LAYOUT,
        menu_items=DEFAULT_MENU_ITEMS,
    )
