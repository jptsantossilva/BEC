from bec.page_config import configure_page

configure_page()

from bec.dashboard import *

if __name__ == "__main__":
    set_authentication()
    set_pages()
    main()
