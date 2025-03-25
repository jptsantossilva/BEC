from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import json
import re
from datetime import datetime
import os

import utils.config as config
import utils.telegram as telegram

BASE_URL = "https://www.binance.com"
ANNOUNCEMENTS_LIST_URL = "https://www.binance.com/en/support/announcement/list/161"

DIR = os.path.dirname(os.path.abspath(__file__))
BLACKLIST_FILE = os.path.join(DIR, "blacklisted_pairs.json")


def load_blacklist():
    try:
        with open(BLACKLIST_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_blacklist(blacklist):
    with open(BLACKLIST_FILE, "w") as f:
        json.dump(blacklist, f, indent=2)


def get_page_html(url, page):
    page.goto(url, timeout=60000)
    page.wait_for_load_state("networkidle")
    return page.content()


def get_announcement_links(page):
    html = get_page_html(ANNOUNCEMENTS_LIST_URL, page)
    soup = BeautifulSoup(html, "html.parser")
    links = []

    # Find the <h2> tag that contains "Delisting"
    delisting_header = soup.find("h2", string=lambda s: s and "delisting" in s.lower())

    if not delisting_header:
        print("⚠️ Could not find 'Delisting' header.")
        return links

    # Navigate up to the parent container that holds the links
    container = delisting_header.find_parent("div")

    if not container:
        print("⚠️ Could not find parent container for delisting section.")
        return links

    # Now search for all <a> tags inside this container only
    for a in container.find_all("a", href=True):
        href = a["href"]
        if "/en/support/announcement/detail/" in href:
            full_url = BASE_URL + href
            links.append(full_url)

    return links

def extract_trading_pairs_from_announcement(url, page):
    
    html = get_page_html(url, page)
    soup = BeautifulSoup(html, "html.parser")

    # Get Published Date
    date_div = soup.find("div", class_="typography-subtitle2")
    published_date = None
    if date_div:
        date_str = date_div.get_text(strip=True)
        try:
            published_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        except ValueError:
            print(f"⚠️ Could not parse date string: {date_str}")

    # Extract Trading Pairs
    # content = ' '.join(soup.stripped_strings)

    # Extract content inside <div id="support_article">
    article_div = soup.find("div", id="support_article")
    if not article_div:
        print("⚠️ Could not find <div id='support_article'> in announcement.")
        return [], published_date

    content = ' '.join(article_div.stripped_strings)

    pairs = set(re.findall(r'\b[A-Z]{1,5}/[A-Z]{3,5}\b', content))
    pairs |= set(re.findall(r'\b[A-Z]{4,10}\b', content))

    allowed_quotes = ('USDT', 'USDC', 'BTC')

    filtered_pairs = [
        pair for pair in pairs
        if '/' in pair and any(pair.endswith(f"/{quote}") for quote in allowed_quotes)
    ]

    return sorted(filtered_pairs), published_date

def main():
    print(f"🔍 Checking Binance delisting announcements at {datetime.now()}")

    delisting_start_date = config.get_setting("delisting_start_date")
    start_date = datetime.fromisoformat(delisting_start_date)

    blacklist = load_blacklist()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        links = get_announcement_links(page)

        for link in links:
            if link in blacklist:
                continue

            msg_new_delist = f"New Delisting Announcement found on Binance:\n{link}" 
            print(msg_new_delist)

            pairs, published_date = extract_trading_pairs_from_announcement(link, page)

            # Always record the announcement, even if no pairs are found
            blacklist[link] = {
                "date_checked": str(datetime.now()),
                "published_date": str(published_date),
                "pairs": pairs
            }

            ## save on every loop iteration to increase robustness. cases if gets interrupted for some reason
            save_blacklist(blacklist)

            if not published_date:
                print(f"⚠️ Could not get published date for {link}, skipping.")
                continue

            if published_date < start_date:
                print(f"⏩ Skipping old announcement ({published_date}): {link}")
                continue
            
            if pairs:
                print(f"🔴 Found delisted pairs: {pairs}")

                pairs_string = ', '.join(pairs)

                msg = f"{msg_new_delist}\nDelisting Pairs: {pairs_string}"
                telegram.send_telegram_message(telegram.telegram_token_signals, telegram.EMOJI_INFORMATION, msg)
            else:
                print("⚠️ No valid trading pairs found in this announcement.")

        browser.close()

    save_blacklist(blacklist)
    print("✅ Blacklist check completed.")


if __name__ == "__main__":
    main()
