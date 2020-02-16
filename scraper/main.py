import time

from bs4 import BeautifulSoup
import requests

from tornado import concurrent

from scraper.database.database import add_page, add_to_reverse_index, delete_duplicate_keywords_from_db
from scraper.language.language import nr_of_words_in_url
from scraper.scraping.scraper import extract_content

# TODO: implement functionality to crawl the urls that were discovered


# TODO: problem scraping pages that don't do server side rendering

from scraper.scraping.urls_from_sitemaps import get_sitemaps_from_url
from bs4 import BeautifulSoup

# MAIN FUNCTIONALITY
i = 0


def scrape(url):
    global i
    headers = {
        'User-Agent': '*',
        'From': 'karl.gustav1789@gmail.com'
    }
    current_milli_time = lambda: int(round(time.time() * 1000))

    response_time_ms = current_milli_time()

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        response_time_ms = current_milli_time() - response_time_ms
        # Extract the info
        content = extract_content(url, soup)

        # Save results to DB
        if content["title"] and content["word_count_dict"]:
            nr_of_words = nr_of_words_in_url(content["word_count_dict"])

            page_id = add_page(content["title"],
                               content["url"], content["meta"],
                               content["favicon"],
                               content["urls"],
                               nr_of_words,
                               response_time_ms)

            # Update the reverse index
            discovered_keywords = [*content["word_count_dict"].keys()]
            add_to_reverse_index(discovered_keywords, page_id)
            print("completed scraping for", url)
            i += 1
        else:
            print("Problem scraping ", url + ".")

    return None


def crawl(urls_to_scrape):
    if urls_to_scrape.shape[0] > 0:
        print("Starting to crawl", len(urls_to_scrape), "urls")
        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as x:
            x.map(scrape, urls_to_scrape)


def start_scraper():
    global i
    # Initial array of URLs to scrape
    urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/scraping/test_domains.txt')]

    print("Starting to scrape", len(urls_to_scrape), "domains")
    start_time = time.process_time()

    # Crawl each url individually
    for url in urls_to_scrape:
        discovered_urls = get_sitemaps_from_url(url)
        if discovered_urls is not None:
            crawl(discovered_urls)
            print("Completed crawling", i, "pages for", url)
        else:
            print("No urls found for", url)
        i = 0
        delete_duplicate_keywords_from_db()

    end_time = time.process_time()
    print("Scraping done in", (end_time - start_time) / 60, "minutes")
