import datetime
import time

import requests
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from tornado import concurrent
from tqdm import tqdm

from scraper.database.database import add_to_reverse_index, get_client
from scraper.entity.page_statisitcs import PageStatistics
from scraper.scraping.scraper import extract_content, url_is_valid
from scraper.scraping.urls_from_sitemaps import get_urls_from_domain
from scraper.utils.utils import get_domain

# TODO: problem scraping pages that are not pure html
# TODO: can't scrape PDFs

i = 0
headers = {
    'User-Agent': '*',
    'From': 'karl.gustav1789@gmail.com'
}

current_milli_time = lambda: int(round(time.time() * 1000))


def scrape(url, client):
    global i
    global headers

    current_time = datetime.datetime.utcnow()

    response_time_ms = current_milli_time()
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return None
    response.close()

    # TODO: support PDF crawling
    # TODO: support jpg and png

    image_formats = ("image/png", "image/jpeg", "image/jpg", "image/gif")
    if response.headers["content-type"] in image_formats:
        return None

    http_encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
    html_encoding = EncodingDetector.find_declared_encoding(response.content, is_html=True)
    encoding = html_encoding or http_encoding

    # TODO: html5lib is super, super, super, slow. fix this somehow

    soup = BeautifulSoup(response.content, 'html5lib',from_encoding=encoding)

    response_time_ms = current_milli_time() - response_time_ms
    # Extract the page info
    word_count, page = extract_content(url, soup, current_time, client)

    # Save results to DB
    if (page.title is not None) and (word_count is not None):
        try:
            page_id = page.add_page(client=client)

            page_statistics = PageStatistics(page_id=page_id,
                                             current_time=current_time,
                                             page=page,
                                             speed=response_time_ms,
                                             client=client)

            page_statistics.add_page_statistics(current_time=current_time,
                                                client=client)

            # Update the reverse index
            discovered_keywords = list(word_count.keys())
            add_to_reverse_index(discovered_keywords, page_id, client)

            new_domains = page.extract_domains_linked_domains(get_domain(page.url))
            i += 1
            # Get domains to scrape next
            return new_domains
        except Exception as e:
            print("Exception in adding results to the DB for:{}. Message: {}".format(url, e))

    else:
        print("Problem scraping: {}. Content type {}".format(url, response.headers["content-type"]))
    return None


def crawl(urls_to_scrape,domain):
    domains_found = []
    client = get_client()

    # Crawl the given urls
    if urls_to_scrape.shape[0] > 0:
        urls_to_scrape.sort()
        print("Starting to crawl {} urls for domain: {}".format(len(urls_to_scrape),domain))
        total = len(urls_to_scrape)
        pbar = tqdm(total=total)

        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
            results = {executor.submit(scrape, url, client): url for url in urls_to_scrape}
            for future in concurrent.futures.as_completed(results):
                url = results[future]
                try:
                    data = future.result()
                    if data is not None and len(data) > 0:
                        domains_found.extend(data)
                        pbar.update(1)
                    else:
                        pbar.update(1)
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))

        pbar.close()
    client.close()
    return list(set(domains_found))


def start_scraper():
    global i
    # Initial array of URLs to scrape
    urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/scraping/domains/test_domains.txt')]

    print("Starting to scrape {} domains".format(len(urls_to_scrape)))
    start_time = time.process_time()
    end_time = time.process_time()
    max_crawling_time_in_minutes = 180
    already_crawled = []

    # Crawl each url individually
    while len(urls_to_scrape) > 0:
        url = urls_to_scrape.pop()
        if url not in already_crawled and url_is_valid(url):
            print("Domains left {}".format(len(urls_to_scrape)))
            if ((end_time - start_time) / 60.0) < max_crawling_time_in_minutes:
                discovered_urls = get_urls_from_domain(url)
                if discovered_urls is not None and len(discovered_urls.shape) > 0:
                    new_domains = crawl(discovered_urls,url)  # crawl(discovered_urls)
                    urls_to_scrape.extend(new_domains)
                    print("Completed crawling {} pages for {}".format(i, url))
                else:
                    print("No urls found for {}".format(len(url)))
                i = 0
                end_time = time.process_time()
            else:
                urls_to_scrape = []
            already_crawled.append(url)

    print("Scraping done in {} minutes".format((end_time - start_time) / 60))
