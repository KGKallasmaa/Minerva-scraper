import re
import requests
from scraper.language import language
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib.robotparser as urobot
import ssl

rp = urobot.RobotFileParser()
ssl._create_default_https_context = ssl._create_unverified_context


def extract_content(url, soup):
    return_dictionary = \
        {
            "title": None,
            "meta": None,
            "headings": None,
            "divs": None,
            "urls": None,
            "word_count_dict": None
        }

    try:
        #   print("Original URL",url)
        #   print()
        # Get title
        return_dictionary["title"] = soup.title.string
        # print(return_dictionary["title"])

        # Get meta
        meta = [meta['content'] for meta in soup.findAll(attrs={"name": re.compile(r"description", re.I)})]
        if meta:
            return_dictionary["meta"] = meta[0]
        # Get all divs
        list_of_divs = soup.findAll('div')
        if list_of_divs:
            return_dictionary["divs"] = list_of_divs

        # Get all headings
        list_of_headings = [headlines.text.strip() for headlines in soup.find_all(re.compile('^h[1-6]$'))]
        if list_of_headings:
            return_dictionary["headings"] = list_of_headings

        # Get all urls
        urls = [link.get('href') for link in soup.find_all('a')]
        urls = list(filter(None, urls))
        add_prefit = lambda x: x if (("http://" in x) or ("https://" in x)) else url + x
        urls = set(map(add_prefit, urls))


        # Filter out pages we can vistit for a fiven domain
        domains_and_urls = dict()
        for urll in urls:
            if urll != url:
                domain = urlparse(urll).netloc
                domain = "http://" + domain if "http://" in url else "https://" + domain
                if domain not in domains_and_urls.keys():
                    domains_and_urls[domain] = robot_url_fetching_check(domain,urls)


        urls = [item for sublist in domains_and_urls.values() for item in sublist]
        urls = list(set(urls))
        if urls:
            return_dictionary["urls"] = urls
        # Get all word counts
        word_count = language.word_count(soup.get_text())
        #print(word_count)
        if word_count:
            return_dictionary["word_count_dict"] = word_count

    except Exception as e:
        print(e)

    return return_dictionary


def url_is_valid(url):
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None


def get_urls_from_xml(url):
    if url is None:
        return None
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    raw_urls = [link.get_text() for link in soup.find_all('loc')]

    extracted_urls = list(filter(lambda url: ".xml" not in url, raw_urls))
    to_be_crawled_urls = list(filter(lambda url: ".xml" in url, raw_urls))

    extracted_urls = extracted_urls + list(filter(lambda url: get_urls_from_xml(url), to_be_crawled_urls))

    return list(map(lambda url: url.replace('\n', '').replace(' ', ''), extracted_urls))


def get_urls_from_sitemap(url):
    domain = urlparse(url).netloc
    domain = "http://" + domain if "http://" in url else "https://" + domain

    response = requests.get(domain + "/robots.txt")
    soup = BeautifulSoup(response.text, "html.parser")
    lines = soup.prettify().split('\n')

    sitemaps = list(filter(lambda line: "Sitemap:" in line, lines))
    sitemaps = list(map(lambda line: line.replace('Sitemap: ', ''), sitemaps))

    if len(sitemaps) == 0:
        # Trying a common setup
        sitemaps.append(domain + "/sitemap.xml")

    extracted_urls = [list(filter(None, get_urls_from_xml(sitemap))) for sitemap in sitemaps]
    extracted_urls = [item for sublist in extracted_urls for item in sublist]
    return robot_url_fetching_check(domain, extracted_urls)


def robot_url_fetching_check(domain, urls):
    """
    TODO: add aditional support
    1. How many requests reuests can be made per seond?

    CHECK for resourses: https://docs.python.org/3/library/urllib.robotparser.html
    """
    forbidden_domains = ["http://analytics.google.com"]
    if domain in forbidden_domains:
        return []
    results = []
    try:
        global rp
        rp.set_url(domain + "/robots.txt")
        rp.read()
        return list(filter(lambda url: rp.can_fetch("*", url), urls))
    except Exception as e:
        pass
        # print("catching errors")
       # print(e)
    return results
