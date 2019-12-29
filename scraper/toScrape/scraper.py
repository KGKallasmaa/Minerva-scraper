import re
import requests
from scraper.language import language
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib.robotparser as urobot
import ssl

rp = urobot.RobotFileParser()
ssl._create_default_https_context = ssl._create_unverified_context


def get_text(soup):
    for s in soup(['script', 'style']):
        s.decompose()
    return ' '.join(soup.stripped_strings)


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
        # Get title
        return_dictionary["title"] = soup.title.string

        # Get meta
        meta = [meta['content'] for meta in soup.findAll(attrs={"name": re.compile(r"description", re.I)})]
        if meta:
            return_dictionary["meta"] = meta[0]

        #Get favicion #TODO

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


        # TODO:  Filter out pages only our pot can visit

        urls = list(set(urls))
        if urls:
            return_dictionary["urls"] = urls

        # Get all word counts
        word_count = language.word_count(get_text(soup))
        if word_count:
            return_dictionary["word_count_dict"] = word_count

    except Exception as e:
        pass

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

    if len(to_be_crawled_urls) > 0:
        extracted_urls += [get_urls_from_xml(urll) for urll in to_be_crawled_urls]

    return extracted_urls



def get_urls_from_sitemap(url):
    domain = urlparse(url).netloc
    domain = "http://" + domain if "http://" in url else "https://" + domain

    response = requests.get(domain + "/robots.txt")
    soup = BeautifulSoup(response.text, "html.parser")
    lines = soup.prettify().split('\n')

    sitemaps = list(filter(lambda line: "Sitemap:" in line, lines))
    sitemaps = list(map(lambda line: line.replace('Sitemap: ', ''), sitemaps))

    # Trying a common setup
    if len(sitemaps) == 0:
        sitemaps.append(domain + "/sitemap.xml")

    extracted_urls = [get_urls_from_xml(sitemap) for sitemap in sitemaps][0]

    extracted_urls = [item for sublist in extracted_urls for item in sublist]
    #TODO: implement.We should only crawl pages that we are allowed to
    return extracted_urls

def robot_url_fetching_check(domain, urls):
    """
    TODO: add aditional support
    1. How many requests reuests can be made per seond?
    CHECK for resourses: https://docs.python.org/3/library/urllib.robotparser.html
    """
    forbidden_domains = ["http://analytics.google.com"]
    if domain in forbidden_domains:
        return []
    global rp
    rp.set_url(domain + "/robots.txt")
    rp.read()
    return list(filter(lambda url: rp.can_fetch("*", url), urls))