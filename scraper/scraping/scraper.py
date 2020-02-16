import re
from scraper.language import language
from urllib.parse import urlparse
import urllib.robotparser as urobot
import ssl
import pyhash
import zlib

rp = urobot.RobotFileParser()
ssl._create_default_https_context = ssl._create_unverified_context
fp = pyhash.farm_fingerprint_64()


def get_text(soup):
    for s in soup(['script', 'style']):
        s.decompose()
    return ' '.join(soup.stripped_strings)


def get_favicon(url, soup):
    domain = urlparse(url).netloc
    domain = "http://" + domain if "http://" in url else "https://" + domain

    icon_link = soup.find("link", rel="shortcut icon")
    if icon_link is None:
        icon_link = soup.find("link", rel="icon")
    if icon_link is None:
        return domain + '/favicon.ico'
    result = icon_link["href"]

    if url_is_valid(result):
        return result
    return domain + result


def get_canoncial(soup):
    canonical = soup.find('link', {'rel': 'canonical'})
    return canonical['href']


def get_urls(url, soup):
    urls = [link.get('href') for link in soup.find_all('a')]
    urls = list(filter(None, urls))
    add_prefit = lambda x: x if (("http://" in x) or ("https://" in x)) else url + x

    return list(set(map(add_prefit, urls)))


def extract_content(url, soup):
    # The main function

    # Get title and initialise the
    return_dictionary = {"title": soup.title.string.capitalize(),
                         "meta": None,
                         "url":url,
                         "favicon": None,
                         "headings": None,
                         "divs": None,
                         "urls": None,
                         "word_count_dict": None,
                         }

    # Get meta
    meta = [meta['content'] for meta in soup.findAll(attrs={"name": re.compile(r"description", re.I)})]
    if meta:
        return_dictionary["meta"] = meta[0]

    # Get favicon
    return_dictionary["favicon"] = get_favicon(url, soup)

    # Get canonical
    canonical = get_canoncial(soup)
    if canonical:
        return_dictionary["url"] = canonical

    # Get all divs
    list_of_divs = soup.findAll('div')
    if list_of_divs:
        return_dictionary["divs"] = list_of_divs

    # Get all headings
    list_of_headings = [headlines.text.strip() for headlines in soup.find_all(re.compile('^h[1-6]$'))]
    if list_of_headings:
        return_dictionary["headings"] = list_of_headings

    # Get all urls
    urls = get_urls(url, soup)
    if urls:
        return_dictionary["urls"] = compress_urls(urls)

    # Get all keywords and their count
    word_count = language.word_count(get_text(soup))
    if word_count:
        return_dictionary["word_count_dict"] = word_count


    return return_dictionary


def calculate_fingerprint(page_data):
    values_as_str =''.join('{}{}'.format(key, val) for key, val in page_data.items())
    rest = fp(values_as_str)
    return rest

def compress_urls(list_of_urls):
    list_of_urls.sort()
    my_string = ','.join(map(str, list_of_urls)).encode()
    return zlib.compress(my_string, 2)

def de_compress(compressed_string):
    decoded = zlib.decompress(compressed_string).decode("utf-8")
    return decoded.split(",")

def get_initial_domain_content(url, current_time, favicon):
    domain = urlparse(url).netloc
    domain = "http://" + domain if "http://" in url else "https://" + domain

    domain_data = {
        "domain_url": domain,
        "favicon": favicon,
        "is_secure": "https" in domain,
        "last_crawl_time_UTC": current_time,
        "first_crawl_time_UTC": current_time,
    }
    return domain_data


def url_is_valid(url):
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None
