import re
from scraper.language import language
from urllib.parse import urlparse
import urllib.robotparser as urobot
import ssl

rp = urobot.RobotFileParser()
ssl._create_default_https_context = ssl._create_unverified_context


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


def extract_content(url, soup):
    return_dictionary = \
        {
            "title": None,
            "meta": None,
            "favicon": None,
            "headings": None,
            "divs": None,
            "urls": None,
            "word_count_dict": None
        }

    try:
        # Get title
        return_dictionary["title"] = soup.title.string.capitalize()

        # Get meta
        meta = [meta['content'] for meta in soup.findAll(attrs={"name": re.compile(r"description", re.I)})]
        if meta:
            return_dictionary["meta"] = meta[0]

        # Get favicon
        return_dictionary["favicon"] = get_favicon(url, soup)
        # Get canonical
        canonical = get_canoncial(soup)
        if canonical:
            url = canonical

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