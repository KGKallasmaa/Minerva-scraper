
import pyhash

from scraper.utils.utils import get_domain, compress_urls, de_compress

fp = pyhash.farm_fingerprint_64()


class Page:
    def __init__(self, url, title, meta, divs, headings, domain_id, current_time, urls):
        self.url = url
        self.title = title
        self.domain_id = domain_id
        self.meta = meta
        self.urls = compress_urls(urls)
        self.first_crawl_UTC = current_time
        self.last_crawl_UTC = current_time

        self.divs = divs
        self.headings = headings

        self.heading1 = []
        self.heading2 = []
        self.heading3 = []


    def get_values_for_db(self):
        return {
            "url": self.url,
            "title": self.title,
            "domain_id": self.domain_id,
            "meta": self.meta,
            "urls": self.urls,
            "first_crawl_UTC": self.first_crawl_UTC,
            "last_crawl_UTC": self.last_crawl_UTC
        }

    def add_urls(self, urls):
        current_urls = de_compress(self.urls)
        current_urls.extend(urls)
        self.urls = compress_urls(list(set(current_urls)))

    def get_fingerprint(self):
        raw_data = [self.url, self.title, self.meta, self.urls, self.headings]
        return self.get_fingerprint_from_raw_data(raw_data)

    def get_fingerprint_from_raw_data(self, raw_data):
        string = ''.join(map(str, raw_data))
        return fp(string)

    def add_page(self, current_time, client):

        db = client.get_database("Index")
        new_page = self.get_values_for_db()
        pages = db['pages']

        current_fingerprint = self.get_fingerprint()

        old_data = pages.find_one({"url": self.url})

        if old_data:
            raw_data = [old_data["url"], old_data["title"], old_data["meta"], old_data["urls"]]
            old_fingerprint = self.get_fingerprint_from_raw_data(raw_data)

            if old_fingerprint == current_fingerprint:
                return old_data["_id"]
            new_page['last_crawl_UTC'] = current_time
            pages.update({'_id': old_data['_id']}, {'$set': new_page})
            return old_data["_id"]
        else:
            pages.insert_one(new_page)
        return pages.find_one({"url": self.url})["_id"]

    def extract_domains_linked_domains(self, domain):
        # Returns urls that are not from the same domain
        urls = de_compress(self.urls)

        if len(urls) < 1:
            return []

        url_filter = lambda url: domain not in url
        suitable_urls = list(filter(url_filter, urls))

        if len(suitable_urls) < 1:
            return []

        get_domain_from_url = lambda url: get_domain(url)
        domains = list(map(get_domain_from_url, suitable_urls))

        if len(domains) < 1:
            return []

        return list(set(domains))
