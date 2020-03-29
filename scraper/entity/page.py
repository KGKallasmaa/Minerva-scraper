from scraper.utils.utils import get_domain, compress_urls, de_compress, get_fingerprint_from_raw_data
from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateOne


class Page:
    def __init__(self, url, title, meta, divs, headings, domain_id, current_time, urls, client):
        self.url = url
        self.title = title
        self.meta = meta
        self.url_is_canonical = True

        self.domain_id = domain_id

        self.first_crawl_UTC = self.get_first_crawl_time_by_url(url, current_time, client)
        self.last_crawl_UTC = current_time

        self.divs = divs
        self.headings = headings
        self.urls = compress_urls(urls)

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
        raw_data = [self.url, self.title, self.meta, self.urls, self.urls]
        return get_fingerprint_from_raw_data(raw_data)

    def add_page(self, client):

        db = client.get_database("Index")
        new_page = self.get_values_for_db()
        pages = db['pages']

        old_data = pages.find_one({"url": self.url}, {"url": 1, "_id": 0, "meta": 1, "urls": 1})

        if old_data:
            raw_data = [old_data["url"], old_data["title"], old_data["meta"], old_data["urls"]]
            old_fingerprint = get_fingerprint_from_raw_data(raw_data)
            current_fingerprint = self.get_fingerprint()

            if old_fingerprint == current_fingerprint:
                return old_data["_id"]
            # TODO: move it to a seprete insert file
            pages.bulk_write([UpdateOne({'_id': old_data['_id']}, {'$set': new_page})], ordered=False)

            return old_data["_id"]
        else:
            pages.bulk_write([InsertOne(new_page)], ordered=False)

        return pages.find_one({"url": self.url}, {"_id": 1})["_id"]

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

    def get_first_crawl_time_by_url(self, url, current_time, client):

        db = client.get_database("Index")
        pages = db['pages']

        current_page = pages.find_one({"url": url}, {"_id": 0, "first_crawl_UTC": 1})

        if current_page:
            return current_page.get('first_crawl_UTC')
        return current_time
