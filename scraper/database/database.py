import ssl
from datetime import datetime, timedelta

import ciso8601 as ciso8601
import pytz
from pymongo import MongoClient

from scraper.utils.utils import execute_tasks

utc = pytz.UTC


def get_client():
    # todo use .env
    username = "admin"
    pw = "75o3eiompG4wGGVj"

    client = MongoClient(
        "mongodb+srv://" + username + ":" + pw + "@gowizcluster0-wsbvt.mongodb.net/test?retryWrites=true&w=majority",
        ssl_cert_reqs=ssl.CERT_NONE, connect=False)
    return client


def get_domain_id(domain, domain_obj, current_time, client):
    db = client.get_database("Index")
    domains = db['domains']
    current_domain_data = domains.find_one({"domain": domain},
                                           {"_id": 1, "domain": 1, "last_crawl_UTC": 1, "ssl_is_present": 1})

    if current_domain_data is not None:
        domain_data = {
            "last_crawl_UTC": current_time,
        }
        # We should not update to often
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        if current_domain_data["last_crawl_UTC"] > one_hour_ago:
            return current_domain_data["_id"]

        ssl_is_present = "https://" in domain
        if ssl_is_present != current_domain_data["ssl_is_present"]:
            domain_data['ssl_is_present'] = ssl_is_present
        domains.update({'_id': current_domain_data["_id"]}, {'$set': domain_data})
        return current_domain_data["_id"]

    domain_data = domain_obj.get_values_for_db()

    domains.insert_one(domain_data)
    return domains.find_one({"domain": domain})["_id"]


def make_bulk_updates(results_from_db, page_id, client):
    if len(results_from_db) < 1:
        return None
    db = client.get_database("Index")

    counter = 0
    bulk = db['reverse_index'].initialize_unordered_bulk_op()
    tasks = []

    for present_entries in results_from_db:
        # process in bulk
        keyword = present_entries["keyword"]
        current_pages = present_entries["pages"]

        index = dict((y, x) for x, y in enumerate(current_pages))
        page_id_present = index.get(page_id, None)

        if page_id_present is not None:
            current_pages.append(page_id)
            updates = {
                "pages": current_pages
            }
            bulk.find({'keyword': keyword}).update({'$set': updates})
            counter += 1

        if counter % 1000 == 0:
            tasks.append(bulk)
            bulk = db['reverse_index'].initialize_unordered_bulk_op()
            counter = 0

    if counter % 1000 != 0:
        tasks.append(bulk)

    execute_tasks(tasks)


def make_bulk_inserts(keywords_missing_in_the_db, page_id, client):
    if len(keywords_missing_in_the_db) < 1:
        return None
    db = client.get_database("Index")
    counter = 0
    bulk = db['reverse_index'].initialize_unordered_bulk_op()

    tasks = []

    for missing_keyword in keywords_missing_in_the_db:
        new_entry = {
            "keyword": missing_keyword,
            "pages": [page_id],
        }
        bulk.insert(new_entry)
        counter += 1

        if counter % 1000 == 0:
            tasks.append(bulk)
            bulk = db['reverse_index'].initialize_unordered_bulk_op()
            counter = 0

    if counter % 1000 != 0:
        tasks.append(bulk)

    execute_tasks(tasks)


def add_to_reverse_index(keywords, page_id, client):
    db = client.get_database("Index")

    keywords_document = db['reverse_index']

    results_from_db = []
    for d in keywords_document.find({"keyword": {"$in": keywords}}, {"_id": 0, "keyword": 1, "pages": 1}).sort(
            [('$natural', 1)]):
        results_from_db.append(d)

    # What keywords are missing?

    keywords_present_in_the_db = [entry["keyword"] for entry in results_from_db]

    keywords_missing_in_the_db = set(keywords) - set(keywords_present_in_the_db)

    make_bulk_inserts(keywords_missing_in_the_db, page_id, client)  # todo one thread will do it
    make_bulk_updates(results_from_db, page_id, client)  # todo. another thread will do that


def pages_we_will_not_crawl(url_lastmod, client):
    db = client.get_database("Index")
    pages = db['pages']

    # No page is crawled more then once per hour
    one_hour_from_now = datetime.utcnow() + timedelta(hours=1)

    current_data = []

    for e in pages.find({"url": {"$in": list(url_lastmod.keys())}, "last_crawl_UTC": {"$lt": one_hour_from_now}},
                        {"_id": 0, "url": 1, "last_crawl_UTC": 1}).sort([('$natural', 1)]):
        current_data.append(e)

    if len(current_data) < 1:
        return []

    pages_not_to_crawl = [None] * len(current_data)

    for i in range(len(current_data)):
        current_url = current_data[i]["url"]

        last_mod_date = url_lastmod.get(current_url)  # this data was present in the sitemap of the pate
        last_crawl_time = current_data[i]["last_crawl_UTC"]
        if last_mod_date is not None and last_crawl_time is not None:
            start_timestamp = ciso8601.parse_datetime(last_mod_date).replace(tzinfo=utc)
            last_crawl_time = last_crawl_time.replace(tzinfo=utc)
            if last_crawl_time > start_timestamp:
                pages_not_to_crawl[i] = current_url

    return list(filter(None, pages_not_to_crawl))
