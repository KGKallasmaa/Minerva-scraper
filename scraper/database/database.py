import datetime
import ssl
import ciso8601 as ciso8601
from pymongo import MongoClient, CursorType
import numpy as np

import pytz

utc = pytz.UTC


def get_client():
    # todo use .env
    username = "admin"
    pw = "75o3eiompG4wGGVj"

    client = MongoClient(
        "mongodb+srv://" + username + ":" + pw + "@gowizcluster0-wsbvt.mongodb.net/test?retryWrites=true&w=majority",
        ssl_cert_reqs=ssl.CERT_NONE, connect=False)
    return client


def get_page_rank_by_page_id(page_id, client):
    db = client.get_database("Analytics")
    page_statics = db['page_statistics']
    current_analytics_data = page_statics.find_one({"_id": 0, "pageRank": 1}, {"page_id": page_id})
    if current_analytics_data:
        return current_analytics_data['pageRank']
    return 0


def get_domain_id(domain, domain_obj, current_time, client):
    db = client.get_database("Index")
    domains = db['domains']
    current_domain_data = domains.find_one({"domain": domain})

    if current_domain_data is not None:
        domain_data = {
            "last_crawl_UTC": current_time,
        }
        # We should not update to often
        one_hour_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
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
    db = client.get_database("Index")

    counter = 0
    bulk = db['reverse_index'].initialize_unordered_bulk_op()
    no_updates = True
    for present_entries in results_from_db:
        # process in bulk
        keyword = present_entries["keyword"]
        current_pages = present_entries["pages"]

        if page_id not in current_pages:
            current_pages.append(page_id)
            updates = {
                "pages": list(current_pages)
            }
            bulk.find({'keyword': keyword}).update({'$set': updates})
            counter += 1
            no_updates = False

        if counter % 500 == 0:
            bulk.execute()
            bulk = db['reverse_index'].initialize_unordered_bulk_op()
            counter = 0

    if not no_updates:
        if counter % 500 == 0:
            bulk.execute()


def make_bulk_inserts(keywords_missing_in_the_db, page_id, client):
    db = client.get_database("Index")
    counter = 0
    bulk = db['reverse_index'].initialize_unordered_bulk_op()
    no_updates = True
    for missing_keyword in keywords_missing_in_the_db:
        # process in bulk
        new_entry = {
            "keyword": missing_keyword,
            "pages": [page_id],
        }
        bulk.insert(new_entry)
        counter += 1
        no_updates = False

        if counter % 500 == 0:
            bulk.execute()
            bulk = db['reverse_index'].initialize_unordered_bulk_op()
            counter = 0

    if not no_updates:
        if counter % 500 == 0:
            bulk.execute()


def add_to_reverse_index(keywords, page_id, client):
    db = client.get_database("Index")
    # TODO: remove page from keyworrds if the page no loger has those keywords

    keywords_document = db['reverse_index']

    results_from_db = np.array(list(
        keywords_document.find({"keyword": {"$in": keywords}}, {"_id": 0, "keyword": 1, "pages": 1},
                               cursor_type=CursorType.EXHAUST)))

    # What keywords are missing?

    keywords_present_in_the_db = [entry["keyword"] for entry in results_from_db]

    keywords_missing_in_the_db = set(keywords) - set(keywords_present_in_the_db)

    make_bulk_inserts(keywords_missing_in_the_db, page_id, client)  # todo one thread will do it
    make_bulk_updates(results_from_db, page_id, client)  # todo. another thread will do that



def pages_we_will_not_crawl(url_lastmod, client):
    db = client.get_database("Index")
    pages = db['pages']
    current_data = pages.find({"url": {"$in": list(url_lastmod.keys())}}, {"_id": 0, "url": 1, "last_crawl_UTC": 1})

    pages_not_to_crawl = []

    for el in current_data:
        last_mod_date = url_lastmod.get(el["url"])
        if last_mod_date is not None:
            start_timestamp = ciso8601.parse_datetime(last_mod_date).replace(tzinfo=utc)
            end_timestamp = el['last_crawl_UTC'].replace(tzinfo=utc)
            if end_timestamp > start_timestamp:
                pages_not_to_crawl.append(el['url'])
    return set(pages_not_to_crawl)
