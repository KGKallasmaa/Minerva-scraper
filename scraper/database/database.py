import random
import ssl

from pymongo import MongoClient, CursorType
from datetime import datetime
from pymongo import DeleteOne
import numpy as np

from scraper.scraping.scraper import get_initial_domain_content, calculate_fingerprint

username = "admin"
pw = "75o3eiompG4wGGVj"

client = MongoClient(
    "mongodb+srv://" + username + ":" + pw + "@gowizcluster0-wsbvt.mongodb.net/test?retryWrites=true&w=majority",
    ssl_cert_reqs=ssl.CERT_NONE, connect=False)
print("Connected to the client")
db = client.get_database("Index")
print("Connected to the db")


def get_pages():
    # TODO: make it more efficient
    return np.array(list(db['pages'].find({}, {"_id": 1, "page_url": 1, "urls": 1}, cursor_type=CursorType.EXHAUST)))



def add_page(title, url, meta, favicon, urls, nr_of_words, response_time_ms):
    if urls is None:
        urls = []

    current_time = datetime.utcnow()
    domain_id = add_domain(current_time, url, favicon)

    new_page = {
        "title": title.strip(),
        "domain_id": domain_id,
        "favicon": favicon,  # TODO: we should only save one favicon address pre domain
        "meta": meta,
        "urls": urls,  # the urls that the page links to
        "nr_of_words": nr_of_words,
    }
    pages = db['pages']

    fingerprint = calculate_fingerprint(new_page)

    new_page["fingerprint"] = fingerprint
    new_page["page_url"] = url
    new_page["last_crawl_time_UTC"] = current_time
    new_page["response_time_ms"] = response_time_ms

    # Is this new page?
    if pages.find_one({"page_url": url}):
        current_page_data = pages.find_one({"page_url": url})
        # Should we update the page?
        if current_page_data["fingerprint"] == fingerprint:
            return current_page_data["_id"]
        new_page["pageRank"] = current_page_data["pageRank"]
        pages.update({'_id': current_page_data['_id']}, {'$set': new_page})

    else:
        new_page["pageRank"] = 0
        pages.insert_one(new_page)
    return pages.find_one({"page_url": url})["_id"]


def add_domain(current_time, url, favicon):
    domains = db['domains']
    domain_data = get_initial_domain_content(url, current_time, favicon)
    domain = domain_data['domain_url']

    if domains.find_one({"domain_url": domain}):
        current_domain_data = domains.find_one({"domain_url": domain})
        domain_data['first_crawl_time_UTC'] = current_domain_data['first_crawl_time_UTC']
        domains.update({'_id': current_domain_data['_id']}, {'$set': domain_data})
        return current_domain_data["_id"]
    else:
        domains.insert_one(domain_data)
    return domains.find_one({"domain_url": domain})["_id"]


def make_bulk_updates(results_from_db, page_id):
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


def make_bulk_inserts(keywords_missing_in_the_db, page_id):
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


def add_to_reverse_index(keywords, page_id):
    # TODO: remove page from keyworrds if the page no loger has those keywords

    keywords_document = db['reverse_index']

    results_from_db = np.array(list(
        keywords_document.find({"keyword": {"$in": keywords}}, {"_id": 0, "keyword": 1, "pages": 1},
                               cursor_type=CursorType.EXHAUST)))

    # What keywords are missing?

    keywords_present_in_the_db = [entry["keyword"] for entry in results_from_db]

    keywords_missing_in_the_db = set(keywords) - set(keywords_present_in_the_db)

    make_bulk_inserts(keywords_missing_in_the_db, page_id)  # todo one thread will do it
    make_bulk_updates(results_from_db, page_id)  # todo. another thread will do that


def bulk_update_pagerank(current_pages, pageranks):
    counter = 0
    bulk = db['pages'].initialize_unordered_bulk_op()

    for i in range(len(pageranks)):
        # process in bulk

        updates = {
            "pageRank": pageranks[i]
        }
        bulk.find({"_id": current_pages[i]["_id"]}).update({'$set': updates})
        counter += 1

        if counter % 1000 == 0:
            bulk.execute()
            bulk = db['pages'].initialize_unordered_bulk_op()
            counter = 0

    if counter % 1000 == 0:
        bulk.execute()


def delete_duplicate_keywords_from_db():
    if random.random() < 0.02:  # we don't want the remove duplicates every time. It's too expensive
        print("Staring to look duplicates in the db")
        keywords_document = db['reverse_index']

        results_from_db = np.array(
            list(keywords_document.find({}, {"_id": 1, "keyword": 1}, cursor_type=CursorType.EXHAUST)))

        keywords_present_in_the_db = [entry["keyword"] for entry in results_from_db]

        duplicate_keyword_index = [i for i in range(len(keywords_present_in_the_db)) if
                                   not i == keywords_present_in_the_db.index(keywords_present_in_the_db[i])]

        if len(duplicate_keyword_index) > 0:
            print("Found", len(duplicate_keyword_index), "duplicate topics that will be deleted")
            duplicate_results = [results_from_db[index] for index in duplicate_keyword_index]

            duplicate_ids = [entry["_id"] for entry in duplicate_results]

            to_be_deleted = [DeleteOne({'_id': id}) for id in duplicate_ids]
            keywords_document.bulk_write(to_be_deleted, ordered=False)
        else:
            print("No duplicated found")
