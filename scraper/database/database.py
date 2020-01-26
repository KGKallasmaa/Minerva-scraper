import random
import ssl

from pymongo import MongoClient, CursorType
from datetime import datetime
from pymongo import DeleteOne
import numpy as np

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


def add_page(title, url, meta, favicon, urls):
    global pages
    if urls is None:
        urls = []

    new_page = {
        "title": " ".join(title.split(" ")[1:]),
        "page_url": url,  # the url that is used to acces the page
        "favicon": favicon,  # TODO: we should only save one favicon address pre domain
        "meta": meta,
        "urls": urls,  # the urls that the page links to
        "pageRank": 0,
        "crawled_time_UTC": datetime.utcnow()
    }
    pages = db['pages']

    # Is this new page?
    if pages.find_one({"page_url": url}):
        return pages.find_one({"page_url": url})["_id"]
    else:
        pages.insert_one(new_page)
        return pages.find_one({"page_url": url})["_id"]


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
                "pages": list(set(current_pages))
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

    results_from_db = np.array(list(keywords_document.find({"keyword": {"$in": keywords}}, {"_id": 0, "keyword": 1, "pages": 1}, cursor_type=CursorType.EXHAUST)))

    # What keywords are missing?

    keywords_present_in_the_db = [entry["keyword"] for entry in results_from_db]

    keywords_missing_in_the_db = set(keywords) - set(keywords_present_in_the_db)

    make_bulk_inserts(keywords_missing_in_the_db, page_id) #todo one thread will do it
    make_bulk_updates(results_from_db, page_id) # todo. another thread will do that


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
    if random.random() < 0.02: # we don't want the remove duplicates every time. It's too expensive
        print("Staring to look duplicates in the db")
        keywords_document = db['reverse_index']

        results_from_db = np.array(list(keywords_document.find({}, {"_id": 1, "keyword": 1}, cursor_type=CursorType.EXHAUST)))

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
