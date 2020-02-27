import random
import ssl

from pymongo import MongoClient, CursorType
from pymongo import DeleteOne
import numpy as np

username = "admin"
pw = "75o3eiompG4wGGVj"

client = MongoClient(
    "mongodb+srv://" + username + ":" + pw + "@gowizcluster0-wsbvt.mongodb.net/test?retryWrites=true&w=majority",
    ssl_cert_reqs=ssl.CERT_NONE, connect=False)
print("Connected to the client")
#db = client.get_database("Index")
print("Connected to the db")


def get_pages():
    global client
    db = client.get_database("Index")
    pages = []
    for d in db['pages'].find({}, {"_id": 1, "url": 1, "urls": 1}).sort([('$natural', 1)]):
        pages.append(d)
    return np.array(pages)

def get_page_rank_by_page_id(page_id):
    global client
    db = client.get_database("Analytics")
    page_statics = db['page_statistics']
    current_analytics_data = page_statics.find_one( {"_id": 0, "pageRank": 1},{"page_id": page_id})
    if current_analytics_data:
        return current_analytics_data['pageRank']
    return 0


def get_domain_id(domain, domain_obj):
    global client
    db = client.get_database("Index")
    domains = db['domains']
    current_domain_data = domains.find_one({"domain": domain})
    if current_domain_data:
        return current_domain_data["_id"]

    domain_data = {
        "domain": domain_obj.domain,
        "favicon": domain_obj.favicon,
        "first_crawl_UTC": domain_obj.first_crawl_UTC,
        "last_crawl_UTC": domain_obj.last_crawl_UTC,
        "domain_statistics_id": domain_obj.domain_statistics_id
    }
    domains.insert_one(domain_data)
    return get_domain_id(domain, domain_obj)


def add_page(page,current_time):
    global client
    db = client.get_database("Index")
    new_page = {
        "url": page.url,
        "title": page.title,
        "domain_id": page.domain_id,
        "meta": page.meta,
        "urls": page.urls,
        "first_crawl_UTC": page.first_crawl_UTC,
        "last_crawl_UTC": page.last_crawl_UTC
    }
    pages = db['pages']

    current_fingerprint = page.get_fingerprint()

    old_data = pages.find_one({"url": page.url})
    if old_data:
        raw_data = [old_data["url"], old_data["title"], old_data["meta"], old_data["urls"]]
        old_fingerprint = page.get_fingerprint_from_raw_data(raw_data)

        if old_fingerprint == current_fingerprint:
            return old_data["_id"]
        new_page['last_crawl_UTC'] = current_time
        pages.update({'_id': old_data['_id']}, {'$set': new_page})
        return old_data["_id"]
    else:
        pages.insert_one(new_page)
    return pages.find_one({"url": page.url})["_id"]


def add_page_statistics(page_stat,current_time):
    global client
    db = client.get_database("Analytics")
    new_page_statistics = {
        "page_id":page_stat.page_id,
        "update_date_UTC":current_time,
        "language": page_stat.language,
        "pageRank": page_stat.page_rank,
        "page_load_speed": page_stat.page_load_speed,
        "url_length": page_stat.url_length,
    }
    page_statistics = db['page_statistics']

    old_data = page_statistics.find_one({"page_id": page_stat.page_id})
    if old_data:
        #todo To we need to apdate
        page_statistics.update({'page_id': old_data['page_id']}, {'$set': new_page_statistics})
    else:
        page_statistics.insert_one(new_page_statistics)



def make_bulk_updates(results_from_db, page_id):
    global client
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


def make_bulk_inserts(keywords_missing_in_the_db, page_id):
    global client
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


def add_to_reverse_index(keywords, page_id):
    global client
    db = client.get_database("Index")
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


def bulk_update_pagerank(current_pages, pageranks,current_time):
    global client
    db = client.get_database("Analytics")
    counter = 0
    bulk = db['page_statistics'].initialize_unordered_bulk_op()

    for i in range(len(pageranks)):

        # process in bulk
        updates = {
            "update_date_UTC":current_time,
            "pageRank": pageranks[i]
        }
        bulk.find({"page_id": current_pages[i]["_id"]}).update({'$set': updates})
        counter += 1

        if counter % 1000 == 0:
            bulk.execute()
            bulk = db['page_statistics'].initialize_unordered_bulk_op()
            counter = 0

    if counter % 1000 != 0:
        bulk.execute()





def get_keywords():
    global client
    db = client.get_database("Index")
    keywords = []
    for d in db['reverse_index'].find({}, {"_id": 1, "keyword": 1}).sort([('$natural', 1)]):
        keywords.append(d)
    return np.array(keywords)


def delete_duplicate_keywords_from_db():
    global client
    db = client.get_database("Index")
    if random.random() < 0.02:  # we don't want the remove duplicates every time. It's too expensive
        print("Staring to look duplicates in the db")

        results_from_db = get_keywords()

        keywords_present_in_the_db = [entry["keyword"] for entry in results_from_db]

        duplicate_keyword_index = [i for i in range(len(keywords_present_in_the_db)) if
                                   not i == keywords_present_in_the_db.index(keywords_present_in_the_db[i])]

        if len(duplicate_keyword_index) > 0:
            print("Found", len(duplicate_keyword_index), "duplicate topics that will be deleted")
            duplicate_results = [results_from_db[index] for index in duplicate_keyword_index]

            duplicate_ids = [entry["_id"] for entry in duplicate_results]

            to_be_deleted = [DeleteOne({'_id': id}) for id in duplicate_ids]
            db['reverse_index'].bulk_write(to_be_deleted, ordered=False)
        else:
            print("No duplicated found")
