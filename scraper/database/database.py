import ssl

from pymongo import MongoClient

username = "admin"
pw = "syPQkR59Kst7ltg2"

client = MongoClient(
    "mongodb+srv://" + username + ":" + pw + "@gowizcluster0-wsbvt.mongodb.net/test?retryWrites=true&w=majority",
    ssl_cert_reqs=ssl.CERT_NONE, connect=False)
print("Connected to the client")
db = client.get_database("Index")
print("Connected to the db")


def get_pages():
    global db
    return list(db['pages'].find())


def add_page(title, url, meta, urls):
    global pages
    if urls is None:
        urls = []

    new_page = {
        "title": title,
        "page_url": url,  # the url that is used to acces the page
        "meta": meta,
        "urls": urls,  # the urls that the page links to
        "pageRank": 0
    }
    pages = db['pages']

    # Is this new page?
    if pages.find_one({"page_url": url}):
        return pages.find_one({"page_url": url})["_id"]
    else:
        pages.insert_one(new_page)
        return pages.find_one({"page_url": url})["_id"]


def add_to_reverse_index(keywords, page_id):
    keywords_document = db['reverse_index']
    many_new_entries = []
    for keyword in keywords:
        if keywords_document.find_one({"keyword": keyword}):
            current_pages = keywords_document.find_one({"keyword": keyword})["pages"]
            if page_id not in current_pages:
                current_pages.append(page_id)
                updates = {
                    "pages": current_pages
                }
                keywords_document.update_one({"keyword": keyword}, {"$set": updates})

        else:
            new_entry = {
                "keyword": keyword,
                "pages": [page_id],
            }
            many_new_entries.append(new_entry)

    if len(many_new_entries) > 0:
        keywords_document.insert_many(many_new_entries)


def update_pagrank(current_pages, pageranks):
    pages = db['pages']
    for i in range(len(pageranks)):
        updates = {
            "pageRank": pageranks[i]
        }
        pages.update_one({"_id": current_pages[i]["_id"]}, {"$set": updates})
