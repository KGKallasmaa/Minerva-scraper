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


def add_page(title, url, meta):
    global db
    new_page = {
        "title": title,
        "url": url,
        "meta": meta
    }
    pages = db['pages']

    # Is this new page?
    if pages.find_one({"url": url}):
        return pages.find_one({"url": url})["_id"]
    else:
        pages.insert_one(new_page)
        return pages.find_one({"url": url})["_id"]


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

    keywords_document.insert_many(many_new_entries)
