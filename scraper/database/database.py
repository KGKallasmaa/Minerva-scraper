import ssl

from pymongo import MongoClient

username = "admin"
pw = "ES5UFn-_b9v-ue7"

print("Connecting to the client")
client = MongoClient(
    "mongodb+srv://" + username + ":" + pw + "@gowizcluster0-wsbvt.mongodb.net/test?retryWrites=true&w=majority",
    ssl_cert_reqs=ssl.CERT_NONE, connect=False)
print("Connection made")
print("Connecting to the db")
db = client.get_database("Index")
print("Connection made")


def add_page(title, url):
    global db
    new_page = {
        "title": title,
        "url": url,
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
    for keyword in keywords:
        # TODO: implement updaating
        if keywords_document.find_one({"keyword": keyword}):
            current_pages = keywords_document.find_one({"keyword": keyword})["pages"]
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
            keywords_document.insert_one(new_entry)
