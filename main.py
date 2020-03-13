import threading

from scraper.main import start_scraper


def scraper():
    start_scraper()


if __name__ == '__main__':


    threads = []

    ## Starting the scraper

    thread1 = threading.Thread(target=scraper)
    threads.append(thread1)


    for t in threads:
        t.start()