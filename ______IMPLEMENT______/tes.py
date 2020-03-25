import requests
from bs4 import BeautifulSoup
import re

headers = {
    'User-Agent': '*',
    'From': 'karl.gustav1789@gmail.com'
}

# todo: finish robots.txt designing

def robot_url_fetching_check(domain, urls):
    """
    TODO: add aditional support
    1. How many requests reuests can be made per seond?
    CHECK for resourses: https://docs.python.org/3/library/urllib.robotparser.html
    """

    """"




    forbidden_domains = ["http://analytics.google.com"]
    if domain in forbidden_domains:
        return []
    global rp
    rp.set_url(domain + "/robots.txt")
    rp.read()
    print("hi")
    print(rp)
    res = list(filter(lambda url: rp.can_fetch("*", url), urls))
    print(res)
    return  urls

    """
    # Return values
    request_rate = 10  # 10 request per unit
    request_seconds = 60  # unit of account (60s).
    to_be_crawled_domains = urls.copy()

    default_response = request_rate, request_seconds, to_be_crawled_domains
    no_crawling = 0, 0, []

    # Nr of request per second 60/10

    robots_url = domain + "/robots.txt"
    response = requests.get(robots_url, headers=headers)

    # Step 1: check response status
    if response.status_code != 200:
        return no_crawling

    # Step 2: Find my disallowed urls
    soup = BeautifulSoup(response.text, "html.parser")
    lines = soup.text.split('\n')

    current_useragent_is_asterix = False

    not_allowed_baths = []
    for row in lines:
        if "User-Agent:" in row or "User-agent:" in row:
            if not current_useragent_is_asterix:
                if "User-Agent: *" in row or "User-agent: *" in row:
                    current_useragent_is_asterix = True

            else:
                if "User-agent: *" in row or "User-agent: *" in row:
                    current_useragent_is_asterix = False
        if current_useragent_is_asterix:
            if "Disallow:" in row or "Disallow: " in row:
                if "Disallow: *" in row or "Disallow:*" in row:
                    return no_crawling

                not_allowed_path = row.replace("Disallow:", "").strip()
                not_allowed_baths.append(not_allowed_path)



    print("printing baths",not_allowed_baths)


    test_url = "https://brilliant.org/profile/bog/followers"
    rex = "/profile/*/followers"
    x = re.search(rex, test_url)
    print(x)

    # print(response.text)

    #    rp = urllib.robotparser.RobotFileParser()
    #    rp.set_url(robots_url)
    #   rp.read()
    #   rrate = rp.request_rate("*")
    #  if rrate is not None:
    #     print("Number of request can be crawled in ", rrate.seconds, " seconds is", rrate.requests)
    # print("Crawl delay", rp.crawl_delay("*"))

    # res = list(filter(lambda url: rp.can_fetch("*", url), urls))
    # print("pages we can crawl")
    # print(res)

    return None


if __name__ == '__main__':
    domain = "https://brilliant.org"
    urls = ["https://brilliant.org", "https://brilliant.org/about/#our-mission", "https://brilliant.org/principles/",
            "https://brilliant.org/courses/"]
    print(robot_url_fetching_check(domain, urls))
