import requests
import re
from bs4 import BeautifulSoup
import utils
from urllib.parse import urlparse


def kegg(url: str, output_file: str):
    """
    Scrape the kegg pages containing the pathway and go through each pages to get all the ec ec_number
    contained in the rectangle (html shape)
    Args:
        url: main page contained the pathway
        output_file: name and location of the result
    """

    r = requests.get(url)
    html_data = r.content
    parsed_data = BeautifulSoup(html_data, "html.parser")
    links = parsed_data.find_all("a", href=True)
    data = {}
    parsed_url = urlparse(url)

    base_url = parsed_url.scheme + "://" + parsed_url.netloc
    print(base_url)
    for link in links:
        if link["href"].startswith("/pathway/"):
            r_path = requests.get(base_url + link["href"])
            html_pathway = r_path.content
            parsed_pathway = BeautifulSoup(html_pathway, "html.parser")
            rects = parsed_pathway.find_all(shape="rect")
            pathway = link["href"].split("/")[2]
            data[pathway] = []
            print(f"Pathway: {pathway}")
            for rect in rects:
                pattern = r"\d+\.\d+\.\d+\.\d+"
                if rect.get("title"):
                    match = re.search(pattern, rect["title"])
                    if match:
                        ec_number = match.group()
                        if ec_number not in data[pathway]:
                            data[pathway].append(ec_number)
    for key in list(data.keys()):
        # Check if the value corresponding to the key is an empty list
        if isinstance(data[key], list) and not data[key]:
            # Remove the key-value pair from the data
            print(key)
            del data[key]
    utils.save_pickle(data=data, output_file=output_file)


# url = "https://www.genome.jp/kegg/pathway.html"
# output_file = "./data/kegg.pickle"
# print(f"start the scraping of kegg at {utils.current_time()}")
# kegg(url, output_file)
# print(f"end of the scraping of kegg at {utils.current_time()}")
