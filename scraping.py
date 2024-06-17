import requests
import re
from bs4 import BeautifulSoup
import utils
from urllib.parse import urlparse


def kegg(url: str, output_file: str, logger):
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
    parsed_url = urlparse(url)
    list_elements = parsed_data.find_all(class_="list")
    data = {}
    base_url = parsed_url.scheme + "://" + parsed_url.netloc
    for list_element in list_elements:
        # Find the previous sibling that is a div
        pathway_class = list_element.find_previous_sibling("b")
        pattern = r"^\d+\.\d+ (.+)"
        match = re.match(pattern, pathway_class.text)
        if match:
            pathway_class_name = match.group(1)
        links = list_element.find_all("a", href=True)
        for link in links:
            if link["href"].startswith("/pathway/"):
                r_path = requests.get(base_url + link["href"])
                html_pathway = r_path.content
                parsed_pathway = BeautifulSoup(html_pathway, "html.parser")
                rects = parsed_pathway.find_all(shape="rect")
                pathway = link["href"].split("/")[2]
                for rect in rects:
                    pattern = r"\d+\.\d+\.\d+\.\d+"
                    if rect.get("title"):
                        match = re.search(pattern, rect["title"])
                        if match:
                            ec_number = match.group()
                            if pathway_class_name not in data:
                                data[pathway_class_name] = {}
                            if pathway not in data[pathway_class_name]:
                                data[pathway_class_name][pathway] = []
                            if ec_number not in data[pathway_class_name][pathway]:
                                data[pathway_class_name][pathway].append(ec_number)
    utils.save_pickle(data=data, output_file=output_file, logger=logger)
