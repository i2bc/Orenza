import requests
import re
from bs4 import BeautifulSoup
import utils

r = requests.get("https://www.genome.jp/kegg/pathway.html")
html_data = r.content

parsed_data = BeautifulSoup(html_data, "html.parser")


def kegg(url: str, output_file: str):
    links = parsed_data.find_all("a", href=True)
    data = {}
    for link in links:
        if link["href"].startswith("/pathway/"):
            base_url = url
            r_path = requests.get(base_url + link["href"])
            html_pathway = r_path.content
            parsed_pathway = BeautifulSoup(html_pathway, "html.parser")
            rects = parsed_pathway.find_all(shape="rect")
            pathway = link["href"].split("/")[2]
            data[pathway] = []
            for rect in rects:
                pattern = r"\d+\.\d+\.\d+\.\d+"
                if rect.get("title"):
                    match = re.search(pattern, rect["title"])
                    if match:
                        ec_number = match.group()
                        data[pathway].append(ec_number)

        for key in data:
            print(data[key])
    utils.save_pickle(data=data, output_file=output_file)
