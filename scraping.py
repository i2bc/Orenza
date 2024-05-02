import requests
import re
from bs4 import BeautifulSoup

r = requests.get("https://www.genome.jp/kegg/pathway.html")
html_data = r.content

parsed_data = BeautifulSoup(html_data, "html.parser")

links = parsed_data.find_all("a", href=True)
# print(parsed_data.prettify())
for link in links:
    if link["href"].startswith("/pathway/"):
        base_url = "https://www.genome.jp"
        r_path = requests.get(base_url + link["href"])
        html_pathway = r_path.content
        parsed_pathway = BeautifulSoup(html_pathway, "html.parser")
        rects = parsed_pathway.find_all(shape="rect")
        for rect in rects:
            pattern = r"[0-9]+\.[0-9]\.+[0-9]+\.[0-9]+"
            if rect.get("title"):
                match = re.search(pattern, rect["title"])
                if match:
                    print(match.group())
