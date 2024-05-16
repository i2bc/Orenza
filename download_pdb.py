#!/usr/bin/env python3

import requests
import os
import concurrent.futures
from bs4 import BeautifulSoup
import utils


url = "https://files.rcsb.org/pub/pdb/data/structures/divided/xml/"


def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
    else:
        basename = os.path.basename(filename)
        print(f"Failed to download_file {basename}")


def get_subfolder(url: str):
    r = requests.get(url)
    html_data = r.content

    parsed_data = BeautifulSoup(html_data, "html.parser")

    links = parsed_data.find_all("a", href=True)
    folders = []
    for link in links:
        if link.get("href"):
            if len(link["href"]) == 3:  # Ensure that the link is of the format letter/digit letter/digit
                if link["href"] not in folders:
                    folders.append(link["href"])
    return folders


folders = get_subfolder(url)

download_output = "./data/pdb/"


def download_subfolder(base_url: str, output_path: str, folder: str):
    subfolder_url = os.path.join(base_url, folder)
    subfolder_name = folder[:-1]
    full_subfolder_name = os.path.join(output_path, subfolder_name)
    if not os.path.exists(full_subfolder_name):
        os.makedirs(full_subfolder_name)

    r = requests.get(subfolder_url)
    html_data = r.content
    parsed_data = BeautifulSoup(html_data, "html.parser")
    links = parsed_data.find_all("a", href=true)
    for link in links:
        if link.get("href"):
            if link["href"].endswith("xml.gz"):
                full_url = os.path.join(subfolder_url, link["href"])
                full_name = os.path.join(full_subfolder_name, link["href"])
                download_file(full_url, full_name)


i = 0
print(f"start the download of pdb at {utils.current_time()}")
for folder in folders:
    download_subfolder(url, download_output, folder)
    print(f"Count: {i}")
    i += 1
    if i >= 20:
        break
print(f"end the download of pdb at {utils.current_time()}")
# print(f"start the download of pdb at {utils.current_time()}")
# executor = concurrent.futures.ThreadPoolExecutor(max_workers=500)
# futures = [executor.submit(download_subfolder, url, download_output, folder) for folder in folders]
# concurrent.futures.wait(futures)
# print(f"end the download of pdb at {utils.current_time()}")
