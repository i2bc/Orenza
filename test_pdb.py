#!/usr/bin/env python
import os
import yaml
import download
import utils
import parse
import populate
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import gzip
import re
from time import process_time
from glob import glob
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed


def pdb_get_subfolder(url: str):
    """
    Get a list of the links (url) on the page
    Args:
        url: link to the page

    """
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


def pdb_download_subfolder(base_url: str, output_path: str, folder: str):
    """
    download all the content contained in the specified subfolder
    Args:
        base_url: the page containing the subfolder
        output_path: path where the folders will be download
        folder: name of the folder to download
    """
    subfolder_url = os.path.join(base_url, folder)
    subfolder_name = folder[:-1]
    full_subfolder_name = os.path.join(output_path, subfolder_name)
    if not os.path.exists(full_subfolder_name):
        os.makedirs(full_subfolder_name)

    r = requests.get(subfolder_url)
    html_data = r.content
    parsed_data = BeautifulSoup(html_data, "html.parser")
    links = parsed_data.find_all("a", href=True)
    for link in links:
        if link.get("href"):
            if link["href"].endswith("xml.gz"):
                full_url = os.path.join(subfolder_url, link["href"])
                full_name = os.path.join(full_subfolder_name, link["href"])
                download.https(full_url, full_name)


def subfolder_list(root_dir: str):
    subfolder = glob(f"{root_dir}/*")
    return subfolder


def analyze_subfolder(subfolder: str, output_file: str):
    files = glob(f"{subfolder}/*")
    data = {}
    for file in files:
        pdb(file, data)
    output_file += "/" + os.path.basename(subfolder)
    utils.save_pickle(data, output_file)


def pdb_iterate(root_dir: str, output_file: str):
    data = {}
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            full_path = os.path.join(root, file)
            pdb(full_path, data)

    print(data)
    utils.save_pickle(data, output_file)


def threading_pdb_iterate(root_dir: str, output_file: str):
    threads = []
    data = {}
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            full_path = os.path.join(root, file)
            thread = threading.Thread(target=pdb, args=(full_path,))
            threads.append(thread)
            thread.start()
    for thread in threads:
        thread.join()
    print(data)
    utils.save_pickle(data, output_file)


# Precompile regular expressions
pattern_pdb_id = re.compile(r"^<PDBx:datablock\s+datablockName=\"(\w+)\"")
pattern_ec_number = re.compile(r"<PDBx:pdbx_ec>(\d+\.\d+\.\d+\.\d+)")
pattern_sp_id = re.compile(r"<PDBx:pdbx_db_accession>(\w+)<\/PDBx:pdbx_db_accession>")


def multiprocessing_pdb_iterate(root_dir: str, output_file: str):
    data = {}
    with ProcessPoolExecutor() as executor:
        futures = []
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                full_path = os.path.join(root, file)
                futures.append(executor.submit(pdb, full_path))

        for future in as_completed(futures):
            result = future.result()
            for ec_number, values in result.items():
                if ec_number in data:
                    data[ec_number].extend(values)
                else:
                    data[ec_number] = values

    utils.save_pickle(data, output_file)


def pdb(input_file: str):
    result = {}
    with gzip.open(input_file, "rb") as f:
        lines = f.readlines()
        ec_number = ""
        uniprot_id = ""
        pdb_id = ""
        ec_found = False
        for line in lines:
            line_decoded = line.decode()
            match = pattern_pdb_id.search(line_decoded)
            if match:
                pdb_id = match.group(1)

            match = pattern_ec_number.search(line_decoded)
            if match and not ec_found:
                ec_number = match.group(1)
                ec_found = True

            match = pattern_sp_id.search(line_decoded)
            if match:
                uniprot_id = match.group(1)

        if ec_number:
            result[ec_number] = [(pdb_id, uniprot_id)]
    return result


with open("./config.yaml", "r") as yaml_file:
    config = yaml.load(yaml_file, Loader=yaml.FullLoader)

output_folder = config["output"]
database = config["database"]
pdb_url = config["pdb"]["url"]
pdb_subfolder_path = os.path.join(output_folder, "pdb")
# TODO: readd data to folder path
pdb_pickle = os.path.join(output_folder, "pdb.pickle")
pdb_worker = config["pdb"]["worker"]

t_start = process_time()
print(f"Start of pdb parsing at {utils.current_time()}")
parse.multiprocessing_pdb_iterate(pdb_subfolder_path, pdb_pickle)
t_end = process_time()
print(f"Elapsed time: {t_start}, {t_end}")
print(f"Elapsed time for parsing: {t_end-t_start}")
