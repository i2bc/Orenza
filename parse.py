#!/usr/bin/env python

import os
import pickle
import gzip
import xml.etree.ElementTree as ET
import re
from Bio import SwissProt


def gunzip_file(input_file: str, output_file: str):
    """
    This function decompress a .gz file and write it as a decompressed file
    Args:
        input_file : path to the file to be decompressed
        output_file: path and name to the decompressed file
    """
    try:
        with gzip.open(input_file, "rb") as f_in:
            with open(output_file, "wb") as f_out:
                f_out.write(f_in.read())
    except Exception as e:
        print(f"Error occurred while trying to unzip: {input_file}, error: {e}")


def save_pickle(data: dict, output_file: str):
    """
    This function stores a dictionary of parsed data as a pickle file.
    Args:
        data : a dictionary containing the data
        output_file : name and path of the output file
        path : the path where you want the file to be stored
    """
    try:
        with open(output_file, "wb") as f:
            pickle.dump(data, f)
    except Exception as e:
        print(f"Error occurred while saving pickle file: {e}")


def load_pickle(input_file: str):
    """
    This function loads a pickle file.
    Args:
        input_file: Name and path of the pickle file.
    Returns:
        The data loaded from the pickle file.
    """
    try:
        with open(input_file, "rb") as f:
            data = pickle.load(f)
        return data
    except Exception as e:
        print(f"Error occurred while loading pickle file: {e}")
        return None


def uniprot(input_file: str, output_file: str):
    """
    This function parse the data of dat file from uniprot

    Args:
        input_file : Name and path of the file to be parsed
        output_file : Name and path of the output file

    Returns:
        dict: A dictionary where each key is an accession and the corresponding value is a dictionary
            containing extracted EC numbers and a boolean indicating their completeness.
    """
    with open(input_file) as handle:
        data = {}

        for record in SwissProt.parse(handle):
            if "EC=" in record.description:
                accession = record.accessions[0]
                data[accession] = {}
                ec_numbers = []

                if isinstance(record.description, str):
                    ec_numbers = re.findall(r"EC=([\d.-]+)", record.description)
                    listEc = []
                    already_listed = False
                    for ec in ec_numbers:
                        for tup in listEc:  # Prevent adding multiple time the same ec number in the tuple
                            if ec in tup:
                                already_listed = True
                                continue
                        if already_listed:
                            continue
                        if "-" in ec:
                            ec_complete = False
                            listEc.append((ec, ec_complete))
                        else:
                            ec_complete = True
                            listEc.append((ec, ec_complete))
                    data[accession]["ec_numbers"] = listEc
    save_pickle(data, output_file)

    return data


def explorenz(input_file, output_file):
    """
    This function parse the data of an xml file from parse_explorenz
    and extract the entry info of the ec number

    Args:
        input_file : The path to the file to be parsed
        output_file : Name and path of the output file


    Returns:
        dict: A dictionary of dictionaries, where each top-level key is an EC number and
            the corresponding value is a dictionary containing its associated information.
    """
    tree = ET.parse(input_file)
    root = tree.getroot()
    data = {}  # will contain the ecc and a subdictionary with his parameter
    for table_data in root.findall("database/table_data[@name='entry']"):
        for row in table_data.findall("row"):
            ec_num = row.find("field[@name='ec_num']")  # get the current_ec of the row
            if ec_num is not None:
                data[ec_num.text] = {}
                for field in row.findall("field"):
                    if field.attrib["name"] != "ec_num":
                        data[ec_num.text][field.attrib["name"]] = field.text
    save_pickle(data, output_file)

    return data


"""
----------------Test---------------
"""


def analyse_swiss(filename):
    with open(filename) as handle:
        for record in SwissProt.parse(handle):
            if record.accessions[0] == "P26670":
                for a in dir(record):
                    if not a.startswith("__"):
                        print("Attribute: ", a, "\n", getattr(record, a))
            if record.accessions[0] == "P26670":
                break


def type_swiss(dataPickle):
    with open(dataPickle, "rb") as f:
        i = 0
        data = pickle.load(f)
        for key in data:
            print("data[key]: ", data[key])
            print(type(data[key]["ec_numbers"]))
            i += 1
            if i > 1000:
                break


sprot_path = "/home/demonz/programmation/stage/orenza/af2_web/bioi2server/orenza/data/uniprot_sprot.dat"
explorenz_path = "/home/demonz/programmation/stage/orenza/af2_web/bioi2server/orenza/data/enzyme-data.xml"

# parse_swiss(sprot_path)
# parse_explorenz(explorenz_path)
# load_pickle(explorenz_path)
# type_swiss("../data/pickle/swiss.pickle")


# analyse_swiss(file_output)

# file_explore = "../data/enzyme-data.xml"
# data_explore = parse_explorenz(file_explore)
#
# for key in data_explore["1.1.1.1"]:
#    print("Cle : ", key, "\n", "Value: ", data_explore["1.1.1.1"][key])
