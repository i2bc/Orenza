import gzip
import xml.etree.ElementTree as ET
import re
from Bio import SwissProt


file_path = "uniprot_sprot.dat.gz"


def decompress_file(input_file, output_file):
    with gzip.open(input_file, "rb") as f_in:
        with open(output_file, "wb") as f_out:
            f_out.write(f_in.read())


# Example usage:
file_output = "../data/uniprot_sprot.dat"
# decompress_file(file_path, file_output)


def parse_swiss(filename):
    """
    This function parse the data of dat file from uniprot

    Args:
        filename : The path to the file to be parsed

    Returns:
        dict: A dictionary where each key is an accession and the corresponding value is a dictionary
            containing extracted EC numbers and a boolean indicating their completeness.
    """
    with open(filename) as handle:
        data = {}

        for record in SwissProt.parse(handle):
            if "EC=" in record.description:
                accession = record.accessions[0]
                data[accession] = {}

                for desc in record.description:
                    if isinstance(desc, str):  # list of type unknown so need to check its type to use regex on it
                        ec_numbers = re.findall(r"EC=([\d.-]+)", desc)
                        data[accession]["ec_numbers"] = ec_numbers
                        if "-" in ec_numbers:
                            ec_complete = False
                            data[accession]["ec_complete"] = ec_complete
                        else:
                            ec_complete = True
                            data[accession]["ec_complete"] = ec_complete

    return data


def analyse_swiss(filename):
    with open(filename) as handle:
        for record in SwissProt.parse(handle):
            if record.accessions[0] == "P26670":
                for a in dir(record):
                    if not a.startswith("__"):
                        print("Attribute: ", a, "\n", getattr(record, a))
            if record.accessions[0] == "P26670":
                break


def parse_explorenz(filename):
    """
    This function parse the data of an xml file from parse_explorenz
    and extract the entry info of the ec number

    Args:
        filename : The path to the file to be parsed

    Returns:
        dict: A dictionary of dictionaries, where each top-level key is an EC number and
            the corresponding value is a dictionary containing its associated information.
    """
    tree = ET.parse(filename)
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
    return data


"""
----------------Test---------------
"""
# parse_swiss(file_output)
# analyse_swiss(file_output)

file_explore = "../data/enzyme-data.xml"
# data_explore = parse_explorenz(file_explore)
#
# for key in data_explore["1.1.1.1"]:
#    print("Cle : ", key, "\n", "Value: ", data_explore["1.1.1.1"][key])
