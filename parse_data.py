import gzip
import xml.etree.ElementTree as ET
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
    """
    with open(filename) as handle:
        data = {}
        for record in SwissProt.parse(handle):
            if "EC=" in record.description:
                entry_name = record.entry_name
                # TODO need all accession number ?
                accession = record.accessions[
                    0
                ]  # get the first accession number, get all ?
                data[accession] = {}
                data[accession]["entry_name"] = entry_name
    return data


# parse_swiss(file_output)


def parse_explorenz(filename):
    """
    This function parse the data of an xml file from parse_explorenz
    and extract the entry info of the ec number

    Args:
        filename : The path to the file to be parsed

    Returns:
        return a dict of a dict containing the info of each ec
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
