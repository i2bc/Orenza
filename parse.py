#!/usr/bin/env python

from concurrent.futures import ProcessPoolExecutor, as_completed
import gzip
import tarfile
import xml.etree.ElementTree as ET
import re
import os
from io import StringIO
from html.parser import HTMLParser
import utils


# from https://stackoverflow.com/questions/753052/strip-html-from-strings-in-python
class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, data):
        self.text.write(data)

    def get_data(self):
        return self.text.getvalue()


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def gunzip_file(input_file: str, output_file: str, logger, block_size=65536):
    """
    This function decompress a .gz file and write it as a decompressed file
    Args:
        input_file : path to the file to be decompressed
        output_file: path and name to the decompressed file
    """
    if not os.path.exists(input_file):
        logger.error(f"Input file does not exist: {input_file}")
        return

    try:
        with gzip.open(input_file, "rb") as f_in:
            with open(output_file, "wb") as f_out:
                while True:
                    block = f_in.read(block_size)
                    if not block:
                        break
                    f_out.write(block)
        logger.info(f"Decompression complete: {output_file}")
    except FileNotFoundError:
        logger.exception(f"File not found: {input_file}")
    except OSError as e:
        logger.exception(f"OS error occurred: {str(e)}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {str(e)}")


def extract_tar(input_file: str, output_folder: str):
    with tarfile.open(input_file, "r") as f:
        f.extractall(output_folder,filter='data')


# based on this https://web.expasy.org/docs/userman.html from 27/03/2024
def read_uniprot(input_file: str):
    """
    Read the uniprot field and yield of proteins containing ec numbers

    Args:
        input_file: name and path of the input file
    Yield:
        yield protein containing atleast one ec number
    """
    with open(input_file, "r") as f:
        line = f.readline()
        current = ""
        contain_ec = False
        while line:
            current += line
            if line.startswith("DE"):
                if "EC=" in line:
                    contain_ec = True

            if line.startswith("//"):
                if contain_ec:
                    yield current
                current = ""
                contain_ec = False
            line = f.readline()


def uniprot(input_file: str, output_file: str, logger):
    """
    Parse the data of uniprot.dat type of file

    Args:
        input_file: name and path to the input file
        output_file: name and path of the parsed data in pickle format
    """
    data = {}
    for query in read_uniprot(input_file):
        ec_list = []
        lines = query.splitlines()
        first_accession_line = True
        for line in lines:
            if first_accession_line:
                if line.startswith("AC"):
                    first_accession_line = False
                    # pattern given by the userman (see link above)
                    pattern = "([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})"
                    match = re.search(pattern, line)  # only get the primary accession number
                    if match:
                        accession = match.group()
            if line.startswith("DE"):
                if "EC=" in line:
                    pattern = r"((\d+|-)\.){3}(\d+|-)"
                    match = re.search(pattern, line)
                    if match:
                        ec_list.append(match.group())
        data[accession] = {}

        ec_set = set(ec_list)  # change in set to remove duplicate ec
        list_ec_complete = []
        for ec in ec_set:
            is_complete = True
            if "-" in ec or not ec[-1].isdigit():
                is_complete = False
            list_ec_complete.append((ec, is_complete))

        data[accession]["ec_numbers"] = list_ec_complete

    utils.save_pickle(data=data, output_file=output_file, logger=logger)


"""
Data structure of the xml that I get as of 24/04/2024
<table_structure name="entry">
<field Field="ec_num" Type="varchar(12)" Null="NO" Key="UNI" Default="" Extra="" Comment=""/>
<field Field="accepted_name" Type="varchar(300)" Null="YES" Key="" Extra="" Comment=""/>
<field Field="reaction" Type="text" Null="YES" Key="" Extra="" Comment=""/>
<field Field="other_names" Type="text" Null="YES" Key="" Extra="" Comment=""/>
<field Field="sys_name" Type="text" Null="YES" Key="" Extra="" Comment=""/>
<field Field="comments" Type="text" Null="YES" Key="" Extra="" Comment=""/>
<field Field="links" Type="text" Null="YES" Key="" Extra="" Comment=""/>
<field Field="class" Type="int" Null="YES" Key="" Extra="" Comment=""/>
<field Field="subclass" Type="int" Null="YES" Key="" Extra="" Comment=""/>
<field Field="subsubclass" Type="int" Null="YES" Key="" Extra="" Comment=""/>
<field Field="serial" Type="int" Null="YES" Key="" Extra="" Comment=""/>
<field Field="status" Type="char(3)" Null="YES" Key="" Extra="" Comment=""/>
<field Field="diagram" Type="text" Null="YES" Key="" Extra="" Comment=""/>
<field Field="cas_num" Type="varchar(100)" Null="YES" Key="" Extra="" Comment=""/>
<field Field="glossary" Type="text" Null="YES" Key="" Extra="" Comment=""/>
<field Field="last_change" Type="timestamp" Null="NO" Key="" Default="CURRENT_TIMESTAMP" Extra="DEFAULT_GENERATED on update CURRENT_TIMESTAMP" Comment=""/>
<field Field="id" Type="int" Null="NO" Key="PRI" Extra="auto_increment" Comment=""/>
<key Table="entry" Non_unique="0" Key_name="id" Seq_in_index="1" Column_name="id" Collation="A" Cardinality="8109" Null="" Index_type="BTREE" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="0" Key_name="ec_num" Seq_in_index="1" Column_name="ec_num" Collation="A" Cardinality="8109" Null="" Index_type="BTREE" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="1" Column_name="ec_num" Cardinality="1" Null="" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="2" Column_name="accepted_name" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="3" Column_name="reaction" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="4" Column_name="other_names" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="5" Column_name="sys_name" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="6" Column_name="comments" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="7" Column_name="links" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="8" Column_name="diagram" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="9" Column_name="cas_num" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<key Table="entry" Non_unique="1" Key_name="ec_num_2" Seq_in_index="10" Column_name="glossary" Cardinality="1" Null="YES" Index_type="FULLTEXT" Comment="" Index_comment="" Visible="YES"/>
<options Name="entry" Engine="MyISAM" Version="10" Row_format="Dynamic" Rows="8109" Avg_row_length="651" Data_length="5285912" Max_data_length="281474976710655" Index_length="3602432" Data_free="0" Auto_increment="8120" Create_time="2024-04-04 04:00:10" Update_time="2024-04-04 04:00:12" Check_time="2024-04-04 04:00:14" Collation="latin1_swedish_ci" Create_options="" Comment=""/>
</table_structure>
"""

"""
	<table_structure name="hist">
		<field Field="ec_num" Type="varchar(12)" Null="NO" Key="PRI" Default="" Extra="" Comment="" />
		<field Field="action" Type="varchar(11)" Null="NO" Key="" Default="" Extra="" Comment="" />
		<field Field="note" Type="text" Null="YES" Key="" Extra="" Comment="" />
		<field Field="history" Type="text" Null="YES" Key="" Extra="" Comment="" />
		<field Field="class" Type="int" Null="YES" Key="" Extra="" Comment="" />
		<field Field="subclass" Type="int" Null="YES" Key="" Extra="" Comment="" />
		<field Field="subsubclass" Type="int" Null="YES" Key="" Extra="" Comment="" />
		<field Field="serial" Type="int" Null="YES" Key="" Extra="" Comment="" />
		<field Field="status" Type="char(3)" Null="YES" Key="" Extra="" Comment="" />
		<field Field="last_change" Type="timestamp" Null="NO" Key="" Default="CURRENT_TIMESTAMP" Extra="DEFAULT_GENERATED on update CURRENT_TIMESTAMP" Comment="" />
		<key Table="hist" Non_unique="0" Key_name="PRIMARY" Seq_in_index="1" Column_name="ec_num" Collation="A" Cardinality="8109" Null="" Index_type="BTREE" Comment="" Index_comment="" Visible="YES" />
		<key Table="hist" Non_unique="0" Key_name="ec_num" Seq_in_index="1" Column_name="ec_num" Collation="A" Cardinality="8109" Null="" Index_type="BTREE" Comment="" Index_comment="" Visible="YES" />
		<options Name="hist" Engine="MyISAM" Version="10" Row_format="Dynamic" Rows="8109" Avg_row_length="110" Data_length="892672" Max_data_length="281474976710655" Index_length="416768" Data_free="0" Auto_increment="1" Create_time="2024-04-04 04:00:14" Update_time="2024-04-04 04:00:15" Collation="latin1_swedish_ci" Create_options="" Comment="" />
	</table_structure>
"""


def explorenz_ec(input_file: str, output_file: str, logger):
    """
    This function parse the data of an xml file from parse_explorenz
    and extract the entry info of the ec number

    Args:
        input_file : The path to the file to be parsed
        output_file : Name and path of the output file
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

    for table_data in root.findall("database/table_data[@name='hist']"):
        for row in table_data.findall("row"):
            ec_num = row.find("field[@name='ec_num']")  # get the current_ec of the row
            date_created = row.find("field[@name='history']")

            pattern = "created ([0-9]*)"
            match = re.search(pattern=pattern, string=date_created.text)
            if match:
                created = match.group(1)
            if data[ec_num.text]:
                data[ec_num.text]["created"] = created
            ec_action = row.find("field[@name='action']")
            if (
                ec_action.text == "deleted" or ec_action.text == "transferred"
            ):  # check history to the deleted action because the entry is still present even if deleted
                data.pop(ec_num.text)
    utils.save_pickle(data, output_file, logger)


"""
	<table_structure name="class">
		<field Field="id" Type="int" Null="NO" Key="PRI" Extra="auto_increment" Comment="" />
		<field Field="class" Type="int" Null="NO" Key="" Default="0" Extra="" Comment="" />
		<field Field="subclass" Type="int" Null="YES" Key="" Extra="" Comment="" />
		<field Field="subsubclass" Type="int" Null="YES" Key="" Extra="" Comment="" />
		<field Field="heading" Type="varchar(255)" Null="YES" Key="" Extra="" Comment="" />
		<field Field="note" Type="text" Null="YES" Key="" Extra="" Comment="" />
		<field Field="last_change" Type="timestamp" Null="NO" Key="" Default="CURRENT_TIMESTAMP" Extra="DEFAULT_GENERATED on update CURRENT_TIMESTAMP" Comment="" />
		<key Table="class" Non_unique="0" Key_name="PRIMARY" Seq_in_index="1" Column_name="id" Collation="A" Cardinality="403" Null="" Index_type="BTREE" Comment="" Index_comment="" Visible="YES" />
		<options Name="class" Engine="MyISAM" Version="10" Row_format="Dynamic" Rows="403" Avg_row_length="153" Data_length="62020" Max_data_length="281474976710655" Index_length="7168" Data_free="0" Auto_increment="673" Create_time="2024-04-04 04:00:10" Update_time="2024-04-04 04:00:10" Collation="latin1_swedish_ci" Create_options="" Comment="" />
	</table_structure>
	<table_data name="class">
"""


def explorenz_nomenclature(input_file: str, output_file: str, logger):
    """
    This function parse the data of an xml file from parse_explorenz
    and extract the class information to build the nomenclature

    Args:
        input_file : The path to the file to be parsed
        output_file : Name and path of the output file
    """
    tree = ET.parse(input_file)
    root = tree.getroot()
    data = {}  # will contain the ecc and a subdictionary with his parameter
    for table_data in root.findall("database/table_data[@name='class']"):
        for row in table_data.findall("row"):
            first = row.find("field[@name='class']")
            second = row.find("field[@name='subclass']")
            third = row.find("field[@name='subsubclass']")
            heading = row.find("field[@name='heading']")
            heading = strip_tags(heading.text)
            ec = first.text + "."
            if second.text == "0":
                ec = ec + "-.-.-"
            else:
                ec = ec + second.text + "."
                if third.text == "0":
                    ec = ec + "-.-"
                else:
                    ec = ec + third.text + ".-"
            data[ec] = {
                "first_number": first.text,
                "second_number": second.text,
                "third_number": third.text,
                "heading": heading,
            }
    utils.save_pickle(data, output_file, logger)


def brenda(input_file: str, output_file: str, logger):
    """
    This function parse the .txt from brenda and extract the ec number
    and the associated species.

    Args:
        input_file : The path to the file to be parsed
        output_file : Name and path of the output file
    """
    with open(input_file, "r") as f:
        lines = f.readlines()
        data = {}
        new_query = True
        ec_number = ""
        species = []
        for line in lines:
            if line.startswith("ID"):
                pattern = "ID\t([0-9.]+)"
                match = re.search(pattern, line)
                if match:
                    ec_number = match.group(1)
                    new_query = False
                    data[ec_number] = {}
            if line.startswith("PR") and not new_query:
                pattern = r"PR\t#[0-9]+# (\w* \w*)"
                match = re.search(pattern, line)
                if match:
                    stripped_match = match.group(1).strip()
                    if stripped_match not in species and stripped_match != "no activity":
                        species.append(stripped_match)

            if line.startswith(r"///") and not new_query:
                data[ec_number]["species"] = species.copy()
                ec_number = ""
                species.clear()
                new_query = True
        utils.save_pickle(data, output_file, logger)


def multiprocessing_pdb_iterate(root_dir: str, output_file: str, logger):
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

    utils.save_pickle(data, output_file, logger)


# Precompiling to improve efficiency over multiple files
pattern_pdb_id = re.compile(r"^<PDBx:datablock\s+datablockName=\"(\w+)\"")
pattern_ec_number = re.compile(r"<PDBx:pdbx_ec>(\d+\.\d+\.\d+\.\d+)")
pattern_sp_id = re.compile(r"<PDBx:pdbx_db_accession>(\w+)<\/PDBx:pdbx_db_accession>")


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
