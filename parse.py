#!/usr/bin/env python

import pickle
import gzip
import xml.etree.ElementTree as ET
import re
from io import StringIO
from html.parser import HTMLParser


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


def gunzip_file(input_file: str, output_file: str, block_size=65536):
    """
    This function decompress a .gz file and write it as a decompressed file
    Args:
        input_file : path to the file to be decompressed
        output_file: path and name to the decompressed file
    """
    try:
        with gzip.open(input_file, "rb") as f_in:
            with open(output_file, "wb") as f_out:
                while True:
                    block = f_in.read(block_size)
                    if not block:
                        break
                    else:
                        f_out.write(block)
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


# def uniprot(input_file: str, output_file: str):
#    """
#    This function parse the data of dat file from uniprot
#
#    Args:
#        input_file : Name and path of the file to be parsed
#        output_file : Name and path of the output file
#
#    Returns:
#        dict: A dictionary where each key is an accession and the corresponding value is a dictionary
#            containing extracted EC numbers and a boolean indicating their completeness.
#    """
#    with open(input_file) as handle:
#        data = {}
#
#        for record in SwissProt.parse(handle):
#            if "EC=" in record.description:
#                accession = record.accessions[0]
#                data[accession] = {}
#                ec_numbers = []
#
#                if isinstance(record.description, str):
#                    ec_numbers = re.findall(r"EC=([\d.-]+)", record.description)
#                    listEc = []
#                    already_listed = False
#                    for ec in ec_numbers:
#                        for tup in listEc:  # Prevent adding multiple time the same ec number in the tuple
#                            if ec in tup:
#                                already_listed = True
#                                continue
#                        if already_listed:
#                            continue
#                        if "-" in ec:
#                            ec_complete = False
#                            listEc.append((ec, ec_complete))
#                        else:
#                            ec_complete = True
#                            listEc.append((ec, ec_complete))
#                    data[accession]["ec_numbers"] = listEc
#        # save_pickle(data, output_file)
#        print(len(data))


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
        i = 0
        current = ""
        contain_ec = False
        while line:
            current += line
            # i += 1
            if i > 100000:
                break
            if line.startswith("DE"):
                if "EC=" in line:
                    contain_ec = True

            if line.startswith("//"):
                # print("Query = ", current)
                if contain_ec:
                    yield current
                current = ""
                contain_ec = False

            line = f.readline()


def uniprot(input_file: str, output_file: str):
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
                    pattern = r"[\d.-]+"
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

    save_pickle(data=data, output_file=output_file)


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


def explorenz_ec(input_file: str, output_file: str):
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

    save_pickle(data, output_file)


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


def explorenz_nomenclature(input_file: str, output_file: str):
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
    save_pickle(data, output_file)


# def explore_hist(input_file: str):
#    tree = ET.parse(input_file)
#    root = tree.getroot()
#    data = {}  # will contain the ecc and a subdictionary with his parameter
#    for table_data in root.findall("database/table_data[@name='entry']"):
#        for row in table_data.findall("row"):
#            ec_num = row.find("field[@name='ec_num']")  # get the current_ec of the row
#            if ec_num is not None:
#                data[ec_num.text] = {}
#                for field in row.findall("field"):
#                    if field.attrib["name"] != "ec_num":
#                        data[ec_num.text][field.attrib["name"]] = field.text
#
#    for table_data in root.findall("database/table_data[@name='hist']"):
#        for row in table_data.findall("row"):
#            ec_num = row.find("field[@name='ec_num']")  # get the current_ec of the row
#            date_created = row.find("field[@name='history']")
#
#            pattern = "created ([0-9]*)"
#            match = re.search(pattern=pattern, string=date_created.text)
#            if match:
#                created = match.group(1)
#            if data[ec_num.text]:
#                data[ec_num.text]["created"] = created
#                print(ec_num.text, " : ", data[ec_num.text]["created"])


"""
----------------Test---------------
"""

# explorenz_ec("./data/enzyme-data.xml", "./data/explorenz_ec.pickle")


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


sprot_path = "/home/demonz/programmation/stage/orenza/af2_web/bioi2server/orenza/script/data/uniprot_sprot.dat"
explorenz_path = "/home/demonz/programmation/stage/orenza/af2_web/bioi2server/orenza/script/data/enzyme-data.xml"
# uniprotV2(sprot_path, "test")
# process_query(sprot_path)
# uniprot(sprot_path, "test")
# parse_explorenz(explorenz_path)
# load_pickle(explorenz_path)
# type_swiss("../data/pickle/swiss.pickle")


# analyse_swiss(file_output)

# file_explore = "../data/enzyme-data.xml"
# data_explore = parse_explorenz(file_explore)
#
# for key in data_explore["1.1.1.1"]:
#    print("Cle : ", key, "\n", "Value: ", data_explore["1.1.1.1"][key])
