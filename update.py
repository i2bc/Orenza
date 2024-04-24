#!/usr/bin/env python
import os
import yaml
import download
import utils
import parse
import populate
import link


with open("./config.yaml", "r") as yaml_file:
    config = yaml.load(yaml_file, Loader=yaml.FullLoader)

output_folder = config["output"]
database = config["database"]

# Explorenz
explorenz_url = config["explorenz"]["url"]
explorenz_data_compressed = os.path.join(output_folder, "data", config["explorenz"]["output_file"])
explorenz_data_uncompressed = os.path.splitext(explorenz_data_compressed)[0]  # remove the compressed extension
explorenz_pickle = os.path.join(output_folder, "data", "explorenz.pickle")
explorenz_file_delete = [explorenz_data_compressed, explorenz_data_uncompressed]
print(f"Start of explorenz download {utils.current_time()}")
download.http(url=explorenz_url, local_file=explorenz_data_compressed)
parse.gunzip_file(input_file=explorenz_data_compressed, output_file=explorenz_data_uncompressed)
print(f"Start of explorenz parsing {utils.current_time()}")
parse.explorenz(input_file=explorenz_data_uncompressed, output_file=explorenz_pickle)

for file in explorenz_file_delete:
    if os.path.isfile(file):
        os.remove(file)

print(f"Start of explorenz populating db {utils.current_time()}")
populate.explorenz(explorenz_pickle, database)
print(f"End of explorenz {utils.current_time}")

# Sprot
sprot_ftp = config["sprot"]["ftp"]
sprot_remote_file = config["sprot"]["remote_file"]
sprot_data_compressed = os.path.join(output_folder, "data", config["sprot"]["output_file"])
sprot_data_uncompressed = os.path.splitext(sprot_data_compressed)[0]
sprot_pickle = os.path.join(output_folder, "data", "sprot.pickle")
sprot_file_delete = [sprot_data_compressed, sprot_data_uncompressed]

print(f"Start of sprot download {utils.current_time()}")
download.ftp(ftp_host=sprot_ftp, remote_file=sprot_remote_file, local_file=sprot_data_compressed)
parse.gunzip_file(input_file=sprot_data_compressed, output_file=sprot_data_uncompressed)
print(f"Start of sprot parsing {utils.current_time()}")
parse.uniprot(input_file=sprot_data_uncompressed, output_file=sprot_pickle)

for file in sprot_file_delete:
    if os.path.isfile(file):
        os.remove(file)
try:
    print(f"Start of sprot populating db {utils.current_time()}")
    populate.uniprot(filename=sprot_pickle, database=database, table_type="sprot")
except ValueError as e:
    print(e)

# Trembl
trembl_ftp = config["trembl"]["ftp"]
trembl_remote_file = config["trembl"]["remote_file"]
trembl_data_compressed = os.path.join(output_folder, "data", config["trembl"]["output_file"])
trembl_data_uncompressed = os.path.splitext(trembl_data_compressed)[0]
trembl_pickle = os.path.join(output_folder, "data", "trembl.pickle")
trembl_file_delete = [trembl_data_compressed, trembl_data_uncompressed]

print(f"Start of trembl download {utils.current_time()}")
download.ftp(ftp_host=trembl_ftp, remote_file=trembl_remote_file, local_file=trembl_data_compressed)
print(f"Start of trembl gunzip {utils.current_time()}")
parse.gunzip_file(input_file=trembl_data_compressed, output_file=trembl_data_uncompressed)
print(f"Start of trembl parsing {utils.current_time()}")
parse.uniprot(input_file=trembl_data_uncompressed, output_file=trembl_pickle)

for file in trembl_file_delete:
    if os.path.isfile(file):
        os.remove(file)
try:
    print(f"Start of trembl populating {utils.current_time()}")
    populate.uniprot(filename=trembl_pickle, database=database, table_type="trembl")
except ValueError as e:
    print(e)


# Link database:
link.swiss_explorenz(database=database, table_type="sprot")
link.swiss_explorenz(database=database, table_type="trembl")
