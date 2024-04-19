#!/usr/bin/env python
import download
import parse_data
import populate_db

# import link_data
import yaml

# TODO: add multiprocessing once the process work


with open("./config.yaml", "r") as yaml_file:
    config = yaml.load(yaml_file, Loader=yaml.FullLoader)

args = config["explorenz"]["download"]["args"]
dict_download_function = {"http": download.http, "ftp": download.ftp}
dict_parser_function = {"explorenz": parse_data.explorenz}
dict_populate_function = {"explorenz": populate_db.explorenz}

for database in config:
    download_function_name = config[database]["download"]["function"]
    download_args = config[database]["download"]["args"]
    download_function = dict_download_function.get(download_function_name, None)

    if download_function:
        # TODO: care because unpack in the order, should precise it well
        download_function(**download_args)
    else:
        print(f"Incorrect download function name provided: {download_function_name}")

    parser_function_name = config[database]["parsing"]["function"]
    parser_args = config[database]["parsing"]["args"]
    parser_function = dict_parser_function.get(parser_function_name, None)

    if parser_function:
        parser_function(**parser_args)
    else:
        print(f"Incorrect parser function name provided: {parser_function_name}")

    populate_function_name = config[database]["populate"]["function"]
    populate_args = config[database]["populate"]["args"]
    populate_function = dict_populate_function.get(populate_function_name, None)

    if populate_function:
        populate_function(**populate_args)
    else:
        print(f"Incorrect populate function name prodived: {populate_function_name}")
