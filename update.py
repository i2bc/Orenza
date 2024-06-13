#!/usr/bin/env python
import os
import requests
import yaml
import download
import parse
import populate
import link
import scraping
import concurrent.futures
import customLog

with open("./config.yaml", "r") as yaml_file:
    config = yaml.load(yaml_file, Loader=yaml.FullLoader)

output_folder = config["output"]
database = config["database"]

logger = customLog.get_logger()

## Explorenz
update_explorenz = config["download"]["explorenz"]
if update_explorenz:

    explorenz_url = config["explorenz"]["url"]
    explorenz_data_compressed = os.path.join(output_folder, "data", config["explorenz"]["output_file"])
    explorenz_data_uncompressed = os.path.splitext(explorenz_data_compressed)[0]  # remove the compressed extension
    explorenz_ec_pickle = os.path.join(output_folder, "data", "explorenz_ec.pickle")
    explorenz_nomenclature_pickle = os.path.join(output_folder, "data", "explorenz_nomenclature.pickle")
    explorenz_file_delete = [explorenz_data_compressed, explorenz_data_uncompressed]

    logger = customLog.set_context(logger, "explorenz")

    logger.info("Start of the download")
    download.http(url=explorenz_url, filename=explorenz_data_compressed, logger=logger)
    parse.gunzip_file(input_file=explorenz_data_compressed, output_file=explorenz_data_uncompressed, logger=logger)
    logger.info("Start of parsing")
    parse.explorenz_ec(input_file=explorenz_data_uncompressed, output_file=explorenz_ec_pickle, logger=logger)
    parse.explorenz_nomenclature(
        input_file=explorenz_data_uncompressed, output_file=explorenz_nomenclature_pickle, logger=logger
    )

    for file in explorenz_file_delete:
        if os.path.isfile(file):
            os.remove(file)

    logger.info("Start of db populating")
    try:
        populate.explorenz_ec(explorenz_ec_pickle, database, logger)
        populate.explorenz_nomenclature(explorenz_nomenclature_pickle, database, logger=logger)
    except ValueError as e:
        logger.exception(e)
    logger.info("End of protocol")


## Sprot
update_sprot = config["download"]["sprot"]
if update_sprot:
    sprot_ftp = config["sprot"]["ftp"]
    sprot_remote_file = config["sprot"]["remote_file"]
    sprot_data_compressed = os.path.join(output_folder, "data", config["sprot"]["output_file"])
    sprot_data_uncompressed = os.path.splitext(sprot_data_compressed)[0]
    sprot_pickle = os.path.join(output_folder, "data", "sprot.pickle")
    sprot_file_delete = [sprot_data_compressed, sprot_data_uncompressed]

    logger = customLog.set_context(logger, "sprot")

    logger.info("Start of the download")
    download.ftp(ftp_host=sprot_ftp, remote_file=sprot_remote_file, local_file=sprot_data_compressed, logger=logger)
    logger.info("Start of the extraction")
    parse.gunzip_file(input_file=sprot_data_compressed, output_file=sprot_data_uncompressed, logger=logger)
    logger.info("Start of the parsing")
    parse.uniprot(input_file=sprot_data_uncompressed, output_file=sprot_pickle, logger=logger)

    for file in sprot_file_delete:
        if os.path.isfile(file):
            os.remove(file)
    try:
        logger.info("Start of db populating")
        populate.uniprot(filename=sprot_pickle, database=database, table_type="sprot", logger=logger)
    except ValueError as e:
        logger.exception(e)

    logger.info("End of protocol")

## Trembl
update_trembl = config["download"]["trembl"]
if update_trembl:
    trembl_ftp = config["trembl"]["ftp"]
    trembl_remote_file = config["trembl"]["remote_file"]
    trembl_data_compressed = os.path.join(output_folder, "data", config["trembl"]["output_file"])
    trembl_data_uncompressed = os.path.splitext(trembl_data_compressed)[0]
    trembl_pickle = os.path.join(output_folder, "data", "trembl.pickle")
    trembl_file_delete = [trembl_data_compressed, trembl_data_uncompressed]

    logger = customLog.set_context(logger, "trembl")
    logger.info("Start of the download")
    download.ftp(ftp_host=trembl_ftp, remote_file=trembl_remote_file, local_file=trembl_data_compressed, logger=logger)
    logger.info("Start of the extraction")
    parse.gunzip_file(input_file=trembl_data_compressed, output_file=trembl_data_uncompressed, logger=logger)
    logger.info("Start of the parsing")
    parse.uniprot(input_file=trembl_data_uncompressed, output_file=trembl_pickle, logger=logger)

    for file in trembl_file_delete:
        if os.path.isfile(file):
            os.remove(file)
    try:
        logger.info("Start of db populating")
        populate.uniprot(filename=trembl_pickle, database=database, table_type="trembl", logger=logger)
    except ValueError as e:
        logger.exception(e)

    logger.info("End of protocol")

# Kegg
update_kegg = config["download"]["kegg"]
if update_kegg:
    kegg_url = config["kegg"]["url"]
    kegg_pickle = os.path.join(output_folder, "data", "kegg.pickle")

    logger = customLog.set_context(logger, "kegg")
    logger.info("Start of scraping")
    scraping.kegg(kegg_url, kegg_pickle)
    logger.info("Start of db populating")
    populate.kegg(kegg_pickle, database, logger=logger)
    logger.info("End of protocol")

# Brenda
update_brenda = config["download"]["brenda"]
if update_brenda:
    brenda_data_compressed = os.path.join(output_folder, "data", config["brenda"]["compressed_file"])
    brenda_data_uncompressed = brenda_data_compressed.replace(".tar.gz", "")

    brenda_pickle = os.path.join(output_folder, "data", "brenda.pickle")
    brenda_file_delete = [brenda_data_uncompressed, brenda_data_compressed]

    logger = customLog.set_context(logger, "brenda")

    logger.info("Start of the extraction")
    parse.extract_tar(brenda_data_compressed, output_folder)
    logger.info("Start of the parsing")
    parse.brenda(brenda_data_uncompressed, brenda_pickle, logger=logger)

    for file in brenda_file_delete:
        if os.path.isfile(file):
            os.remove(file)

    try:
        logger.info("Start of db populating")
        populate.brenda(brenda_pickle, database, logger=logger)
    except ValueError as e:
        logger.exception(e)
    logger.info("End of the protocol")

# PDB
update_pdb = config["download"]["pdb"]
if update_pdb:
    pdb_url = config["pdb"]["url"]
    pdb_subfolder_path = os.path.join(output_folder, "pdb")
    pdb_pickle = os.path.join(output_folder, "data", "pdb.pickle")
    pdb_worker = config["pdb"]["worker"]

    logger = customLog.set_context(logger, "pdb")
    logger.info("Start of the download")
    pdb_subfolders = download.pdb_get_subfolder(pdb_url)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=pdb_worker)
    futures = [
        executor.submit(download.pdb_download_subfolder, pdb_url, pdb_subfolder_path, folder) for folder in pdb_subfolders
    ]
    concurrent.futures.wait(futures)

    logger.info("Start of the parsing")
    parse.multiprocessing_pdb_iterate(pdb_subfolder_path, pdb_pickle, logger=logger)
    logger.info("Start of db populating")
    populate.pdb(pdb_pickle, database, logger=logger)
    logger.info("End of protocol")

# Link database if either of these db is updated because we wipe the data when updating so it is needed to recreate all the links for the corresponding db
if update_explorenz or update_pdb or update_sprot or update_trembl or update_brenda:

    logger = customLog.set_context(logger, "linking")
    logger.info("Start of linking")
    link.pdb(database, logger)
    link.swiss_explorenz(database, "sprot", logger)
    link.swiss_explorenz(database, "trembl", logger)
    link.species(database, logger)
    logger.info("End of linking")
