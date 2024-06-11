#!/usr/bin/env python
import os
import yaml
import download
import utils
import parse
import populate
import link
import scraping
import concurrent.futures
from time import process_time
import customLog

with open("./config.yaml", "r") as yaml_file:
    config = yaml.load(yaml_file, Loader=yaml.FullLoader)

output_folder = config["output"]
database = config["database"]

## Explorenz
update_explorenz = False
if update_explorenz:

    explorenz_url = config["explorenz"]["url"]
    explorenz_data_compressed = os.path.join(output_folder, "data", config["explorenz"]["output_file"])
    explorenz_data_uncompressed = os.path.splitext(explorenz_data_compressed)[0]  # remove the compressed extension
    explorenz_ec_pickle = os.path.join(output_folder, "data", "explorenz_ec.pickle")
    explorenz_nomenclature_pickle = os.path.join(output_folder, "data", "explorenz_nomenclature.pickle")
    explorenz_file_delete = [explorenz_data_compressed, explorenz_data_uncompressed]
    ## TODO: verify if still work with https instead of http function.

    logger = customLog.get_logger()
    # logger = customLog.ContextLoggerAdapter(logger, {"database": "explorenz"})
    logger = customLog.set_context(logger, "explorenz")

    logger.info("Start of the download")
    download.https(url=explorenz_url, filename=explorenz_data_compressed)
    parse.gunzip_file(input_file=explorenz_data_compressed, output_file=explorenz_data_uncompressed)
    logger.info("Start of parsing")
    parse.explorenz_ec(input_file=explorenz_data_uncompressed, output_file=explorenz_ec_pickle)
    parse.explorenz_nomenclature(input_file=explorenz_data_uncompressed, output_file=explorenz_nomenclature_pickle)

    for file in explorenz_file_delete:
        if os.path.isfile(file):
            os.remove(file)

    logger.info("Start of populating")
    populate.explorenz_ec(explorenz_ec_pickle, database)
    populate.explorenz_nomenclature(explorenz_nomenclature_pickle, database)
    logger.info("End of populating")


## Sprot
update_sprot = False
if update_sprot:
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


link.swiss_explorenz(database=database, table_type="sprot")

## Trembl
update_trembl = False
if update_trembl:
    trembl_ftp = config["trembl"]["ftp"]
    trembl_remote_file = config["trembl"]["remote_file"]
    trembl_data_compressed = os.path.join(output_folder, "data", config["trembl"]["output_file"])
    trembl_data_uncompressed = os.path.splitext(trembl_data_compressed)[0]
    trembl_pickle = os.path.join(output_folder, "data", "trembl.pickle")
    trembl_file_delete = [trembl_data_compressed, trembl_data_uncompressed]

    print(f"Start of trembl download {utils.current_time()}")
    download.ftp(
        ftp_host=trembl_ftp,
        remote_file=trembl_remote_file,
        local_file=trembl_data_compressed,
    )
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
link.swiss_explorenz(database=database, table_type="trembl")

# Kegg
update_kegg = False
if update_kegg:
    kegg_url = config["kegg"]["url"]
    kegg_pickle = os.path.join(output_folder, "data", "kegg.pickle")

    print(f"Start of kegg scraping {utils.current_time()}")
    scraping.kegg(kegg_url, kegg_pickle)
    print(f"Start of kegg populating {utils.current_time()}")
    populate.kegg(kegg_pickle, database)

# Brenda
update_brenda = False
if update_brenda:
    brenda_data_compressed = os.path.join(output_folder, "data", config["brenda"]["compressed_file"])
    brenda_data_uncompressed = brenda_data_compressed.replace(".tar.gz", "")
    brenda_data_uncompressed = "/data/work/I2BC/hugo.pointier/script/data/brenda_2023_1.txt/brenda_2023_1.txt"

    brenda_pickle = os.path.join(output_folder, "data", "brenda.pickle")
    brenda_file_delete = [brenda_data_uncompressed, brenda_data_compressed]

    print(f"Start of brenda extraction {utils.current_time()}")
    parse.extract_tar(brenda_data_compressed, output_folder)
    print(f"Start of brenda parsing  {utils.current_time()}")
    parse.brenda(brenda_data_uncompressed, brenda_pickle)

    for file in brenda_file_delete:
        if os.path.isfile(file):
            os.remove(file)

    try:
        print(f"Start of brenda populating at {utils.current_time()}")
        populate.brenda(brenda_pickle, database)
        print(f"End of brenda populating at {utils.current_time()}")
    except ValueError as e:
        print(e)

# PDB
update_pdb = False
if update_pdb:
    pdb_url = config["pdb"]["url"]
    pdb_subfolder_path = os.path.join(output_folder, "pdb")
    pdb_pickle = os.path.join(output_folder, "data", "pdb.pickle")
    pdb_worker = config["pdb"]["worker"]

    ## TODO: delete file after testing size variability
    pdb_subfolders = download.pdb_get_subfolder(pdb_url)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=pdb_worker)
    futures = [
        executor.submit(download.pdb_download_subfolder, pdb_url, pdb_subfolder_path, folder) for folder in pdb_subfolders
    ]
    concurrent.futures.wait(futures)

    t_start = process_time()
    print(f"Start of pdb parsing at {utils.current_time()}")
    parse.multiprocessing_pdb_iterate(pdb_subfolder_path, pdb_pickle)
    t_end = process_time()
    print(f"Elapsed time: {t_start}, {t_end}")
    print(f"Elapsed time for parsing: {t_end-t_start}")
    print(f"Start to populate pdb table at {utils.current_time()}")
    populate.pdb(pdb_pickle, database)
    print(f"End of pdb populating at {utils.current_time()}")
    print(f"Start of pdb linking at {utils.current_time()}")
    link.pdb(database)
