#!/usr/bin/env python
import os
import yaml
import download
import parse
import populate
import link
import scraping
import concurrent.futures
import customLog
import argparse

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.yaml"), "r") as yaml_file:
    config = yaml.load(yaml_file, Loader=yaml.FullLoader)

logger = customLog.get_logger()

def dl_explorenz(output_folder,overwrite=False):
    """download explorenz data"""
    global config
    global logger

    explorenz_url = config["explorenz"]["url"]
    explorenz_data_compressed = os.path.join(output_folder, "data", config["explorenz"]["output_file"])
    explorenz_data_uncompressed = os.path.splitext(explorenz_data_compressed)[0]  # remove the compressed extension
    explorenz_ec_pickle = os.path.join(output_folder, "data", "explorenz_ec.pickle")
    explorenz_nomenclature_pickle = os.path.join(output_folder, "data", "explorenz_nomenclature.pickle")
    explorenz_file_delete = [explorenz_data_compressed, explorenz_data_uncompressed]

    if not (not os.path.exists(explorenz_ec_pickle) or not os.path.exists(explorenz_nomenclature_pickle) or overwrite):
        logger.info("dl_explorenz nothing to be done")
        return
    
    logger = customLog.set_context(logger, "explorenz")
    logger.info("Start of the download")
    download.http(url=explorenz_url, filename=explorenz_data_compressed, logger=logger)
    parse.gunzip_file(input_file=explorenz_data_compressed, output_file=explorenz_data_uncompressed, logger=logger)
    logger.info("Start of parsing")
    parse.explorenz_ec(input_file=explorenz_data_uncompressed, output_file=explorenz_ec_pickle, logger=logger)
    # [CQ]: outputs a dict of {EC number: {infos}}, e.g. obj['1.1.1.1'] = {'accepted_name': 'alcohol dehydrogenase', 'reaction': '(1) a primary alcohol + NAD+[...]', 'other_names': 'aldehyde reductase; ADH; [...]', 'sys_name': 'alcohol:NAD+ oxidoreductase', 'comments': 'A zinc protein. Acts on primary [...]', 'links': 'BRENDA, EAWAG-BBD, EXPASY, GENE, GTD, KEGG, PDB', 'class': '1', 'subclass': '1', 'subsubclass': '1', 'serial': '1', 'status': None, 'diagram': 'For diagram of mevalonate biosynthesis, {terp/MVA}', 'cas_num': '9031-72-5', 'glossary': None, 'last_change': '2024-05-20 13:03:28', 'id': '1', 'created': '1961'}
    parse.explorenz_nomenclature(
        input_file=explorenz_data_uncompressed, output_file=explorenz_nomenclature_pickle, logger=logger
    )
    # [CQ]: outputs a dict of {pseudo EC number: {infos}}, with all combinations of '?.?.?.-' e.g. obj['1.1.2.-'] = {'first_number': '1', 'second_number': '1', 'third_number': '2', 'heading': 'With a cytochrome as acceptor'}
    
    # clean up
    for file in explorenz_file_delete:
        if os.path.isfile(file):
            os.remove(file)
    logger.info("Done")


def dl_sprot(output_folder,overwrite=False):
    """download sprot data"""    
    global config
    global logger

    sprot_ftp = config["sprot"]["ftp"]
    sprot_remote_file = config["sprot"]["remote_file"]
    sprot_data_compressed = os.path.join(output_folder, "data", config["sprot"]["output_file"])
    sprot_data_uncompressed = os.path.splitext(sprot_data_compressed)[0]
    sprot_pickle = os.path.join(output_folder, "data", "sprot.pickle")
    sprot_file_delete = [sprot_data_compressed, sprot_data_uncompressed]

    if not (not os.path.exists(sprot_pickle) or overwrite):
        logger.info("dl_sprot nothing to be done")
        return
    
    logger = customLog.set_context(logger, "sprot")
    logger.info("Start of the download")
    download.ftp(ftp_host=sprot_ftp, remote_file=sprot_remote_file, local_file=sprot_data_compressed, logger=logger)
    logger.info("Start of the extraction")
    parse.gunzip_file(input_file=sprot_data_compressed, output_file=sprot_data_uncompressed, logger=logger)
    logger.info("Start of the parsing")
    parse.uniprot(input_file=sprot_data_uncompressed, output_file=sprot_pickle, logger=logger)

    # clean up
    for file in sprot_file_delete:
        if os.path.isfile(file):
            os.remove(file)
    logger.info("Done")


def dl_trembl(output_folder,overwrite=False):
    """download trembl data"""  
    global config
    global logger

    trembl_ftp = config["trembl"]["ftp"]
    trembl_remote_file = config["trembl"]["remote_file"]
    trembl_data_compressed = os.path.join(output_folder, "data", config["trembl"]["output_file"])
    trembl_data_uncompressed = os.path.splitext(trembl_data_compressed)[0]
    trembl_pickle = os.path.join(output_folder, "data", "trembl.pickle")
    trembl_file_delete = [trembl_data_compressed, trembl_data_uncompressed]

    if not (not os.path.exists(trembl_pickle) or overwrite):
        logger.info("dl_trembl nothing to be done")
        return
    
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
    logger.info("Done")


def dl_kegg(output_folder,overwrite=False):
    """download kegg data"""  
    global config
    global logger

    kegg_url = config["kegg"]["url"]
    kegg_pickle = os.path.join(output_folder, "data", "kegg.pickle")

    if not (not os.path.exists(kegg_pickle) or overwrite):
        logger.info("dl_kegg nothing to be done")
        return
    
    logger = customLog.set_context(logger, "kegg")
    logger.info("Start of scraping")
    scraping.kegg(kegg_url, kegg_pickle, logger)
    logger.info("Done")


def dl_brenda(output_folder,overwrite=False):
    """download brenda data"""      
    global config
    global logger

    brenda_data_compressed = config["brenda"]["compressed_file"]
    brenda_data_uncompressed = os.path.join(output_folder, "data", os.path.basename(brenda_data_compressed.replace(".tar.gz", "")))
    brenda_pickle = os.path.join(output_folder, "data", "brenda.pickle")
    brenda_file_delete = [brenda_data_uncompressed]

    if not (not os.path.exists(brenda_pickle) or overwrite):
        logger.info("dl_brenda nothing to be done")
        return
    
    logger = customLog.set_context(logger, "brenda")
    logger.info("Start of the extraction")
    parse.extract_tar(brenda_data_compressed, os.path.join(output_folder, "data"))
    logger.info("Start of the parsing")
    parse.brenda(brenda_data_uncompressed, brenda_pickle, logger=logger)

    for file in brenda_file_delete:
        if os.path.isfile(file):
            os.remove(file)
    logger.info("Done")


def dl_pdb(output_folder,overwrite=False):
    """download pdb data"""  
    global config
    global logger

    pdb_url = config["pdb"]["url"]
    pdb_subfolder_path = os.path.join(output_folder, "pdb")
    pdb_pickle = os.path.join(output_folder, "data", "pdb.pickle")
    pdb_worker = config["pdb"]["worker"]

    if not (not os.path.exists(pdb_pickle)):
        logger.info("dl_pdb nothing to be done")
        return
    
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
    logger.info("Done")


def populate_db(output_folder,database):
    """Populate the DB with data in pickle"""
    global config
    global logger
    
    logger = customLog.set_context(logger, "Populating")
    logger.info("Start of db populating")
    try:
        explorenz_ec_pickle = os.path.join(output_folder, "data", "explorenz_ec.pickle")
        if os.path.exists(explorenz_ec_pickle): populate.explorenz_ec(explorenz_ec_pickle, database, logger)
        explorenz_nomenclature_pickle = os.path.join(output_folder, "data", "explorenz_nomenclature.pickle")
        if os.path.exists(explorenz_nomenclature_pickle): populate.explorenz_nomenclature(explorenz_nomenclature_pickle, database, logger=logger)
        sprot_pickle = os.path.join(output_folder, "data", "sprot.pickle")
        if os.path.exists(sprot_pickle): populate.uniprot(filename=sprot_pickle, database=database, table_type="sprot", logger=logger)
        trembl_pickle = os.path.join(output_folder, "data", "trembl.pickle")    
        if os.path.exists(trembl_pickle): populate.uniprot(filename=trembl_pickle, database=database, table_type="trembl", logger=logger)
        kegg_pickle = os.path.join(output_folder, "data", "kegg.pickle")
        if os.path.exists(kegg_pickle): populate.kegg(filename=kegg_pickle, database=database, logger=logger)
        brenda_pickle = os.path.join(output_folder, "data", "brenda.pickle")    
        if os.path.exists(brenda_pickle): populate.brenda(filename=brenda_pickle, database=database, logger=logger)
        pdb_pickle = os.path.join(output_folder, "data", "pdb.pickle")
        if os.path.exists(pdb_pickle): populate.pdb(filename=pdb_pickle, database=database, logger=logger)
    except ValueError as e:
        logger.exception(e)


def link_tables(database):
    """Final step: set up the links between tables"""
    global config
    global logger

    logger = customLog.set_context(logger, "linking")
    logger.info("Start of linking")
    link.pdb(database, logger)
    link.swiss_explorenz(database, "sprot", logger)
    link.swiss_explorenz(database, "trembl", logger)
    link.species(database, logger)
    logger.info("End of linking")
 

def main():
    global config
    global logger

    parser = argparse.ArgumentParser()
    parser.add_argument('function', choices=['dl_explorenz', 'dl_sprot', 'dl_trembl', 
                                             'dl_kegg', 'dl_brenda', 'dl_pdb', 'populate'], 
                        help="The function to execute.")
    parser.add_argument("--overwrite", action="store_true", 
                        help="Use this option to force overwrite of already existing pickle files.") 
    args = parser.parse_args()

    database = config["database"]
    output_folder = config["output"]
    tmpdir = os.path.join(config["tmpdir"],args.function)

    if not os.path.exists(os.path.join(tmpdir,"data")):
        os.makedirs(os.path.join(tmpdir,"data"),exist_ok=True)
    if not os.path.exists(os.path.join(output_folder,"data")):
        os.makedirs(os.path.join(output_folder,"data"),exist_ok=True)

    if args.function != "populate":
        os.system(f"cp {output_folder}/data/* {tmpdir}/data/")

    if args.function == "dl_explorenz":
        dl_explorenz(tmpdir,overwrite=args.overwrite)
        os.system(f"mv {tmpdir}/data/* {output_folder}/data/")
        os.system(f"rm -r {tmpdir}")
    
    if args.function == "dl_sprot":
        dl_sprot(tmpdir,overwrite=args.overwrite)
        os.system(f"mv {tmpdir}/data/* {output_folder}/data/")
        os.system(f"rm -r {tmpdir}")

    if args.function == "dl_trembl":
        dl_trembl(tmpdir,overwrite=args.overwrite)
        os.system(f"mv {tmpdir}/data/* {output_folder}/data/")
        os.system(f"rm -r {tmpdir}")

    if args.function == "dl_kegg":
        dl_kegg(tmpdir,overwrite=args.overwrite)
        os.system(f"mv {tmpdir}/data/* {output_folder}/data/")
        os.system(f"rm -r {tmpdir}")

    if args.function == "dl_brenda":
        dl_brenda(tmpdir,overwrite=args.overwrite)
        os.system(f"mv {tmpdir}/data/* {output_folder}/data/")
        os.system(f"rm -r {tmpdir}")

    if args.function == "dl_pdb":
        dl_pdb(tmpdir,overwrite=args.overwrite)
        os.system(f"mv {tmpdir}/data/* {output_folder}/data/")
        os.system(f"rm -r {tmpdir}")

    if args.function == "populate":
        if not os.path.exists(os.path.dirname(database)):
            os.makedirs(os.path.dirname(database),exist_ok=True)
        if args.overwrite:
            os.system(f"rm {database}")
        if not os.path.exists(database):
            os.system(f"cp {os.path.dirname(os.path.abspath(__file__))}/db_orenza.sqlite3 {database}")
        os.system(f"cp {output_folder}/data/* {tmpdir}/data/")
        os.system(f"cp {database} {tmpdir}/")
        populate_db(tmpdir,os.path.join(tmpdir,os.path.basename(database)))
        link_tables(os.path.join(tmpdir,os.path.basename(database)))
        os.system(f"mv {tmpdir}/{os.path.basename(database)} {database}")
        os.system(f"rm -r {tmpdir}")
        os.system(f'echo "End update: $(date +%F)" >> {output_folder}/last_update.txt')

if __name__ == "__main__":
    main()    

