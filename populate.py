#!/usr/bin/env python
import sys
import utils


def uniprot(filename, database, table_type, logger):
    """
    Initialize the table trembl or sprot with the info of the parsing
    and the joint table between ec and this one

    Args:
        filename: the name and path of the data to be added to the database (pickle format)
        database: the name and path of the database to be updated
        table_type: need to precise the table to be updated trembl or sprot
    """
    if table_type not in ["sprot", "trembl"]:
        raise ValueError("Invalid table_type. Allowed values are 'sprot' or 'trembl'.")

    logger.info("Start updating table")
    uniprot_table = f"orenza_{table_type}"
    joint_table = f"orenza_{table_type}_ec_numbers"
    ec_table = "orenza_ec"
    con = utils.create_connection(database, logger)
    if not con:
        sys.exit()

    cur = con.cursor()

    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {uniprot_table})"):
        cur.execute(f"DELETE FROM {uniprot_table}")
        cur.execute(f"DELETE FROM {joint_table}")
        con.commit()

    logger.info("Start loading data")
    uniprot_data = utils.load_pickle(filename, logger)

    logger.info("Start creating table")
    if uniprot_data:
        for key in uniprot_data:
            accession = key

            query_uniprot = f"INSERT INTO {uniprot_table} (accession) VALUES (?)"
            cur.execute(query_uniprot, (accession,))
            tuple_list_ec = uniprot_data[key][
                "ec_numbers"
            ]  # data structure : [(ec_number1, ec_complete), (ec_number2, ec_complete)...]
            for tup in tuple_list_ec:
                query_joint_table = f"INSERT INTO {joint_table} (id, {table_type}_id, ec_id) VALUES(Null, ?, ?)"
                cur.execute(query_joint_table, (accession, tup[0]))
                query_pk_exist = f"SELECT EXISTS (SELECT 1 FROM {ec_table} WHERE number =?)"
                cur.execute(query_pk_exist, (tup[0],))
                exists = cur.fetchone()[0]
                if not exists:
                    query_ec = f"INSERT INTO {ec_table} (number, complete) VALUES(?, ?)"
                    cur.execute(query_ec, tup)
            con.commit()
    logger.info("Finished updating table")


def explorenz_ec(filename: str, database: str, logger):
    """
    Initialize the enzyme table with the info from the parsing

    Args:
        filename: path and name of the parsing file (pickle format)
        database: path and name of the database to be updated
    """
    logger.info("Start updating")
    table = "orenza_enzyme"
    con = utils.create_connection(database, logger)
    if not con:
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        con.commit()

    logger.info("Start loading data")
    enzyme_data = utils.load_pickle(filename, logger)

    logger.info("Start populating table")
    if enzyme_data:
        for key in enzyme_data:
            ec_number = key
            reaction = enzyme_data[key]["reaction"]
            comments = enzyme_data[key]["comments"]
            orphan = True
            sprot_count = 0
            trembl_count = 0
            pdb_count = 0
            species_count = 0
            created = enzyme_data[key]["created"]
            first_number = enzyme_data[key]["class"]
            second_number = enzyme_data[key]["subclass"]
            third_number = enzyme_data[key]["subsubclass"]
            common_name = enzyme_data[key]["accepted_name"]
            systematic_name = enzyme_data[key]["sys_name"]
            other_name = enzyme_data[key]["other_names"]

            query = f"""
                        INSERT INTO {table} (ec_number, reaction, comments, orphan, sprot_count, 
                                             trembl_count, pdb_count, species_count, created, first_number, second_number, third_number, common_name, systematic_name, other_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
            cur.execute(
                query,
                (
                    ec_number,
                    reaction,
                    comments,
                    orphan,
                    sprot_count,
                    trembl_count,
                    pdb_count,
                    species_count,
                    created,
                    first_number,
                    second_number,
                    third_number,
                    common_name,
                    systematic_name,
                    other_name,
                ),
            )

        con.commit()
        con.close()
    else:
        logger.error("Pickle could not be read")
    logger.info("Finished populating table")


def explorenz_nomenclature(filename: str, database: str, logger):
    """
    Populate nomenclature table with information from explorenz parsing
    Args:
        filename: path and name of the parsing file (pickle format)
        database: path and name of the database to be updated
    """
    logger.info("Start updating nomenclature table")
    table = "orenza_nomenclature"
    con = utils.create_connection(database, logger)

    if not con:
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        con.commit()

    logger.info("Start loading data")
    nomenclature_data = utils.load_pickle(filename, logger)

    logger.info("Start creating table")
    if nomenclature_data:
        for key in nomenclature_data:
            ec_number = key
            heading = nomenclature_data[key]["heading"]
            first_number = nomenclature_data[key]["first_number"]
            second_number = nomenclature_data[key]["second_number"]
            third_number = nomenclature_data[key]["third_number"]
            query = f"""
                        INSERT INTO {table} (ec_number, heading, first_number, second_number, third_number)
                        VALUES (?, ?, ?, ?, ?)
                        """
            cur.execute(
                query,
                (
                    ec_number,
                    heading,
                    first_number,
                    second_number,
                    third_number,
                ),
            )

        con.commit()
        con.close()
    else:
        logger.error("Pickle could not be read")
    logger.info("Finished updating nomenclature table")


def brenda(filename: str, database: str, logger):
    """
    Initialize the  species table with the info of the parsing
    and the joint table between enzyme and this one

    Args:
        filename: the name and path of the data to be added to the database (pickle format)
        database: the name and path of the database to be updated
    """
    logger.info("Start updating table")
    table = "orenza_species"
    joint_table = "orenza_species_enzymes"
    enzyme_table = "orenza_enzyme"

    con = utils.create_connection(database, logger)

    if not con:
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        cur.execute(f"DELETE FROM {joint_table}")
        con.commit()

    logger.info("Start loading data")
    brenda_data = utils.load_pickle(filename, logger)

    logger.info("Start creating table")
    if brenda_data:
        invalid_ec = []
        for key in brenda_data:
            ec_number = key
            species = brenda_data[key]["species"]
            for specie in species:
                query_ec_exist = f"SELECT EXISTS (SELECT 1 FROM {enzyme_table} WHERE ec_number =?)"
                cur.execute(query_ec_exist, (ec_number,))
                ec_exists = cur.fetchone()[0]
                if ec_exists:
                    query_name_exist = f"SELECT EXISTS (SELECT 1 FROM {table} WHERE name=?)"
                    cur.execute(query_name_exist, (specie,))
                    name_exists = cur.fetchone()[0]
                    if not name_exists:
                        query_insert = f"""
                                INSERT INTO {table} (name)
                                VALUES (?)
                                """
                        cur.execute(
                            query_insert,
                            (specie,),
                        )

                    query_joint_table = f"""INSERT INTO {joint_table} (species_id, enzyme_id) VALUES (?, ?)"""
                    cur.execute(query_joint_table, (specie, ec_number))

                if not ec_exists:
                    invalid_ec.append(ec_number)

        con.commit()
        con.close()
        logger.info("List of invalid ec (ec not updated to the current number to explorenz current notation):", invalid_ec)
    else:
        logger.error("Pickle could not be read")

    logger.info("Finished updated table")


def kegg(filename: str, database: str, logger):
    """
    Initialize the  kegg table with the info of the scraping of kegg pathway page
    Args:
        filename: the name and path of the data to be added to the database (pickle format)
        database: the name and path of the database to be updated
    """
    logger.info("Start updating table")
    table = "orenza_kegg"
    joint_table = "orenza_kegg_enzymes"
    enzyme_table = "orenza_enzyme"
    con = utils.create_connection(database, logger)

    if not con:
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        cur.execute(f"DELETE FROM {joint_table}")
        con.commit()

    logger.info("Start loading data")
    kegg_data = utils.load_pickle(filename, logger)

    logger.info("Start creating table")
    if kegg_data:
        invalid_ec = []
        for pathway_class in kegg_data:
            for pathway in kegg_data[pathway_class]:
                ec_numbers = kegg_data[pathway_class][pathway]
                query_pathway_exist = f"SELECT EXISTS (SELECT 1 FROM {table} WHERE pathway=?)"
                cur.execute(query_pathway_exist, (pathway,))
                pathway_exists = cur.fetchone()[0]
                if not pathway_exists:
                    query_insert = f"""
                            INSERT INTO {table} (pathway, pathway_class)
                            VALUES (?, ?)
                            """
                    cur.execute(
                        query_insert,
                        (pathway, pathway_class),
                    )
                for ec in ec_numbers:
                    query_ec_exist = f"SELECT EXISTS (SELECT 1 FROM {enzyme_table} WHERE ec_number =?)"
                    cur.execute(query_ec_exist, (ec,))
                    ec_exists = cur.fetchone()[0]
                    if ec_exists:
                        query_joint_table = f"""INSERT INTO {joint_table} (kegg_id, enzyme_id) VALUES (?, ?)"""
                        cur.execute(query_joint_table, (pathway, ec))

                    if not ec_exists:
                        invalid_ec.append(ec)

        con.commit()
        con.close()
        logger.info("List of invalid ec (ec not updated to the current number to explorenz current notation):", invalid_ec)
    else:
        logger.error("Pickle could not be read")

    logger.info("Finished updating table")


def pdb(filename: str, database: str, logger):
    """
    Initialize the  pdb table with the info of the parsed files of the pdb
    Args:
        filename: the name and path of the data to be added to the database (pickle format)
        database: the name and path of the database to be updated
    """
    logger.info("Start updating table")
    table = "orenza_pdb"
    enzyme_table = "orenza_enzyme"
    con = utils.create_connection(database, logger)

    if not con:
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        con.commit()
    logger.info("Start loading data")
    pdb_data = utils.load_pickle(filename, logger)

    logger.info("Start creating table")
    if pdb_data:
        invalid_ec = []
        for key in pdb_data:
            ec_number = key
            id_tuple = pdb_data[key]
            query_ec_exist = f"SELECT EXISTS (SELECT 1 FROM {enzyme_table} WHERE ec_number=?)"
            cur.execute(query_ec_exist, (ec_number,))
            ec_exists = cur.fetchone()[0]
            if not ec_exists:
                invalid_ec.append(ec_number)
            else:
                for tup in id_tuple:
                    query_table = f"""INSERT INTO {table} (accession, uniprot_accession, ec_number_id) VALUES (?, ?, ?)"""
                    cur.execute(query_table, (tup[0], tup[1], ec_number))

        con.commit()
        con.close()

        logger.info("List of invalid ec (ec not updated to the current number to explorenz current notation):", invalid_ec)
    else:
        logger.error("Pickle could not be read")

    logger.info("Finished updating table")
