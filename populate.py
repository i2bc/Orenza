#!/usr/bin/env python

import sys
import parse
import utils


def uniprot(filename, database, table_type):
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

    print(f"Start updating {table_type} database at: {utils.current_time()}")
    uniprot_table = f"orenza_{table_type}"
    joint_table = f"orenza_{table_type}_ec_numbers"
    ec_table = "orenza_ec"
    con = utils.create_connection(database)
    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()

    cur = con.cursor()

    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {uniprot_table})"):
        cur.execute(f"DELETE FROM {uniprot_table}")
        cur.execute(f"DELETE FROM {joint_table}")
        con.commit()

    print(f"Start loading data at {utils.current_time()}")
    uniprot_data = utils.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
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
    print(f"Finished updating {table_type} table at: {utils.current_time()}")


def explorenz_ec(filename: str, database: str):
    """
    Initialize the enzyme table with the info from the parsing

    Args:
        filename: path and name of the parsing file (pickle format)
        database: path and name of the database to be updated
    """
    print(f"Start updating explorenz_ec database at {utils.current_time()}")
    table = "orenza_enzyme"
    con = utils.create_connection(database)
    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        con.commit()

    print(f"Start loading data at {utils.current_time()}")
    enzyme_data = utils.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
    if enzyme_data:
        for key in enzyme_data:
            ec_number = key
            reaction = enzyme_data[key]["reaction"]
            comments = enzyme_data[key]["comments"]
            orphan = True
            sprot_count = 0
            trembl_count = 0
            species_count = 0
            created = enzyme_data[key]["created"]
            first_number = enzyme_data[key]["class"]
            second_number = enzyme_data[key]["subclass"]
            third_number = enzyme_data[key]["subsubclass"]
            query = f"""
                        INSERT INTO {table} (ec_number, reaction, comments, orphan, sprot_count, 
                                             trembl_count, species_count, created, first_number, second_number, third_number)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    species_count,
                    created,
                    first_number,
                    second_number,
                    third_number,
                ),
            )

        con.commit()
        con.close()
    else:
        print("Enzyme pickle couldn't be read")
    print(f"Finished updating explorenz ec table at {utils.current_time()}")


def explorenz_nomenclature(filename: str, database: str):
    """
    Populate nomenclature table with information from explorenz parsing
    Args:
        filename: path and name of the parsing file (pickle format)
        database: path and name of the database to be updated
    """
    print(f"Start updating explorenz_nomenclature database at {utils.current_time()}")
    table = "orenza_nomenclature"
    con = utils.create_connection(database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        con.commit()

    print(f"Start loading data at {utils.current_time()}")
    nomenclature_data = utils.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
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
        print("Enzyme pickle couldn't be read")
    print(f"Finished updating explorenz nomenclature table at {utils.current_time()}")


def brenda(filename: str, database: str):
    """
    Initialize the  species table with the info of the parsing
    and the joint table between enzyme and this one

    Args:
        filename: the name and path of the data to be added to the database (pickle format)
        database: the name and path of the database to be updated
    """
    print(f"Start updating species(brenda) table at {utils.current_time()}")
    table = "orenza_species"
    joint_table = "orenza_species_enzymes"
    enzyme_table = "orenza_enzyme"

    con = utils.create_connection(database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        cur.execute(f"DELETE FROM {joint_table}")
        con.commit()

    print(f"Start loading data at {utils.current_time()}")
    brenda_data = utils.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
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
        print(f"invalid_ec : {invalid_ec}")
    else:
        print("Brenda pickle couldn't be read")

    print(f"Finished updating species table at {utils.current_time()}")


def kegg(filename: str, database: str):
    """
    Initialize the  kegg table with the info of the scraping of kegg pathway page
    Args:
        filename: the name and path of the data to be added to the database (pickle format)
        database: the name and path of the database to be updated
    """
    print(f"Start updating kegg pathway table at {utils.current_time()}")
    table = "orenza_kegg"
    joint_table = "orenza_kegg_ec_numbers"
    enzyme_table = "orenza_enzyme"
    con = utils.create_connection(database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        cur.execute(f"DELETE FROM {joint_table}")
        con.commit()

    print(f"Start loading data at {utils.current_time()}")
    kegg_data = utils.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
    if kegg_data:
        invalid_ec = []
        for key in kegg_data:
            pathway = key
            ec_numbers = kegg_data[key]
            query_pathway_exist = f"SELECT EXISTS (SELECT 1 FROM {table} WHERE pathway=?)"
            cur.execute(query_pathway_exist, (pathway,))
            pathway_exists = cur.fetchone()[0]
            if not pathway_exists:
                query_insert = f"""
                        INSERT INTO {table} (pathway)
                        VALUES (?)
                        """
                cur.execute(
                    query_insert,
                    (pathway,),
                )
            for ec in ec_numbers:
                query_ec_exist = f"SELECT EXISTS (SELECT 1 FROM {enzyme_table} WHERE ec_number =?)"
                cur.execute(query_ec_exist, (ec,))
                ec_exists = cur.fetchone()[0]
                if ec_exists:
                    print(f"kegg: {pathway}, ec: {ec}")
                    query_joint_table = f"""INSERT INTO {joint_table} (kegg_id, ec_id) VALUES (?, ?)"""
                    cur.execute(query_joint_table, (pathway, ec))

                if not ec_exists:
                    invalid_ec.append(ec)

        con.commit()
        con.close()
        print(f"invalid_ec : {invalid_ec}")
    else:
        print("Kegg pickle couldn't be read")

    print(f"Finished updating kegg pathway table at {utils.current_time()}")


def pdb(filename: str, database: str):
    """
    Initialize the  pdb table with the info of the parsed files of the pdb
    Args:
        filename: the name and path of the data to be added to the database (pickle format)
        database: the name and path of the database to be updated
    """
    print(f"Start updating pdb table at {utils.current_time()}")
    table = "orenza_pdb"
    enzyme_table = "orenza_enzyme"
    con = utils.create_connection(database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        con.commit()

    print(f"Start loading data at {utils.current_time()}")
    pdb_data = utils.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
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
        print(f"invalid_ec : {invalid_ec}")
    else:
        print("PDB pickle couldn't be read")

    print(f"Finished updating pdb table at {utils.current_time()}")


# pdb("./data/pdb.pickle", "../../db_orenza.sqlite3")
# kegg("./data/kegg.pickle", "../../db_orenza.sqlite3")
# brenda("./data/brenda.pickle", "../../db_orenza.sqlite3")
# explorenz_nomenclature("./data/explorenz_nomenclature.pickle", "../../db_orenza.sqlite3")
# update_explorenz("../data/pickle/explorenz.pickle", "./test.sqlite3")
# update_sprot("../data/pickle/swiss.pickle", "./test.sqlite3")
