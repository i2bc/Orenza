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
    uniprot_data = parse.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
    if uniprot_data:
        for key in uniprot_data:
            accession = key

            query_uniprot = f"INSERT INTO {uniprot_table} (accession) VALUES (?)"
            cur.execute(query_uniprot, (accession,))
            tuple_list_ec = uniprot_data[key][
                "ec_numbers"
            ]  # data structure : [(ec_number1, ec_complete), (ec_number2, ec_complete)...]
            #            print(tuple_list_ec)
            for tup in tuple_list_ec:
                query_joint_table = f"INSERT INTO {joint_table} (id, sprot_id, ec_id) VALUES(Null, ?, ?)"
                #                print(accession, tup[0])
                cur.execute(query_joint_table, (accession, tup[0]))
                query_pk_exist = f"SELECT EXISTS (SELECT 1 FROM {ec_table} WHERE number =?)"
                cur.execute(query_pk_exist, (tup[0],))
                exists = cur.fetchone()[0]
                if not exists:
                    query_ec = f"INSERT INTO {ec_table} (number, complete) VALUES(?, ?)"
                    cur.execute(query_ec, tup)
            con.commit()
    print(f"Finished updating {table_type} database at: {utils.current_time()}")


def explorenz(filename, database):
    """
    Initialize the enzyme table with the info from the parsing

    Args:
        filename: path and name of the parsing file (pickle format)
        database: path and name of the database to be updated
    """
    print(f"Start updating explorenz database at {utils.current_time()}")
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
    enzyme_data = parse.load_pickle(filename)

    print(f"Start creating database at {utils.current_time()}")
    if enzyme_data:
        for key in enzyme_data:
            ec_number = key
            reaction = enzyme_data[key]["reaction"]
            comments = enzyme_data[key]["comments"]
            orphan = True
            sprot_count = 0
            trembl_count = 0
            query = f"""
                        INSERT INTO {table} (ec_number, reaction, comments, orphan, sprot_count)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """
            cur.execute(query, (ec_number, reaction, comments, orphan, sprot_count, trembl_count))

        con.commit()
        con.close()
    else:
        print("Enzyme pickle couldn't be read")
    print(f"Finished updating explorenz database at {utils.current_time()}")


# update_explorenz("../data/pickle/explorenz.pickle", "./test.sqlite3")
# update_sprot("../data/pickle/swiss.pickle", "./test.sqlite3")
