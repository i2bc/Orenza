#!/usr/bin/env python

import sqlite3
import sys
from datetime import datetime
import parse_data as parse

# TODO: delete these lines when worked enough with sqlite3
# con = sqlite3.connect("../../db_orenza.sqlite3")
#
# cur = con.cursor()
#
# res = cur.execute("SELECT * FROM orenza_enzyme")
# res = cur.execute("SELECT ec_number FROM orenza_enzyme")
# print("Test: ")
# print(res.fetchone())
# print("Start check table exist")
# cur.execute("SELECT EXISTS (SELECT 1 FROM orenza_enzyme);")
# print("Resultat: ", cur.fetchone()[0])
#
# con.commit()
# con.close()


def current_time():
    """
    Returns:
        The current hour, minutes and second
    """
    current_time = datetime.now()
    formatted_time = current_time.strftime("%H:%M:%S")
    return formatted_time


def create_connection(database_path):
    """
    Create a database connection to the SQLite database specified by db_file
    Parameters:
        database_path: path to the database file
    Returns:
        Connection object or None
    """
    con = None
    try:
        con = sqlite3.connect(database_path)
    except sqlite3.Error as e:
        print("Error connecting to database: ", e)

    return con


def update_sprot(file_path, database_path):
    print(f"Start updating sprot database at: {current_time()}")
    sprot_table = "orenza_sprot"
    joint_table = "orenza_sprot_ec_numbers"
    ec_table = "orenza_ec"
    con = create_connection(database_path)
    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()

    cur = con.cursor()

    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {sprot_table})"):
        cur.execute(f"DELETE FROM {sprot_table}")
        cur.execute(f"DELETE FROM {joint_table}")
        con.commit()

    print(f"Start loading data at {current_time()}")
    sprot_data = parse.load_pickle(file_path)

    print(f"Start creating database at {current_time()}")
    if sprot_data:
        for key in sprot_data:
            accession = key

            query_sprot = f"INSERT INTO {sprot_table} (accession) VALUES (?)"
            cur.execute(query_sprot, (accession,))
            tuple_list_ec = sprot_data[key][
                "ec_numbers"
            ]  # data structure : [(ec_number1, ec_complete), (ec_number2, ec_complete)...]
            #            print(tuple_list_ec)
            for tup in tuple_list_ec:
                query_joint_table = f"INSERT INTO {joint_table} (id, sprot_id, ec_id) VALUES(Null, ?, ?)"
                #                print(accession, tup[0])
                cur.execute(query_joint_table, (accession, tup[0]))
                query_pk_exist = f"SELECT EXISTS (SELECT 1 FROM {ec_table} WHERE number =?)"
                if not cur.execute(query_pk_exist, (tup[0],)):
                    query_ec = f"INSERT INTO {ec_table} (number, complete) VALUES(?, ?)"
                    cur.execute(query_ec, tup)
            con.commit()
    print(f"Finished updating sprot database at: {current_time()}")


def update_explorenz(file_path, database_path):
    print(f"Start updating explorenz database at {current_time()}")
    table = "orenza_enzyme"
    con = create_connection(database_path)
    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()
    cur = con.cursor()
    if cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})"):
        cur.execute(f"DELETE FROM {table}")
        con.commit()

    print(f"Start loading data at {current_time()}")
    enzyme_data = parse.load_pickle(file_path)

    print(f"Start creating database at {current_time()}")
    if enzyme_data:
        for key in enzyme_data:
            ec_number = key
            reaction = enzyme_data[key]["reaction"]
            comments = enzyme_data[key]["comments"]
            orphan = True
            sprot_count = 0
            query = f"""
                        INSERT INTO {table} (ec_number, reaction, comments, orphan, sprot_count)
                        VALUES (?, ?, ?, ?, ?)
                        """
            cur.execute(query, (ec_number, reaction, comments, orphan, sprot_count))

        con.commit()
        con.close()
    else:
        print("Enzyme pickle couldn't be read")
    print(f"Finished updating explorenz database at {current_time()}")


# update_explorenz("../data/pickle/explorenz.pickle", "./test.sqlite3")
update_sprot("../data/pickle/swiss.pickle", "./test.sqlite3")
