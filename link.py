import utils
import sys


def swiss_explorenz(database: str, table_type: str):
    """
    Update enzyme table based on the information of the joint table of ec and sprot or trembl
    Args:
        database : name and path of the database to Update
        table_type: update enzyme based on trembl or sprot
    """
    if table_type not in ["sprot", "trembl"]:
        raise ValueError("Invalid table_type. Allowed values are 'sprot' or 'trembl'.")

    joint_table = f"orenza_{table_type}_ec_numbers"
    ec_table = "orenza_ec"
    enzyme_table = "orenza_enzyme"

    con = utils.create_connection(database=database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()

    cur = con.cursor()

    cur.execute(f"SELECT number FROM {ec_table}")
    ec_number = cur.fetchall()
    for ec in ec_number:
        # cur return tuple, so we get only the ec_number value
        query_matching = f"SELECT ec_number FROM {enzyme_table} WHERE ec_number=?"
        cur.execute(query_matching, ec)
        matching = cur.fetchone()

        if matching:
            query_count = f"SELECT COUNT(*) FROM {joint_table} WHERE ec_id=?"
            cur.execute(query_count, ec)
            count = cur.fetchone()
            query_update = f"UPDATE {enzyme_table} SET orphan=0, {table_type}_count=? WHERE ec_number=?"
            data_update = (count[0], ec[0])
            cur.execute(query_update, data_update)
            con.commit()
    con.close()


def species(database: str):
    joint_table = "orenza_species_enzymes"
    enzyme_table = "orenza_enzyme"

    con = utils.create_connection(database=database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()

    cur = con.cursor()

    cur.execute(f"SELECT DISTINCT enzyme_id FROM {joint_table}")
    ec_number = cur.fetchall()
    for ec in ec_number:
        query_matching = f"SELECT ec_number FROM {enzyme_table} WHERE ec_number=?"
        cur.execute(query_matching, ec)
        matching = cur.fetchone()

        if matching:
            query_count = f"SELECT COUNT(*) FROM {joint_table} where enzyme_id=?"
            cur.execute(query_count, ec)
            count = cur.fetchone()
            query_update = (
                f"UPDATE {enzyme_table} SET species_count=? WHERE ec_number=?"
            )
            data_update = (count[0], ec[0])
            cur.execute(query_update, data_update)
            con.commit()
    con.close()


def pdb(database: str):
    pdb_table = "orenza_pdb"
    enzyme_table = "orenza_enzyme"

    con = utils.create_connection(database=database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()

    cur = con.cursor()

    cur.execute(f"SELECT ec_number_id FROM {pdb_table}")
    ec_number = cur.fetchall()
    for ec in ec_number:
        query_matching = f"SELECT ec_number FROM {enzyme_table} WHERE ec_number=?"
        cur.execute(query_matching, ec)
        matching = cur.fetchone()

        if matching:
            query_count = f"SELECT COUNT(*) FROM {pdb_table} where ec_number_id=?"
            cur.execute(query_count, ec)
            count = cur.fetchone()
            query_update = f"UPDATE {enzyme_table} SET pdb_count=? WHERE ec_number=?"
            data_update = (count[0], ec[0])
            cur.execute(query_update, data_update)
            con.commit()
    con.close()
