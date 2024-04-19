import utils
import sys


def swiss_explorenz(database: str, database_type: str):
    if database_type not in ["sprot", "trembl"]:
        raise ValueError("Invalid database_type. Allowed values are 'sprot' or 'trembl'.")

    uniprot_table = f"orenza_{database_type}"
    joint_table = f"orenza_{database_type}_ec_numbers"
    ec_table = "orenza_ec"

    con = utils.create_connection(database=database)

    if not con:
        print("Couldn't load the database properly see previous error messages")
        sys.exit()

    cur = con.cursor()

    ec_number = cur.execute("SELECT number FROM orenza_ec")
    print(ec_number.fetchall()[10][0])


swiss_explorenz("./db/db_orenza.sqlite3", "sprot")

# WARNING: keep to test if i get same number between both functions
# from orenza.models import Enzyme, Sprot, Ec
#
#
# def link_swiss_explorenz():
#    ec = Ec.objects.all()
#    for e in ec:
#        if e.complete:
#            if Enzyme.objects.filter(pk=e.number).exists():
#                count = len(e.sprots.all())
#                enzyme = Enzyme.objects.get(pk=e.number)
#                if enzyme.orphan:
#                    enzyme.orphan = False
#                enzyme.sprot_count = count
#                enzyme.save()
