import cx_Oracle
import sys
import sql_dataset_creation as dc

try:
    cx_Oracle.init_oracle_client(lib_dir=r"./instantclient")
except Exception as err:
    print("Fehler beim initialisieren des Oracle Clients.")
    print(err)
    sys.exit(1)


username = ""
password = ""

connection = cx_Oracle.connect(username, password, "localhost/rispdb1")

cur = connection.cursor()

for statement in dc.drop_views():
    try:
        print(statement)
        cur.execute(statement)
    except Exception as err:
        print(err)

for statement in dc.dataset_gen():
    try:
        print(statement)
        cur.execute(statement)
    except Exception as err:
        print(err)