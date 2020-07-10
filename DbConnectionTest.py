import cx_Oracle
import sys

try:
    cx_Oracle.init_oracle_client(lib_dir=r"./instantclient")
except Exception as err:
    print("Fehler beim initialisieren des Oracle Clients.")
    print(err)
    sys.exit(1)


username = ""
password = ""

conection = cx_Oracle.connect(username, password, "localhost/rispdb1")

cursor = conection.cursor()

cursor.execute("""
    SELECT DISTINCT CLICKURL
    FROM AOLDATA.QUERYDATA
""")

entryAmount = 100

for url in cursor:
    if(entryAmount == 0):
        break
    print(url)
    entryAmount -= 1
