import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

import cx_Oracle
import sys
import sql_dataset_creation as dc

def VisualizeClickRank():
    """Visualizes the clicks base on the rang in a bar diagram"""
    with connection.cursor() as dbcursor:
        dbcursor.execute("""
            SELECT *
            FROM RANK_FLU
        """)
        flu_ranks = dbcursor.fetchall()
        #remove null 
        flu_ranks = [(0, x[1]) if x[0]==None else x for x in flu_ranks]
        flu_ranks.sort(key=lambda tup: tup[0])

        dbcursor.execute("""
            SELECT *
            FROM RANK_SYMPTOMS
        """)

        symptomes_ranks = dbcursor.fetchall()
        #remove null 
        symptomes_ranks = [(0, x[1]) if x[0]==None else x for x in symptomes_ranks]
        symptomes_ranks.sort(key=lambda tup: tup[0])

        #Draw data
        sns.set(style="white", context="talk")
        f, (ax1, ax2) = plt.subplots(2, 1, figsize=(7, 5))

        ranks, count = zip(*flu_ranks)
        x1 = np.array(ranks[:10])
        sns.barplot(x=x1, y=count[:10], palette="deep", ax=ax1).set_title("Suchen nach \"flu\" abhängig vom Rang")
        ax1.axhline(0, color="k", clip_on=False)
        ax1.set_ylabel("Clicks")
        ax1.set_xlabel("Rang")


        ranks, count = zip(*symptomes_ranks)
        x2 = np.array(ranks[:10])
        sns.barplot(x=x2, y=count[:10], palette="deep", ax=ax2).set_title("Suchen nach \"symptom\" abhängig vom Rang")
        ax2.axhline(0, color="k", clip_on=False)
        ax2.set_ylabel("Clicks")
        ax2.set_xlabel("Rang")

        # Finalize the plot
        sns.despine(bottom=False)
        #plt.setp(f.axes, yticks=[0, 500, 1000, 1500, 2000, 2500])
        plt.tight_layout(h_pad=2)
        plt.show()
    
def VisualizeTotalSearchesPop():
    with connection.cursor() as dbcursor:
        dbcursor.execute(
            """
                SELECT *
                FROM POP_COUNT
                ORDER BY searches DESC
            """)
        prodCount = dbcursor.fetchall()
        sns.set(style="whitegrid")

        f, ax = plt.subplots(figsize=(6, 15))

        word, count = zip(*prodCount)
        y = np.array(word)
        sns.set_color_codes("pastel")
        sns.barplot(x=count, y=y, label="Total", color="b")
        # Add a legend and informative axis label
        ax.legend(ncol=1, loc="lower right", frameon=True)
        ax.set(ylabel="Keyword", xlabel="Suchen nach Schlüsselwort.")
        sns.despine(left=True, bottom=True)
        plt.show()



    
try:
    cx_Oracle.init_oracle_client(lib_dir=r"./instantclient")
except Exception as err:
    print("Fehler beim initialisieren des Oracle Clients.")
    print(err)
    sys.exit(1)


username = ""
password = ""

connection = cx_Oracle.connect(username, password, "localhost/rispdb1")

#VisualizeTotalSearchesPop()
#VisualizeClickRank()