import numpy as np
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt

import cx_Oracle
import sys
import sql_dataset_creation as dc

def __insertMissingDates(countperDatetup, datestup):
    """If a date has no click it doesent exist in the list. And since we cant have list with varying lenghts, we need to insert the missing dates
    and add a zero as click count."""
    countperDate = list(countperDatetup)
    dates = list(datestup)
    listlen = len(countperDate)
    prevDate = dates[0]
    index = 1
    while(index < listlen):
        dayAfterPrevDay = prevDate + dt.timedelta(days=1)
        if dates[index] != dayAfterPrevDay:
            dates.insert(index-1, dayAfterPrevDay)
            countperDate.insert(index-1, 0)
            listlen +=1
            prevDate = dayAfterPrevDay
        else:
            prevDate = dates[index]
        index+=1
    return tuple(countperDate), tuple(dates)

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
        popCount = dbcursor.fetchall()
        sns.set(style="whitegrid")

        f, ax = plt.subplots(figsize=(6, 15))

        word, count = zip(*popCount)
        y = np.array(word)
        sns.set_color_codes("pastel")
        sns.barplot(x=count, y=y, label="Total", color="b")
        # Add a legend and informative axis label
        ax.legend(ncol=1, loc="lower right", frameon=True)
        ax.set(ylabel="Keyword", xlabel="Suchen nach Schlüsselwort.")
        sns.despine(left=True, bottom=True)
        plt.show()
    
def VisualizeTotalSearchesProd():
    
    with connection.cursor() as dbcursor:
        dbcursor.execute(
            """
                SELECT *
                FROM PROD_COUNT
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
        ax.set(ylabel="Keyword", xlabel="Suchen nach Produkten.")
        sns.despine(left=True, bottom=True)
        plt.show()

def VisualizeFluSearches():
    with connection.cursor() as dbcursor:
        dbcursor.execute(
        """
            SELECT count(qid), trunc(r.time,'DD')
            FROM anfrage_flu r
            GROUP BY trunc(r.time,'DD')
            order by trunc(r.time,'DD')
        """)

        flusearch = dbcursor.fetchall()
        flu_searches, flu_date = zip(*flusearch)
        
        seccursor = connection.cursor()
        seccursor.execute(
        """
            SELECT count(qid), trunc(r.time,'DD')
            FROM anfrage_symptoms r
            GROUP BY trunc(r.time,'DD')
            order by trunc(r.time,'DD')
        """)
        sympsearch = seccursor.fetchall()
        symp_searches, symp_date = zip(*sympsearch)
        amount=np.column_stack((flu_searches, symp_searches))
        data = pd.DataFrame(amount, symp_date, columns=["Suchen mit dem Wort \"flu\"","Suchen nach Grippesymptomen"])
        fig, ax = plt.subplots(1, 1)
        sns.set(style="whitegrid")
        sns.lineplot(data=data, palette="deep", linewidth=2, dashes=False, ax=ax)
        ax.set(ylabel="Suchen", xlabel="Datum", title="Korrelation zwischen suchen nach Grippesymptomen und dem Wort Grippe")

        plt.show()

def VisualizeQu():
    with connection.cursor() as dbcursor:
        dbcursor.execute(
            """
                SELECT *
                FROM QU_COUNT
                ORDER BY searches DESC
            """)
        popCount = dbcursor.fetchall()
        sns.set(style="whitegrid")

        f, ax = plt.subplots(figsize=(6, 15))

        word, count = zip(*popCount)
        y = np.array(word)
        sns.set_color_codes("pastel")
        sns.barplot(x=count, y=y, label="Total", color="b")
        # Add a legend and informative axis label
        ax.legend(ncol=1, loc="lower right", frameon=True)
        ax.set(ylabel="Art der Seite", xlabel="Suchen")
        sns.despine(left=True, bottom=True)
        plt.show()

def VisualizeSerchesByWebsiteType():
    with connection.cursor() as dbcursor:
        dbcursor.execute(
        """
            SELECT count(qid), trunc(r.time,'DD')
            FROM QU_MEDICAL r
            GROUP BY trunc(r.time,'DD')
            order by trunc(r.time,'DD')
        """)
        medsearches = dbcursor.fetchall()
        med_count, med_date = zip(*medsearches)
        med_count, med_date = __insertMissingDates(med_count,med_date)
        
        dbcursor.execute(
        """
            SELECT count(qid), trunc(r.time,'DD')
            FROM QU_NEWS r
            GROUP BY trunc(r.time,'DD')
            order by trunc(r.time,'DD')
        """)
        newssearches = dbcursor.fetchall()
        news_count, news_date = zip(*newssearches)
        news_count, news_date = __insertMissingDates(news_count, news_date)

        dbcursor.execute(
        """
            SELECT count(qid), trunc(r.time,'DD')
            FROM QU_OFFICAL r
            GROUP BY trunc(r.time,'DD')
            order by trunc(r.time,'DD')
        """)
        offisearches = dbcursor.fetchall()
        offi_count, offi_date = zip(*offisearches)
        offi_count, offi_date = __insertMissingDates(offi_count, offi_date)

        combined_counts = np.column_stack((med_count, news_count, offi_count))
        data = pd.DataFrame(combined_counts, med_date, columns=["Medical", "News", "Offical"])

        fig, ax = plt.subplots(1, 1)
        sns.set(style="whitegrid")
        sns.lineplot(data=data, palette="deep", linewidth=2, dashes=False, ax=ax)
        ax.set(ylabel="Suchen", xlabel="Datum", title="Geklickte Seiten nach Typ im Zeitverlauf")

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


#VisualizeFluSearches()
#VisualizeTotalSearchesProd()
#VisualizeTotalSearchesPop()
#VisualizeClickRank()
#VisualizeQu()
VisualizeSerchesByWebsiteType()
connection.close()