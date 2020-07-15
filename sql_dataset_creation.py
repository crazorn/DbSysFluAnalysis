import xlrd #verfügbar in Pip

#name list of dynamicly created views. First entry is always count holding aggregates of all others
kword_dic = {
            'qu_medical':list(),
            'qu_offical':list(),
            'qu_news':list(),
            'prod_':list(),
            'pop_':list(),
            'symptoms_':list()}
pop_views=list()
qu_views=('qu_count', 'qu_news', 'qu_offical', 'qu_medical')  #const
intersects = ('intersect_users', 'symptoms_in_flu', 'flu_in_symptoms', 'flu_and_symptoms') #const
prod_views=list()

#creates Dictionary from keyword excel
def read_ktable():

    with xlrd.open_workbook("keywords.xlsx") as ktable:
        
        sheets = ktable.sheet_names()

        #Pseudo loop, only one sheet
        for sheet in sheets:
            sh = ktable.sheet_by_name(sheet)
            
            colval = sh.col_values(0)
            kword_dic['qu_medical']=list(filter(None, colval[1:]))

            colval = sh.col_values(1)
            kword_dic['qu_offical']=list(filter(None, colval[1:]))

            colval = sh.col_values(2)
            kword_dic['qu_news']=list(filter(None, colval[1:]))

            colval = sh.col_values(3)
            kword_dic['prod_']=list(filter(None, colval[1:]))

            colval = sh.col_values(4)
            kword_dic['pop_']=list(filter(None, colval[1:]))

            colval = sh.col_values(5)
            kword_dic['symptoms']=list(filter(None, colval[1:]))


    return kword_dic

def dataset_gen(tables=True):
    kword_dic = read_ktable()

    #create table for anfrage_flu
    if tables:
        yield   ("CREATE TABLE Anfrage_flu "
                "(QID NUMBER(*, 0) NOT NULL, URL VARCHAR2(100 BYTE), QUERY VARCHAR2(1000 BYTE), "
                "TIME TIMESTAMP(6), RANK NUMBER(*, 0), anonid NUMBER(*, 0) NOT NULL,PRIMARY KEY(QID)) ")

        yield   ("INSERT INTO Anfrage_flu( "
                "time,query, qid,rank,url,anonid) "
                "SELECT querytime,query,id,itemrank,CLICKURL,anonid "
                "FROM AOLDATA.querydata WHERE "
                "query like '%flu %' or "
                "query like '%flu.' or "
                "query like '%flu!%' or "
                "query like '%flu?%' or "
                "query like '%flu' or "
                "query like 'flu'")
        
        yield   ("CREATE TABLE Anfrage_symptoms( "
                "QID NUMBER(*, 0) NOT NULL "
                ", URL VARCHAR2(300 BYTE) "
                ", QUERY VARCHAR2(1000 BYTE) "
                ", TIME TIMESTAMP(6) "
                ", RANK NUMBER(*, 0) "
                ", anonid NUMBER(*,0) NOT NULL, "
                "PRIMARY KEY(QID))")
        
        yield   ("INSERT INTO Anfrage_symptoms( "
                "time,query,qid,rank,url,anonid) "
                "SELECT querytime,query,id,itemrank,CLICKURL,anonid "
                "FROM AOLDATA.querydata WHERE "
                "query like '%fever%' or "
                "query like '%cough%' or "
                "query like '%sore throat%' or "
                "query like '%runny nose%' or "
                "query like '%stuffy nose%' or "
                "query like '%muscle ache%' or "
                "query like '%body ache%' or "
                "query like '%headache%' or "
                "query like '%head ache%' or "
                "query like '%fatigue%' or "
                "query like '%vomit%' or "
                "query like '%diarrhea%' or "
                "query like '%shortness of breath%'")

    yield   ("Create view timestats_flu(qid, MONTH, DAY, HOUR) "
            "as select qid, "
            "EXTRACT(MONTH FROM anfrage_flu.time),"
            "EXTRACT(DAY FROM anfrage_flu.time),"
            "EXTRACT(HOUR FROM anfrage_flu.time) "
            "from Anfrage_flu")
    
    yield   ("Create view timestats_symptoms(qid, MONTH, DAY, HOUR) "
            "as select qid, "
            "EXTRACT(MONTH FROM anfrage_symptoms.time),"
            "EXTRACT(DAY FROM anfrage_symptoms.time),"
            "EXTRACT(HOUR FROM anfrage_symptoms.time) "
            "from Anfrage_symptoms")
    
    #dynamic view creation for popular keywords
    pop_views.append('pop_count')
    for kword in kword_dic['pop_']:
        nows_kword = kword.replace(' ', '')
        yield   (f"create view pop_{nows_kword} as "
                f"select * from anfrage_flu where query like '%{kword}%'")
        pop_views.append(f"pop_{nows_kword}")
    
    #Special case view combining bird flu relevant queries
    yield   ("create view pop_bird_combined as "
            "select * from pop_bird union "
            "select * from pop_avian union "
            "select * from pop_h5n1")
    
    pop_views.append('pop_bird_combined')

    pop_count= 'create view pop_count(keyword, searches) as '
    for  kword in kword_dic['pop_']:
        nows_kword = kword.replace(' ', '')
        pop_count += f"select '{nows_kword}', count(distinct time) from pop_{nows_kword} union "
    pop_count += "select 'bird_combined',  count(distinct time) from pop_bird_combined"
    yield pop_count

    #dynamic view for news, offical, medical
    qu_news = 'create view qu_news as '
    for kword in kword_dic['qu_news']:
        qu_news+= f"select * from anfrage_flu where url like '%{kword}%' union "

    qu_news = qu_news[:-6]
    qu_news+=''
    yield qu_news

    qu_offical = 'create view qu_offical as '
    for kword in kword_dic['qu_offical']:
        qu_offical+= f"select * from anfrage_flu where url like '%{kword}%' union "

    qu_offical = qu_offical[:-6]
    qu_offical+=''
    yield qu_offical

    qu_medical = 'create view qu_medical as '
    for kword in kword_dic['qu_medical']:
        qu_medical+= f"select * from anfrage_flu where url like '%{kword}%' union "

    qu_medical = qu_medical[:-6]
    qu_medical+=''
    yield qu_medical

    yield   ("create view qu_count(keyword, searches) as "
            "select 'news', count(distinct time) from qu_news union "
            "select 'offical', count(distinct time) from qu_offical union "
            "select 'medical',  count(distinct time) from qu_medical")

    yield   ("create view flu_in_symptoms as "
            "select qid from anfrage_flu "
            "where exists( "
            "select anonid from anfrage_symptoms where anfrage_symptoms.anonid = anfrage_flu.anonid)")
    
    yield ("create view symptoms_in_flu as "
            "select qid from anfrage_symptoms "
            "where exists( "
            "select anonid from anfrage_flu where anfrage_symptoms.anonid = anfrage_flu.anonid)")
    
    yield   ("create view intersect_users as "
            "select distinct anonid from anfrage_flu " 
            "where exists( "
            "select anonid from anfrage_symptoms where anfrage_symptoms.anonid = anfrage_flu.anonid)")
    
    yield ("create view flu_and_symptoms as "
            "select distinct anonid from( " 
            "select anonid from anfrage_flu union "
            "select anonid from anfrage_symptoms) ")
    
    prod_views.append('prod_count')
    for kword in kword_dic['prod_']:
        nows_kword = kword.replace(' ', '')
        yield (f"create view prod_{nows_kword} as "
                "select * from aoldata.querydata where "
                f"query like '%{kword}%' and "
                "exists(select anonid from flu_and_symptoms where flu_and_symptoms.anonid = aoldata.querydata.anonid)")
        prod_views.append(f"prod_{nows_kword}")

    prod_count = 'create view prod_count(product, searches) as '
    for kword in kword_dic['prod_']:
        nows_kword = kword.replace(' ', '')
        prod_count+= f"select '{nows_kword}', count(distinct querytime) from prod_{nows_kword} union "

    prod_count = prod_count[:-6]
    yield prod_count

    yield ("create view rank_flu(rank, count) as "
            "select rank, count(*) from anfrage_flu group by rank")
    
    yield   ("create view rank_symptoms(rank, count) as "
            "select rank, count(*) from anfrage_symptoms group by rank")

#yield all drop view or table statments
def drop_views(tables=True):
     
    if tables:
        yield "drop table anfrage_flu"
        yield 'drop table anfrage_symptoms'

    for view in pop_views:
        yield f"drop view {view} "
    for view in qu_views:
        yield f"drop view {view} "
    for view in intersects:
        yield f"drop view {view} "
    for view in prod_views:
        yield f"drop view {view} "
    
    yield "drop view timestats_flu"
    yield "drop view timestats_symptoms"
    yield 'drop view rank_flu'
    yield 'drop view rank_symptoms'
    

#generic join for parsed timestat tables view must be one of the created views kept in toplevel lists. 
#Valid inputs from lists pop_views and qu_views
def join_timestats_flu(view):
    return (f"select timestats_flu.qid, timestats_flu.month, timestats_flu.day, timestats_flu.hour "
            f"from timestats_flu inner join {view} on timestats_flu.qid = {view}.qid")
    
