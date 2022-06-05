from datetime import datetime
from typing import Literal
import numpy as np
import pandas as pd
import similarityDB as sim
from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF, XSD, Namespace
from dbConn import openConn, closeConn
from django.core.validators import URLValidator

#open connection to db
con = openConn()

#initialize list
dataArray = []
pkColumn = []
pkTable = []
fkColumn =[]
fkTable = []
fkArray = []

sql = "SELECT table_name FROM information_schema.tables WHERE (table_schema = 'public')"
df_tableName = pd.read_sql(sql, con)

#select data
for i in range(len(df_tableName['table_name'])): #loop for retrieve data for each table of dataframe
    sql2 = "SELECT * FROM %s" %(df_tableName['table_name'][i])  
    df_data = pd.read_sql(sql2, con)
    column_list = df_data.columns
    dataArray.append(df_data)

    #select primary key --> out >> list of primary key and the PK table 
    sql3 = '''
    SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = '%s'::regclass
        AND    i.indisprimary;
    '''%(df_tableName['table_name'][i])

    df_pkInfo = pd.read_sql(sql3, con)
    if not df_pkInfo.empty:
        pkInfo_row = df_pkInfo.index
        for j in range(len(pkInfo_row)):
            pkColumn.append(df_pkInfo['attname'].loc[j])
        pkTable.append(df_tableName['table_name'].loc[i])
    else:
        pass

    #select foreign key --> out >> list of foreign key and the relation
    sql4 = '''
    SELECT
        tc.table_name, 
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';
    '''%(df_tableName['table_name'][i])
    df_fkInfo = pd.read_sql(sql4, con)
    if not df_fkInfo.empty:
        fkArray.append(df_fkInfo)
    else:
        pass

#close connection to db
closeConn(con)

dbo = Namespace("http://dbpedia.org/ontology/")
dbr = Namespace("http://id.dbpedia.org/resource/")
schema = Namespace("https://schema.org/")
    
g = Graph() #initialize graph
g.bind("dbo", dbo)
g.bind("dbr", dbr)
g.bind("schema", schema)

validate = URLValidator()

#METHOD MAPPING RDF DENGAN PRIMARY KEY
def RDFMapping (base, s, p ,o):
    subject = URIRef(base + "/" + s)
    objectType = URIRef(base)
    try: #exception for validate p is URL or not
        validate(p)
        predicate = p
    except:
        predicate = URIRef(objectType + "#" + p)
    # objects = Literal(o, datatype=XSD.dateTime)

    try: #exception for validate o is URL or not
        validate(o)
        objects = o
    except:
        if (type(o) == int):
            objects = Literal(o, datatype=XSD.integer)
        elif (type(o) == datetime):
            objects = Literal(o, datatype=XSD.date)
        else:
            objects = Literal(o)
    g.add((subject, RDF.type, objectType))
    g.add((subject, predicate, objects))

#METHOD MAPPING RDF TANPA PRIMARY KEY
def RDFMappingBlank (base, s, p ,o):
    subject = s
    objectType = URIRef(base)
    try: #exception for validate p is URL or not
        validate(p)
        predicate = p
    except:
        predicate = URIRef(objectType + "#" + p)
    # objects = Literal(o, datatype=XSD.dateTime)

    try: #exception for validate p is URL or not
        validate(o)
        objects = o
    except:
        if (type(o) == int):
            objects = Literal(o, datatype=XSD.integer)
        elif (type(o) == datetime):
            objects = Literal(o, datatype=XSD.date)
        else:
            objects = Literal(o)
    g.add((subject, RDF.type, objectType))
    g.add((subject, predicate, objects))

#METHOD MAPPING REFFERED RDF TO PRIMARY KEY
def RDFMappingref (base, s, p ,o, ref):
    subject = URIRef(base + "/" + s)
    predicate = URIRef(base + "#ref-" + p)
    objects = URIRef(ref + "/" + o)
    g.add((subject, predicate, objects))

#METHOD MAPPING REFFERED RDF TO BLANK NODE
def RDFMappingBlank (base, s, p, o, ref):
    subject = s
    predicate = URIRef(base + "#ref-" + p)
    objects = URIRef(ref + "/" + o)
    g.add((subject, predicate, objects))

#RETRIVE DATA EACH CELL 
print("Load data Relational Database . . .")
matchDict = dict()
matchIndDict = dict()

for i in range (len(dataArray)): #in data frame
    print(f"====== Load table {i+1} of {len(dataArray)} ======")
    diffpoint = False
    match = []
    matchObj = []

    print("Matching Process...")
    for j in dataArray[i]: #for each column in table[i]
        for k in range (len(pkColumn)): #check if the column is PK
            if j == pkColumn[k] and df_tableName['table_name'][i] == pkTable[k]: #create RDF with PK, if the column is PK, do:
                #note --> pkcolumn n pktable must located in same index
                for l in dataArray[i]: #for each column in table[i]
                    print(f"====== Matching column < {l} > of {len(dataArray[i].columns)} total column table {i+1} of {len(dataArray)} ======")
                    l_temp = l.lower()
                    if l_temp not in matchDict:
                        matchDbpedia = (sim.matchTerm(l, sim.listProp)) #out >> 'term' on vocab that match or 'None' if not match
                        matchSchema = (sim.matchTerm(l, sim.listSchema))
                        if matchDbpedia is not None and matchSchema is not None:
                            print("case 1")
                            if matchDbpedia[0][0] >= matchSchema[0][0]:
                                match = matchDbpedia[0][3]
                                matchDict[l_temp] = {1 : match}
                            else:
                                match = matchSchema[0][3]
                                matchDict[l_temp] = {2 : match}
                        elif matchDbpedia is not None and matchSchema is None:
                            print("case 2")
                            match = matchDbpedia[0][3]
                            matchDict[l_temp] = {1 : match}
                        elif matchDbpedia is None and matchSchema is not None:
                            print("case 3")
                            match = matchSchema[0][3]
                            matchDict[l_temp] = {2 : match}
                        else:
                            print("all none")
                            match = None
                            matchDict[l_temp] = {3 : match}
                    else:
                        print("in")
                        for key, value in matchDict[l_temp].items():
                            match = matchDict[l_temp][key]
               
                    for m in range (len(dataArray[i][l])): #for cell in range kolom [i][l]
                        if dataArray[i][l][m] is not None:
                            objects = dataArray[i][l][m]
                            if type(dataArray[i][l][m]) is np.float64 and np.isnan(dataArray[i][l][m]) == True:
                                pass
                            else:
                                if type(dataArray[i][l][m]) is np.float64 or type(dataArray[i][l][m]) is np.int64:
                                    pass
                                else: 
                                    if dataArray[i][l][m] not in matchIndDict:
                                        matchObj = (sim.matchingIndividual(dataArray[i][l][m], sim.listInstance))
                                        matchIndDict[dataArray[i][l][m]] = matchObj
                                    else:
                                        matchObj = matchIndDict[dataArray[i][l][m]]
                                    
                                    if (matchObj is not None):
                                        objects = dbr[matchObj]
                                    else:
                                        objects = dataArray[i][l][m]

                                base = df_tableName['table_name'][i]
                                subject = pkColumn[k] + "=" + str(dataArray[i][j][m])
                                
                                if (match is not None): #condition for identify the predicate is on vocab
                                    for key, value in matchDict[l_temp].items():
                                        if key == 1 :
                                            predicate = dbo[value]
                                        elif key == 2 :
                                            predicate = schema[value]
                                        else:
                                            predicate = l
                                else:
                                    predicate = l
                                
                                RDFMapping(base, subject, predicate, objects)
                                for n in range (len(fkArray)): #mapping for table that have foreign key
                                    for o in fkArray[n]:
                                        for p in range (len(fkArray[n][o])):
                                            if l == fkArray[n]['column_name'][p] and df_tableName['table_name'][i] == fkArray[n]['table_name'][p]:
                                                objRef = str(fkArray[n]['foreign_column_name'][p]) + "=" + str(dataArray[i][l][m])
                                                tabRef = str(fkArray[n]['foreign_table_name'][p])
                                                RDFMappingref(base, subject, l, objRef, tabRef)
                        else: continue
                diffpoint = True #skip next step

    if diffpoint == False: #Mapping with no primary key
        for p in dataArray[i]:
            for q in range (len(dataArray[i][p])):
                subject = BNode(str(dataArray[i][dataArray[i].columns[0]][q]))
                predicate = p
                objects = str(dataArray[i][p][q])
                RDFMappingBlank(base, subject, predicate, objects)

print("Mapping RDB2RDF Done.. ^^")