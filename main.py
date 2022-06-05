from enum import Enum
from operator import itemgetter
import numpy as np
from rdflib.graph import Graph
from rdflib.term import URIRef
from typing import Set, Tuple
from rdflib import RDF, XSD, Literal, Namespace
from rdflib.term import BNode, Node
import retrieveMetadata as rm
import similarityDB as sim
from RDB2RDF import g, dataArray, pkColumn, pkTable, df_tableName
import pandas as pd
# from pprint import pprint

df_metadata = []
matchDict = dict()
url_img = []
url_file = []

#list_url to retrieve
list_url = ['url']

list_image = ['url']
list_file = ['url']

#list url that will added metadata
url_add_img = ['url']
url_add_file = ['url']

url_add_img.extend(list_image)
url_add_file.extend(list_file)

for i in range(len(list_url)):
    print(f"====== {i+1} URL of {len(list_url)} Totals URL ======")
    url_img_temp = []
    url_file_temp = []
    
    try:
        url_img_temp = rm.retrieve_url_image(list_url[i])
        url_img.extend(url_img_temp)

        url_file_temp = rm.retrieve_url_file(list_url[i])
        url_file.extend(url_file_temp)

    except:
        print("restart..")
        continue

url_img.extend(list_image)
url_file.extend(list_file)

df_img = pd.DataFrame()
df_img = rm.retrieve_metadata(url_img, url_add_img)
df_metadata.append(df_img)

df_file = pd.DataFrame()
df_file = rm.retrieve_file_metadata(url_file, url_add_file)
df_metadata.append(df_file)

with open("matchTerm/metadataMM-3.txt", "w+", encoding="utf-8") as file:
    for index in df_metadata:
        file.write(str(index) + '\n\n')

# pprint(df_metadata)

tripleT = Tuple[Node, Node, Node]
tripleSetT = Set[tripleT]
EX = Namespace("http://example.com/")
dbo = Namespace("http://dbpedia.org/ontology/")
schema = Namespace("https://schema.org/")

####################

# for debugging
# g = Graph()
# g.bind("ex", EX)

####################

#for mapping
g.bind("ex", EX)

#function for CREATE PRIMARY Triple. Bnode + s, p, o
def fromTriple (triple: tripleT) -> Tuple[Node, tripleSetT]:
    triples: tripleSetT = set()
    statement = BNode()
    triples.add((statement, RDF.type, RDF.Statement))
    triples.add((statement, RDF.subject, triple[0]))
    triples.add((statement, RDF.predicate, triple[1]))
    triples.add((statement, RDF.object, triple[2]))
    return statement, triples

#function for create reification triple
def addTriple (graph : Graph, triples: tripleSetT) -> Graph:
    for triple in triples:
        graph.add(triple)
    return graph

#function for input triple to graph
def inputGraph (s, p, o):
    g.add((s, p, o))
    return g

file = open("matchTerm/resultAllRDB_DM.txt","a+", encoding='utf8') #it will save matching result descriptive metadata and Data record RDB
fileMatch = open("matchTerm/resultMatchRDB_DM.txt", "a+", encoding='utf8')
fileHighest = open("matchTerm/resultHighRDB_DM.txt", "a+", encoding='utf8')
#############################################################################################################################

# for debugging
# def mapping(s, p, o):
#     subject = URIRef(s)
#     predicate = EX[p[0]]
#     object = Literal(o[0])
#     print(o[0])
#     triple = (subject, predicate, object)
#     tripleStatement, triples = fromTriple(triple)
#     addTriple(g, triples)

#     for i in range(1, len(p)):
#         print(len(p))
#         print(f"i={i}")
#         pred = EX[p[i]]
#         obj = Literal(o[i])
#         print(f"p={p[i]}, o={o[i]}")
#         inputGraph(tripleStatement, pred, obj)

#############################################################################################################################

# #for mapping
def mapping(s, p, o): #it will matching value description metadata with data record RDB
    obj = Literal(o[0])
    maxScore = []
    matchScore = []

    #it is only match index 0 of dataframe for s, p, o primary statement
    print(type(o[0]))
    if (type(o[0]) != np.int64 and type(o[0]) != int and type(o[0]) != np.float64 and type(o[0]) != float):
        for j in range(len(dataArray)):
                for k in dataArray[j]:
                    for l in range (len(pkColumn)):
                        if k == pkColumn[l] and df_tableName['table_name'][j] == pkTable[l]:
                            for m in dataArray[j]:
                                for n in range(len(dataArray[j][m])):
                                    score = 0
                                    if (type(dataArray[j][m][n]) != np.int64 and type(dataArray[j][m][n]) != np.float64):
                                        string2 = str(dataArray[j][m][n])
                                        string1 = str(o[0])
                                        keyTerm = str(dataArray[j][k][n])
                                        pkCol = pkColumn[l]
                                        base = df_tableName['table_name'][j]
                                        score = sim.matchMetadata(string1,string2)
                                        file.write(f"{string1:50s} -- {string2:50s} -- {score} \n")
                                        maxScore.append([score, string1, string2, base, pkCol, keyTerm])
                                    else:
                                        pass
                                file.write("\n=========== NEXT COLUMN ==============\n")
        matchScore.append(max(maxScore, key=itemgetter(0)))
        fileHighest.write(f"{matchScore[0][1]:50} -- {matchScore[0][2]:50} -- {matchScore[0][3]:50} -- {matchScore[0][4]:50} -- {matchScore[0][5]:50} -- {matchScore[0][0]} \n")
        if matchScore[0][0] > 0.8:
            fileMatch.write(f"{matchScore[0][1]:50} -- {matchScore[0][2]:50} -- {matchScore[0][3]:50} -- {matchScore[0][4]:50} -- {matchScore[0][5]:50} -- {matchScore[0][0]} \n")
            base = matchScore[0][3]
            subj = matchScore[0][4] + "=" + matchScore[0][5]
            obj = URIRef(base + "/" + subj)
        else:
            pass
    else: 
        pass

    #it will create Primary triple subject predicate object based on matching
    subject = URIRef(s)
    print("===== Matching primary predicate ... ======")
    predicate = EX[p[0]]
    p0_temp = p[0].lower()
    print(p0_temp)
    if p0_temp not in matchDict:
        # print(matchDict)
        print("not in")
        # predicate = EX[p[0]]
        # matchDict[p0_temp] = {3 : None}

        predDbpedia = sim.matchTerm(p[0], sim.listProp)
        predSchema = sim.matchTerm(p[0], sim.listSchema)
        if predDbpedia is not None and predSchema is not None:
            if predDbpedia[0][0] >= predDbpedia[0][0]:
                predicate = dbo[predDbpedia[0][3]]
                matchDict[p0_temp] = {1 : predicate}
            else:
                predicate = schema[predSchema[0][3]]
                matchDict[p0_temp] = {2 : predicate}
        elif predDbpedia is not None and predSchema is None:
            predicate = dbo[predDbpedia[0][3]]
            matchDict[p0_temp] = {1 : predicate}
        elif predDbpedia is None and predSchema is not None:
            predicate = schema[predSchema[0][3]]
            matchDict[p0_temp] = {2 : predicate}
        else:
            predicate = EX[p[0]]
            matchDict[p0_temp] = {3 : None}
    else:
        print("in")
        for key, value in matchDict[p0_temp].items():
            pred = matchDict[p0_temp][key]
            if pred is not None:
                if key == 1:
                    predicate = dbo[value]
                elif key == 2:
                    predicate = schema[value]
                else:
                    predicate = EX[p[0]]
            else:
                predicate = EX[p[0]]
    if isinstance(o[0], Enum):
        obj = o[0].value
        object = Literal(obj)
    else:
        object = obj
    triple = (subject, predicate, object)
    tripleStatement, triples = fromTriple(triple)
    addTriple(g, triples)
    print(matchDict)
    print("Create primary statement. . . OK")

    #this looping for matching dataframe start from index 1, because index 0 was match before 
    for i in range(1, len(p)):
        maxScoreNonPrimary = []
        matchNonPrimary = []
        print("=============================================\n")
        print("predicate {} of {}".format(i, len(p)))
        pred = EX[p[i]]
        p_temp = p[i].lower()
        print(p_temp)
        if p_temp not in matchDict:
            print("not in")
            predDbpedia = sim.matchTerm(p[i], sim.listProp)
            predSchema = sim.matchTerm(p[i], sim.listSchema)
            if predDbpedia is not None and predSchema is not None:
                if predDbpedia[0][0] >= predDbpedia[0][0]:
                    pred = dbo[predDbpedia[0][3]]
                    matchDict[p_temp] = {1 : pred}
                else:
                    pred = schema[predSchema[0][3]]
                    matchDict[p_temp] = {2 : pred}
            elif predDbpedia is not None and predSchema is None:
                pred = dbo[predDbpedia[0][3]]
                matchDict[p_temp] = {1 : pred}
            elif predDbpedia is None and predSchema is not None:
                pred = schema[predSchema[0][3]]
                matchDict[p_temp] = {2 : pred}
            else:
                pred = EX[p[i]]
                matchDict[p_temp] = {3 : None}
        else:
            print("in")
            for key, value in matchDict[p_temp].items():
                pred = matchDict[p_temp][key]
                if pred is not None:
                    if key == 1:
                        pred = dbo[value]
                    elif key == 2:
                        pred = schema[value]
                    else:
                        pred = EX[p[i]]
                else:
                    pred = EX[p[i]]
            
        obj = Literal(o[i])
        print(type(o[i]))
        if (type(o[i]) != np.int64 and type(o[i]) != int and type(o[i]) != np.float64 and type(o[i]) != float):
            for j in range(len(dataArray)):
                for k in dataArray[j]:
                    for l in range (len(pkColumn)):
                        if k == pkColumn[l] and df_tableName['table_name'][j] == pkTable[l]:
                            for m in dataArray[j]:
                                for n in range(len(dataArray[j][m])):
                                    score = 0
                                    if (type(dataArray[j][m][n]) != np.int64 and type(dataArray[j][m][n]) != np.float64):
                                        string2 = str(dataArray[j][m][n])
                                        string1 = str(o[i])
                                        keyTerm = str(dataArray[j][k][n])
                                        pkCol = pkColumn[l]
                                        base = df_tableName['table_name'][j]
                                        score = sim.matchMetadata(string1, string2)
                                        file.write(f"{string1:50s} -- {string2:50s} -- {score} \n")
                                        maxScoreNonPrimary.append([score, string1, string2, base, pkCol, keyTerm])
                                    else:
                                        pass
                                file.write("\n=========== NEXT COLUMN ==============\n")
            matchNonPrimary.append(max(maxScoreNonPrimary, key=itemgetter(0)))
            fileHighest.write(f"{matchNonPrimary[0][1]:50} -- {matchNonPrimary[0][2]:50} -- {matchNonPrimary[0][3]:50} -- {matchNonPrimary[0][4]:50} -- {matchNonPrimary[0][5]:50} -- {matchNonPrimary[0][0]} \n")
            if matchNonPrimary[0][0] > 0.8:
                fileMatch.write(f"{matchNonPrimary[0][1]:50} -- {matchNonPrimary[0][2]:50} -- {matchNonPrimary[0][3]:50} -- {matchNonPrimary[0][4]:50} -- {matchNonPrimary[0][5]:50} -- {matchNonPrimary[0][0]} \n")
                base = matchNonPrimary[0][3]
                subj = matchNonPrimary[0][4] + "=" + matchNonPrimary[0][5]
                obj = URIRef(base + "/" + subj)
            else:
                pass
        else:
            pass

        if isinstance(o[i], Enum):
            obj = o[i].value
            object = Literal(obj)
        else:
            object = obj
            
        inputGraph(tripleStatement, pred, object)
        
    print(matchDict)
    print("Mapping have done..")
    
#this is same as main function
for i in range(len(df_metadata)):
    print("Match file {} of {}".format(i, len(df_metadata)))
    for j in range(len(df_metadata[i])):
        listPre = []
        listObj = []
        for k in range(df_metadata[i][j].shape[0]):
            listObj.append(df_metadata[i][j].iloc[k][0])
        listPre = df_metadata[i][j].index.values
        print("================================================\n")
        print("Match url {} of {}".format(j, len(df_metadata[i])))
        mapping(df_metadata[i][j].columns.values[0], listPre, listObj) #Call Mapping function with parameter (url, list predikat(index), list object(value))
        file.write("==============================================\n")
        for l in listObj:
            file.write("i:{}, type:{}\n".format(i, type(i)))
        file.write("==============================================\n")

g.serialize(destination="mappingResult/MappingResult.ttl", format="turtle", encoding="utf8") #it will serialize combine with RDB
print("======= Knowledge Graph Successfully Build =======")

#NOTE
# print(df_file.columns.values[0]) #return name of column
# print(df_img.index.values) #return index
# print(df_file.shape[0]) #return lenght of index
# print(df_file.iloc[1][0]) #return  value of cell
 


