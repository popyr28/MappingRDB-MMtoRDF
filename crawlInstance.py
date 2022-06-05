from parse import *
from parse import compile
from pprint import pprint

#use to parsing instance DBpedia
#input: file xml that contains URI city and province of Indonesia
#this code parsing that file to individual string and save to file "File_city" for city and "File_province"  for province, then "File_instance" for all

try:
    fileC = open('dataDBpedia/company2.xml', encoding='utf8')
    fileP = open('dataDBpedia/company1.xml', encoding='utf8')
    File_city = open("parseResult/resultComp2.txt","w+", encoding='utf8')
    File_provinces = open("parseResult/resultComp1.txt","w+", encoding='utf8')
    # File_instance = open("parseResult/resultInstance1.txt","w+", encoding='utf8')
    lineC = fileC.readline()
    lineP = fileP.readline()
    cnt = 1
    listIntances = [] #just in case for look at the sequence list of result
    while lineC:
        instanceCity = compile('<binding name="company"><uri>http://dbpedia.org/resource/{kota}</uri></binding>')
        resultC = instanceCity.parse(lineC.strip())
        if (resultC is not None):
            listIntances.append(resultC['kota'])
            File_city.write("%s\n"%resultC['kota'])
            # File_instance.write("%s\n"%resultC['kota'])
        else:
            pass
        lineC = fileC.readline()
        cnt += 1
    cnt = 1
    while lineP:
        instanceProv = compile('<binding name="a"><uri>http://id.dbpedia.org/resource/{province}</uri></binding>')
        resultP = instanceProv.parse(lineP.strip())
        if (resultP is not None):
            listIntances.append(resultP['province'])
            File_provinces.write("%s\n"%resultP['province'])
            # File_instance.write("%s\n"%resultP['province'])
        else:
            pass
        lineP = fileP.readline()
        cnt += 1
finally:
    print("Parsing done..")
    fileC.close()
    fileP.close()