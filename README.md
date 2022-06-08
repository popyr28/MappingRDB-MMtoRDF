# MappingRDB-MMtoRDF
Main code algorithm in my Thesis to mapping Relational Database (RDB) and Multimedia data into Resource Description Framework (RDF) which is a format that have been used by Knowledge Graph. The algorithm using Direct Mapping method in Mapping and Wu Palmer Similarity method in Matching similarity. The multimedia data come from any URL address that contain file pdf and photo. From that URL, it will be retrieved and get a list of URL address photos and files pdf. Then, by using an algorithm, it will retrieves some metadata from the photos and files.
# Example
Suppose we have database in PostgreSQL and several URL address that we want transform into RDF.
1. Connect the database in the dbConn.py
2. Add the URL addresses into main.py
3. Run main.py
# Note
This algorithm also can be use if we want to mapping only RDB to RDF. Just connect the database and run the RDB2RDF.py

# Visualization several data
![image](https://user-images.githubusercontent.com/52410764/172585486-2255291a-51d9-4e69-8ec9-e7a3e9644196.png)
