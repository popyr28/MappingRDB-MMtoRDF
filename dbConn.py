import psycopg2

def openConn():
    db = psycopg2.connect(
        host="localhost",
        database="database",
        user="user",
        password="password"
    )
    print("Connection to Database : OK")
    return db

def closeConn(db):
    db.close()
    print("Closing Connection to Database : OK")