import mysql.connector
import csv
import pymysql

"""
insert sql file into mysql 
 """
cnx = mysql.connector.connect(user='root',
                             password='**********',
                             host='localhost',
                             database='pcd')
cursor =cnx.cursor()

def executeScriptsFromFile(filename):
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')

    for command in sqlCommands:
        try:
            if command.strip() != '':
                cursor.execute(command)
        except IOError:
            print ("Command skipped: ")

executeScriptsFromFile('/home/jihene/Bureau/sql/france.sql')
cnx.commit()

"""
convert mysql to csv file  
"""
 

def execute(c, command):
    c.execute(command)
    return c.fetchall()

db = pymysql.connect(host='localhost', port=3306, user='root', passwd='********', db='pcd') #, charset='utf8')

c = db.cursor()

for table in execute(c, "show tables;"):
    table = table[0]
    cols = []
    for item in execute(c, "show columns from " + table + ";"):
        cols.append(item[0])
    data = execute(c, "select * from " + table + ";")
    with open(table + ".csv", "w", encoding="utf-8") as out:
        out.write("\t".join(cols) + "\n")
        for row in data:
           out.write("\t".join(str(el) for el in row) + "\n")
print(table + ".csv written")

