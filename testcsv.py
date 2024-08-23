from csvreader import CSVFileReader
from processors.userprocessor import UserRowMapper, UserProcessor
import pyodbc
import logging

try:
    dburl = 'DRIVER={SQL Server};SERVER=localhost;DATABASE=Hunt;UID=huntlogin;PWD=Huntdbpa55'
    conn: pyodbc.Connection =  pyodbc.connect(dburl)
    c1 = CSVFileReader('users.csv', UserRowMapper(), UserProcessor(2000, conn.cursor()), True)
    c1.processRecords()
    conn.commit()
    conn.close()
    print("Done processing file")
except RuntimeError as rerr :
    logging.error("Error in running the process", rerr)

