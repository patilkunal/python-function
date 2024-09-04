import sqlite3
import unittest

from tests.userprocessor import TestUserRowMapper, TestUserProcessor
from readers.csvreader import CSVFileReader

# BAD WAY of writing the Test - this is not a UNIT test. It is END to END test.
# try:
#     dburl = 'DRIVER={SQL Server};SERVER=localhost;DATABASE=Hunt;UID=huntlogin;PWD=huntpass'
#     conn: pyodbc.Connection =  pyodbc.connect(dburl)
#     c1 = CSVFileReader('users.csv', UserRowMapper(), UserProcessor(2000, conn.cursor()), True)
#     c1.processRecords()
#     conn.commit()
#     conn.close()
#     print("Done processing file")
# except RuntimeError as rerr :
#     logging.error("Error in running the process", rerr)

class TestCSVReader(unittest.TestCase):
    def setUp(self):
        self.dbconn = sqlite3.connect(':memory:')
        self.dbconn.executescript("""
            create table USERS(
                id int,
                name varchar(255),
                salary int
            );
        """)

    def tearDown(self) -> None:
        self.dbconn.close()

    def test_csvfile_processing(self):
        csvreader = CSVFileReader('tests/users.csv', TestUserRowMapper(), TestUserProcessor(2000, self.dbconn.cursor()), True)
        csvreader.processRecords()
        self.dbconn.commit()
        cursor2 = self.dbconn.execute('select count(id) as rcount from USERS')
        records = cursor2.fetchall();
        self.assertEqual(records[0][0], 4, "Compare file line count to DB row count")
