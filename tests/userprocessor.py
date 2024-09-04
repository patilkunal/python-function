from common.interfaces import RowMapper
from common.dbprocessor import SQLDBRowProcessor
import pyodbc

# Maps the row from the file to row in DB. 
# Acts as domain model object for code use
class TestUser():
    def __init__(self, id: int, name: str, salary: int):
        self.id: int = id
        self.name: str = name
        self.salary: int = salary


# Implementation to map a row from file to a User object
class TestUserRowMapper(RowMapper[TestUser]):
    def mapRow(self, rowData) -> TestUser:
        # print(f"Mapping row data to User ${rowData}")
        # Map each col to the corresponding fiels in User object
        return TestUser(int(rowData[0]), rowData[1], int(rowData[2]))
    
    def recordCount(self) -> int:
        # User mapper expects at least 3 fields
        return 3

# Implementation of the SQL DB Processor for a User Object
class TestUserProcessor(SQLDBRowProcessor[TestUser]):
    def __init__(self, batchsize: int, cursor: pyodbc.Cursor) -> None:
        super().__init__(batchsize, cursor)

    def preProcess(self):
        # First Delete all rows from the table
        self.cursor.execute("delete from USERS")

    def getFileType(self) -> str:
        return "User"
        
    def processDBRow(self, obj: TestUser):
        # Insert each user object into DB
        sql: str = "INSERT INTO USERS(id, name, salary)  values(?, ?, ?)"
        self.cursor.execute(sql, (obj.id, obj.name, obj.salary))
        