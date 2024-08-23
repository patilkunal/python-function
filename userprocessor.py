from common.interfaces import RowMapper
from common.dbprocessor import SQLDBRowProcessor
import pyodbc

# Maps the row from the file to row in DB. 
# Acts as domain model object for code use
class User():
    def __init__(self, id: int, name: str, salary: int):
        self.id: int = id
        self.name: str = name
        self.salary: int = salary


# Implementation to map a row from file to a User object
class UserRowMapper(RowMapper[User]):
    def mapRow(self, rowData) -> User:
        print(f"Mapping row data to User ${rowData}")
        # Map each col to the corresponding fiels in User object
        return User(int(rowData[0]), rowData[1], int(rowData[2]))
    
    def recordCount(self) -> int:
        # User mapper expects at least 3 fields
        return 3

# Implementation of the SQL DB Processor for a User Object
class UserProcessor(SQLDBRowProcessor[User]):
    def __init__(self, batchsize: int, cursor: pyodbc.Cursor) -> None:
        super().__init__(batchsize, cursor)

    def preProcess(self):
        # First Delete all rows from the table
        self.cursor.execute("delete from dbo.USERS")

    def getFileType(self) -> str:
        return "User"
        
    def processDBRow(self, obj: User):
        # Insert each user object into DB
        sql: str = "INSERT INTO dbo.USERS(id, name, salary)  values(?, ?, ?)"
        self.cursor.execute(sql, obj.id, obj.name, obj.salary)
        