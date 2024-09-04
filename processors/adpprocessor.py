from common.interfaces import RowMapper
from common.dbprocessor import SQLDBRowProcessor
import pyodbc

# DOMAIN OBJECT
class ADPProject():
    def __init__(self, department:str, client:str, project:str, sub_project:str):
        self.department = department
        self.client = client
        self.project = project
        self.sub_project = sub_project

# DOMAIN OBJECT MAPPER
class ADPProjectMapper(RowMapper[ADPProject]):
    def mapRow(self, rowDataArray: list[str]) -> ADPProject:
        return ADPProject(rowDataArray[0], rowDataArray[1], rowDataArray[2], rowDataArray[3])
    
    def recordCount(self) -> int:
        return 4
    
class ADPProjectProcessor(SQLDBRowProcessor[ADPProject]):
    def __init__(self, batchsize: int, cursor: pyodbc.Cursor) -> None:
        super().__init__(batchsize, cursor)

    def preProcess(self):
        pass

    def getFileType(self) -> str:
        return "ADP Hours"

    def processDBRow(self, obj: ADPProject):
        sql = """
        INSERT INTO dbo.ProjectList(department,Client,Project,SubProject) 
        select ?, ? ,?, ? where not exists 
        (select * from dbo.ProjectList where department=? and client=? and project=? and SubProject=?)
        """
        self.cursor.execute(sql, (obj.department, obj.client, obj.project, obj.sub_project, obj.department, obj.client, obj.project, obj.sub_project))

