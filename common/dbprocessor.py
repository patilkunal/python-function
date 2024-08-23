import pyodbc
import abc
from typing import TypeVar
from common.interfaces import RowProcessor

T = TypeVar("T")

# extends the RowProcessor interface for a SQL DB specific interface
class SQLDBRowProcessor(RowProcessor[T]):
    def __init__(self, batchsize: int, cursor: pyodbc.Cursor) -> None:
        self.batchSize = batchsize
        self.cursor = cursor
        self.commitCount = 0
    
    @abc.abstractmethod
    def preProcess(self):
        raise NotImplementedError

    @abc.abstractmethod
    def getFileType(self) -> str:
        raise NotImplementedError
    
    @abc.abstractmethod
    def processDBRow(self, obj: T):
        raise NotImplementedError        

    def processRow(self, obj: T):
        self.processDBRow(obj)
        self.commitCount = self.commitCount + 1
        if(self.commitCount > self.batchSize):
            self.cursor.commit()
            self.commitCount = 0
    
    def close(self):
        self.cursor.commit()
        self.cursor.close()
