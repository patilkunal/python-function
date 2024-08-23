from typing import List, TypeVar, Generic
import abc

T = TypeVar("T")

# Reader interface (would be implemented by file specific reader)
class Reader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def processRecords():
        raise NotImplementedError
    
    @abc.abstractmethod
    def close():
        raise NotImplementedError

# Mapper interface to map each row to a domain object 
# T -> Generic for a actual domain object
class RowMapper(Generic[T], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def mapRow(self, rowDataArray: list[str]) -> T:
        raise NotImplementedError
    
    @abc.abstractmethod
    def recordCount(self) -> int:
        raise NotImplementedError

# Processor interface to map each row from the file to a domain object
# T -> Generics for an actual domain object
class RowProcessor(Generic[T], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def processRow(self, obj: T):
        raise NotImplementedError
    
    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
