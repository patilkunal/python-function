import csv
import logging
from io import StringIO

from common.interfaces import Reader, RowMapper, RowProcessor

# Implementation of Reader interface to CSV files, 
# mapper -> map each line to a domain object
# processor -> processes the mapped domain object
class CSVFileReader(Reader):
    logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self, filename: str, rowMapper: RowMapper, rowProcessor: RowProcessor, skipHeader: bool) -> None:
        self.filename = filename
        self.mapper = rowMapper
        self.processor = rowProcessor
        self.skipHeader = skipHeader
    
    def processRecords(self):
        with open(self.filename) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in csvreader:
                if(self.skipHeader):
                    self.skipHeader = False
                    continue
                if(len(row) >= self.mapper.recordCount()):
                    obj = self.mapper.mapRow(row)
                    self.processor.processRow(obj)
                else:
                    self.logger.error(f"Expected {self.mapper.recordCount()}, but got {len(row)}")
    
    def close(self):
        self.processor.close()

class CSVStringReader(Reader):
    logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self, csvdata: StringIO, rowMapper: RowMapper, rowProcessor: RowProcessor, skipHeader: bool) -> None:
        self.csvdata = csvdata
        self.mapper = rowMapper
        self.processor = rowProcessor
        self.skipHeader = skipHeader
    
    def processRecords(self):
        csvreader = csv.reader(self.csvdata, delimiter=',', quotechar='"')
        for row in csvreader:
            if(self.skipHeader):
                self.skipHeader = False
                continue
            if(len(row) >= self.mapper.recordCount()):
                obj = self.mapper.mapRow(row)
                self.processor.processRow(obj)
            else:
                self.logger.error(f"Expected {self.mapper.recordCount()}, but got {len(row)}")
    
    def close(self):
        self.processor.close()
