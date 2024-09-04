import azure.functions as func
import logging
import os
import pyodbc
from io import BytesIO, StringIO
from ftplib import FTP
from readers.csvreader import CSVStringReader
from processors.adpprocessor import ADPProjectMapper, ADPProjectProcessor

app = func.FunctionApp()

filedata = BytesIO()
def appendData(moreData):
    filedata.write(moreData)

def getFtpData(ftphost: str, ftpport: int, ftpuser: str, ftppass: str, ftpfile: str):
    if(ftphost is None or ftpuser is None or ftppass is None or ftpfile is None):
        logging.error("Unable to find value for ftp host or user or password or file. Exiting.")
        return None
    logging.info(f"Going to connect to {ftphost}")
    ftp = FTP(ftphost)    
    try:
        ftp.connect(host=ftphost, port=ftpport)
        ftp.login(user=ftpuser, passwd=ftppass)
        resp = ftp.retrbinary(cmd='retr ' + ftpfile, callback=appendData)
        logging.info("FTP Response code " + resp)
    except Exception as err:
        logging.error(f"File {ftpfile} not found on the server")
        return None
    finally:
        logging.info("Logging off FTP Server")
        ftp.quit()
    filedata.flush()
    filedata.seek(0)
    logging.info('Retrieved FTP file.')
    databytes = filedata.read()
    return databytes.decode('utf-8')


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="adpHoursTimer", run_on_startup=True,
              use_monitor=False)
@app.function_name(name="AdpHoursFunction") 
def processADPHoursData(adpHoursTimer: func.TimerRequest) -> None:
    dburl = os.environ['AZURE_SQL_CONNECT_STRING']
    if(dburl is None):
        logging.error("Unable to find DB connection setting.")
        exit(1)
    try:
        logging.info('Started executing file import via FTP.')
        ftphost = os.environ['JTS_FTP_HOST']
        ftpport = int(os.environ.get('JTS_FTP_PORT', 21))
        ftpuser = os.environ['JTS_FTP_USER']
        ftppass = os.environ['JTS_FTP_PASSWD']
        filetodownload = os.environ['ADP_HOURS_FILE_NAME']
        strdata = getFtpData(ftphost, ftpport, ftpuser, ftppass, filetodownload)
        if(strdata is None):
            logging.error("Unable to get data from FTP server")
            return
        # logging.info("Got the data of size " + strdata)
    except Exception as err:
        logging.error("Error getting file from FTP server {}:".format(err))
        return

    try:
        logging.info("Getting database connection")
        conn: pyodbc.Connection =  pyodbc.connect(dburl)
        logging.info("Started processing ADP Hours data")
        csvreader = CSVStringReader(StringIO(strdata), rowMapper=ADPProjectMapper(), rowProcessor=ADPProjectProcessor(2000,conn.cursor()), skipHeader=True)
        csvreader.processRecords()
        csvreader.close()
        logging.info("Successfully processed ADP Hours data")
        conn.commit()
        conn.close()
    except Exception as err:
        logging.error("Error processing ADP Hours data: {}".format(err))