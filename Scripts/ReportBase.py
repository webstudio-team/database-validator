from DataXtraction import DataXtractor
import pyodbc
import pandas as pd
import logging
import sys

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_handler = logging.StreamHandler(sys.stdout)

class ReportBase(DataXtractor):

    def __init__(self,list_of_conx_string:list, second_database:str):
        super().__init__(list_of_conx_string=list_of_conx_string, second_database=second_database)
    
    def returnConnectionConfiguration(self):
        return self.conx_string
    
    def testConnection(self):
        try:
            logger.info("Attempting to establish a link between the sytem and the server.")
            pyodbc.connect(self.returnConnectionConfiguration)
        except pyodbc.ProgrammingError:
            logger.error("Could not establish the link.")
            return False
        else:
            logger.info("Connection succesfully established.")
            return True  

    def getRowCount(self, table:str):
        with pyodbc.connect(self.conx_string) as conx:
            cursor = conx.cursor()
            cursor.execute("SELECT COUNT(*) as row_count FROM {table}".format(table = table))
            row_count = cursor.fetchone()[0]
        return row_count

    def getRowCountSecond(self, table:str):
        with pyodbc.connect(self.conx_string) as conx:
            cursor = conx.cursor()
            cursor.execute("SELECT COUNT(*) as row_count FROM {second_db}.{table}".format(second_db=self.second_database,table = table))
            row_count = cursor.fetchone()[0]
        return row_count


    
    

        