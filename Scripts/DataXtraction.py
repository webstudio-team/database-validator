import pandas as pd
import pyodbc
import sys
import logging

#Logging configuration
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_handler = logging.StreamHandler(sys.stdout)

class DataXtractor:
    """
    !!! Configured for windows authentication for SQL Server !!!
    """
    def __init__(self, list_of_conx_string:list, second_database:str):
            #class constructor
            self.table1 = None
            self.table2 = None
            self.sqlStatement = "None"
            self.second_database = second_database
            self.workdict = {}

            #Connect to database
            logger.info(msg="Attempting to connect to database.")

            self.conx_string = f'DRIVER={list_of_conx_string[0]}; SERVER={list_of_conx_string[1]}; DATABASE={list_of_conx_string[2]}; TRUSTED_CONNECTION={list_of_conx_string[3]}'
            print(self.conx_string)
    
    def getTable1(self):
        return self.table1
    def getTable2(self):
        return self.table2
    
    def isDataEqual(self):
        if self.table1 == self.table2:
            return True
        else:
            return False       

    def query(self, table:str):        
        self.table1 = self.toDataframe(table)
            
    def querySecond(self, table:str):
        self.table2 = self.toDataframeSecond(table)
            

    def toDataframe(self, table:str):
        with pyodbc.connect(self.conx_string) as conx:
            get_data=("SELECT * FROM {tableName}".\
            format(second_database=self.second_database, tableName=table)) 
            dFrame = pd.read_sql(get_data,conx)

            return dFrame

    def toDataframeSecond(self, table:str):
        with pyodbc.connect(self.conx_string) as conx:
            get_data=("SELECT * FROM {second_database}.{tableName}".\
            format(second_database=self.second_database, tableName=table)) 
            dFrame = pd.read_sql(get_data,conx)

            return dFrame
            
    def toCsvFile(self, table:str):
        self.table1.to_csv(f"{table}.csv", index=False, chunksize=None)
        self.table2.to_csv(f"{table}Second.csv", index=False,chunksize=None)
    
    
    def searchByPattern(self,table_list:list):
        file_patterns=input("Enter a file pattern to search: ")
        proceed = input("Continue or terminate filtering session? Any key to continue, [q] to terminate filtering session.")
        if proceed == 'q':
            logger.info(msg="Final pattern-matching initialised.")
            self.workdict[file_patterns] = [table for table in table_list if file_patterns.lower() in table.lower()]
        else:
            logger.info(msg="Pattern-matching initialised.")
            self.searchByPattern(table_list)
        self.workdict[file_patterns] = [table for table in table_list if file_patterns.lower() in table.lower()]
        

    def find_all_tables(self):
        try:
            with pyodbc.connect(self.conx_string) as conx:
                cursor = conx.cursor()
                logger.info(msg="Attempting to extract table names from the database.")
                table_list = [('['+row[1]+ ']' + '.' +'['+row[2]+ ']')for row in cursor.tables()]
        except Exception:
            logger.error("An error occured while extracting.")
            proceedToPatternFiltering = input("Proceed to next step [y] or repeat? [r]: ")

            if proceedToPatternFiltering.lower() == 'r':
                logger.info("Repeating the extraction process.")
                self.find_all_tables()
            else:
                logger.warning("Terminating the extraction process.")
                return

        else:
            logger.info(msg=f"Names succesfully extracted: {len(table_list)} tables found.")
        allVsPattern = input("Search by pattern [p] / Load all tables [a]:")
        while True:
            if allVsPattern == 'p':
                self.searchByPattern(table_list)
                break
            elif allVsPattern == 'a':
                self.workdict['All'] = table_list
                break
            else:
                print("Invalid character input.")
    
    def queryManually(self):
        print(self.workdict)
        self.tableToQuery = input("Enter a table name for querying: ")
        logger.info("Proceeding to queries.")
        try:
            self.query(self.tableToQuery)
            self.querySecond(self.tableToQuery)
            self.toCsvFile(self.tableToQuery)
        except Exception:
            logger.warning("An unknown error occured.")
        else:
            terminateContinue = input("End the process [q] or continue [any key].")
            if terminateContinue == 'q':
                return
        finally:
            logger.info("Proceeding to a new query.")
            self.queryManually()

    def queryAutomatically(self):
        print("Objects left for analysis:\n ")
        print(self.workdict)
        print('\n')
        for key in self.workdict:
            if len(self.workdict[key]) != 0:
                self.tableToQuery = self.workdict[key][0]
                print(f"\n Currently processing : {self.tableToQuery}\n")
                try:
                    self.query(self.tableToQuery)
                except pd.io.sql.DatabaseError or pyodbc.ProgrammingError:
                    logger.warning("\nAn error occured during query execution. Repeating step.")
                    self.queryAutomatically()
                try:
                    self.querySecond(self.tableToQuery)
                    self.toCsvFile(self.tableToQuery)
                except pd.io.sql.DatabaseError or pyodbc.ProgrammingError:
                    logger.warning("\nAn error occured during query execution. Repeating step.")
                    self.queryAutomatically()
                else:
                    self.workdict[key].remove(self.tableToQuery)
        else:
            print(f"\nNo objects left to analyze in {key} set. ")
          
    def queryExecution(self):
        self.manualVsAutomatic = input("Manual [m] / Automatic [a] table input. ")
        if self.manualVsAutomatic == 'a':
            self.queryAutomatically()
        elif self.manualVsAutomatic == 'm':
            self.queryManually()
        else:
            print("Invalid character input. Enter a valid character.")
            self.queryExecution()

    def workflow(self):
        self.find_all_tables()
        self.queryExecution()
