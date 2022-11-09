from sqlalchemy import column
from ReportBase import ReportBase
from collections import Counter
import pandas as pd
import logging
import pyodbc
import sys
import os

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_handler = logging.StreamHandler(sys.stdout)

class Report(ReportBase):
    def __init__(self, coConfig:list, second_database:str, marker:str=None):
        super().__init__(list_of_conx_string=coConfig, second_database=second_database)
        self.marker = marker
        self.dFrameOne = None
        self.dFrameTwo = None
        self.reportDict = None
        self.columnsOne = None
        self.columnsTwo = None
    
    def reportDictFlip(self, valueIndex:int, value):
        self.reportDict[self.marker][valueIndex] = value
    def reportDictFlipSecond(self, valueIndex: int, value):
        self.reportDict[f"{self.marker}_second"][valueIndex] = value
    
    def attributeDictFlip(self, dictionary:dict, column: str, valueIndex: int, value):
        dictionary[column][valueIndex] = value

    def connection(self):
        establishedConnection = self.testConnection()
        if establishedConnection == True:
            print("You may proceed to analysis.")
        else:
            print("Connection not established. Check the configuration settings.")

    def loadToDataframe(self, table:str):
        self.dFrameOne = self.toDataframe(table)
    def loadToDataframeSecond(self,table:str):
        self.dFrameTwo = self.toDataframeSecond(table)

    def getData(self, table:str):
        try:
            self.loadToDataframe(table)
            self.loadToDataframeSecond(table)
        except pd.io.sql.DatabaseError or pyodbc.ProgrammingError:
            logging.warning("An unknown error occured. Repeating the step.")
            self.getData(table)
        else:
            if self.dFrameOne is None or self.dFrameTwo is None:
                self.getData(table)

    def testColumns(self):
        self.reportDict = {
            'Attributes': ["Length", "RowCountEqual", "RowCount"],
            self.marker : [0, 0, 0],
            f"{self.marker}_second": [0, 0, 0]
        }

        self.columnsOne = list(self.dFrameOne.columns)
        self.columnsTwo = list(self.dFrameTwo.columns)

        self.columnIntersection = [column for column in self.columnsOne if column in self.columnsTwo]

        if len(self.columnsOne) == len(self.columnsTwo):
            self.reportDictFlip(0, 1)
            self.reportDictFlipSecond(0,1)
        else:
            logger.warning("Column length not the same.")

        if self.dFrameOne.shape[0] == self.dFrameTwo.shape[0]:
            self.reportDictFlip(1, 1)
            self.reportDictFlipSecond(1, 1)
        else:
            logger.warning("Row count different.")
        
        self.reportDictFlip(2, self.dFrameOne.shape[0])
        self.reportDictFlipSecond(2, self.dFrameTwo.shape[0])
        logger.info("Creating report...")
        
        
    def report(self):
        self.testColumns()
        attributeDict = {'Attributes': ["Unique", "Null", "Variability", "Empty"]}
        
        for i in range(len(self.columnIntersection)):
            attributeDict[self.columnIntersection[i]] = [0,0,0,0]
            attributeDict[f"{self.columnIntersection[i]}_second"] = [0,0,0,0]
        
        logger.info("Performing analysis on shared columns")
        
        for column in self.columnIntersection:
            logger.info(f"Performing tests on column {column}")
            variability = Counter(self.dFrameOne[column])
            distinctVar = self.dFrameOne[column].unique()
            variabilityUniqueCount = len(list(filter(lambda x: variability[x] == 1 and str(x) != 'nan', variability)))
            nulls = (self.dFrameOne[column].isna()).sum()
            
            
            self.attributeDictFlip(attributeDict,column, 0, variabilityUniqueCount)
            self.attributeDictFlip(attributeDict,column, 1, nulls)
            self.attributeDictFlip(attributeDict,column, 2, len(distinctVar))

            if len(self.dFrameOne[column]) == 0 :
                self.attributeDictFlip(attributeDict,column, 3, 1)
            else:
                self.attributeDictFlip(attributeDict,column, 3, 0)
        
            logger.info(f"Performing tests on column {column}_second")
            variability = Counter(self.dFrameTwo[column])
            logger.info("Value Counter initialised")
            distinctVar = self.dFrameTwo[column].unique()
            logger.info("Distinct value identification finished")
            variabilityUniqueCount = len(list(filter(lambda x: variability[x] == 1 and str(x) != 'nan', variability)))

            logger.info("Checking nulls")
            nulls = (self.dFrameTwo[column].isna()).sum()
            logger.info("Nulls checked")
            self.attributeDictFlip(attributeDict,f"{column}_second", 0, variabilityUniqueCount) 
            self.attributeDictFlip(attributeDict,f"{column}_second", 1, nulls)
            self.attributeDictFlip(attributeDict,f"{column}_second", 2, len(distinctVar))

            if len(self.dFrameTwo[column]) == 0:
                self.attributeDictFlip(attributeDict,f"{column}_second", 3, 1)
            else:
                self.attributeDictFlip(attributeDict,f"{column}_second", 3, 0)

        columnsFirstNotSecond = list(filter(lambda x: x not in self.columnsTwo, self.columnsOne))
        columnsSecondNotFirst = list(filter(lambda x: x not in self.columnsOne, self.columnsTwo))

        logger.info("Proceeding to outer column analysis")
        columnDif = {}
        columnDif['FirstNotSecond'] = columnsFirstNotSecond
        columnDif['SecondNotFirst'] = columnsSecondNotFirst

        if len(columnsFirstNotSecond) > len(columnsSecondNotFirst):
            for i in range(len(columnsFirstNotSecond)-len(columnsSecondNotFirst)):
                columnsSecondNotFirst.append('-')
        elif len(columnsFirstNotSecond) < len(columnsSecondNotFirst):
            for i in range(len(columnsSecondNotFirst)-len(columnsFirstNotSecond)):
                columnsFirstNotSecond.append('-')

        csvReport=pd.DataFrame.from_dict(self.reportDict)
        csvReport.to_csv(f"C:\\Users\\balazia\\Ukoly\\prepis\\Automation\\DataComparison\\Output\\TextReports\\{self.marker}_general.csv")

        csvReport=pd.DataFrame.from_dict(attributeDict)
        csvReport.to_csv(f"C:\\Users\\balazia\\Ukoly\\prepis\\Automation\\DataComparison\\Output\\TextReports\\{self.marker}.csv")

        columnDiff = pd.DataFrame.from_dict(columnDif)
        columnDiff.to_csv(f"C:\\Users\\balazia\\Ukoly\\prepis\\Automation\\DataComparison\\Output\\TextReports\\{self.marker}_diff.csv")


    def workflow(self):
        self.getData(self.marker)
        print(f"Testing {self.marker}")
        self.report()


class Initialiser(ReportBase):
    def __init__(self, config, second_db):
        super().__init__(config, second_db)
        self.configRaw = config
        self.secondDbRaw = second_db
    

    def tableInits(self):
        with pyodbc.connect(self.conx_string) as conx:
                logger.info(msg="Connection established.")
                cursor = conx.cursor()
                logger.info(msg="Attempting to extract table names from the database.")
                return [('['+row[1]+ ']' + '.' +'['+row[2]+ ']')for row in cursor.tables()]
                
    def patternSearch(self):
        table_list = self.tableInits()
        patternInput = input("Enter a pattern to filter out tables for analysis or quit[q]")
        
        while patternInput != 'q':
            table_list = list(filter(lambda x: patternInput in x.lower(), table_list))
            patternInput = input("Enter a pattern to filter out tables for analysis or quit[q]")
        return table_list

    def organiser(self,tables:list):
        tables=tables
        tasks = [Report(self.configRaw, self.secondDbRaw,str(table)).workflow for table in tables]
        print(tasks)
        for task in tasks:
            task()

    def runTime(self):
        print(self.returnConnectionConfiguration())
        print(f"Test: {self.conx_string}")
        listOrType = input("Provide a pattern to filter through all the table names to compare [l] or enter manually[m]")
        if listOrType == 'm':
            tables = input("Enter names of the tables for analysis separated by comma. ")
            tables = tables.split(',')
        elif listOrType == 'l':
            tables = self.patternSearch()
        
        self.organiser(tables)


