import pandas as pd
from DataXtraction import DataXtractor
import pyodbc
import logging
import sys
import asyncio

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_handler = logging.StreamHandler(sys.stdout)


class CustomAnalysisDeep(DataXtractor):
    def __init__(self, config, second_db):
        super().__init__(config, second_db)
    
    def definecustomQuery(self):
        queryToExecute = ""
        while True:
            userCustomQuery = input("Enter a line of your custom query: ")
            if userCustomQuery == "qq":
                return queryToExecute
            else:
                queryToExecute += " "
                queryToExecute += userCustomQuery

    def loadCustomQuery(self):
        self.customQuery = self.definecustomQuery()
        logger.info("Proceeding to second Query: ")
        self.customQuerySecond = self.definecustomQuery()

    def executeCustomQuery(self):
        try:
            with pyodbc.connect(self.conx_string) as conx:
                dFrameCustom = pd.read_sql(self.customQuery,conx)
        except pd.io.sql.DatabaseError or pyodbc.ProgrammingError:
            logger.warning("An unknown error occured during query exectuion. Repeating the step.")
            self.executeCustomQuery()
        else:
            print("Custom query successfully executed.")

        try:
            with pyodbc.connect(self.conx_string) as conx:
                dFrameCustomSecond = pd.read_sql(self.customQuerySecond, conx)
        except pd.io.sql.DatabaseError or pyodbc.ProgrammingError:
            logger.warning("An unknown error occured during query exectuion. Repeating the step.")
            self.executeCustomQuery()
        else:
            print("Custom query successfully executed.")
        
        if len(dFrameCustom.index) == len(dFrameCustomSecond.index):
            fileNames = input("Row count in the dataframes equal - enter the name of csv file to save it. File can be further analysed via analysis module.")
            comparison = dFrameCustom.reset_index(drop=True).compare(dFrameCustomSecond.reset_index(drop=True))
            comparison.to_csv(f"Automation\\DataComparison\\Output\\customAnalysis\\{fileNames}_comparison.csv",encoding="utf-8")

            with open(f"{fileNames}.txt", "w") as file:
                file.write(f"Executed Query\n\n {self.customQuery}\n\n Second Executed Query\n\n {self.customQuerySecond} ") 
        else:
            fileName = input("Row count in the dataframes not equal - enter the name of csv file to save it.")
            dFrameCustom.to_csv(f"C:\\Users\\balazia\\Ukoly\\prepis\\Automation\\DataComparison\\Output\\customAnalysis\\{fileName}.csv", encoding="UTF-8")
            dFrameCustomSecond.to_csv(f"C:\\Users\\balazia\\Ukoly\\prepis\\Automation\\DataComparison\\Output\\customAnalysis\\{fileName}Second.csv", encoding="UTF-8")
            with open(f"{fileName}.txt", "w") as file:
                file.write(f"Executed Query\n\n {self.customQuery}\n\n Second Executed Query\n\n {self.customQuerySecond} ") 

        
    def customQs(self):
        self.loadCustomQuery()
        self.executeCustomQuery()

"""   
    async def customQs(self):
        self.loadCustomQuery()
        await asyncio.sleep()
        self.executeCustomQuery()

async def workflow():
    tasks=[CustomAnalysisDeep(['{SQL SERVER}', 'UZSSQL01', 'CovidDwhDEV', 'yes' ], '[AZURE SQL DATABASE].[syn-prod-coviddwh]').customQs()\
         for _ in range(5)]
    await asyncio.gather(*tasks)
    

loop = asyncio.get_event_loop()
loop.run_until_complete(workflow())
"""

test = CustomAnalysisDeep(['{SQL SERVER}', 'UZSSQL01', 'CovidDwhDEV', 'yes' ], '[AZURE SQL DATABASE].[syn-prod-coviddwh]')
test.customQs()
    
