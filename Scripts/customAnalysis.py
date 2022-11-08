from importlib.resources import path
import pandas as pd
from collections import Counter
import re
import sys
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_handler = logging.StreamHandler(sys.stdout)


class Analysis():

    def __init__(self, pathOne, pathTwo):
        self.pathOne = pathOne
        self.dFrameOne = pd.read_csv(pathOne, low_memory=False,encoding='UTF-8')
        self.dFrameTwo = pd.read_csv(pathTwo, low_memory=False,encoding='UTF-8')

    def variability(self, dataframe) ->dict:
        logger.info("Testing column variability of the dataframe.")
        columnVariability = {}
        for column in dataframe:
            variability = Counter(dataframe[column])
            variability = variability.most_common()
            columnVariability[column] = len(variability)
        return columnVariability

    def columnSorting(self):
        logger.info("Proceeding to column sorting.")
        variabilityFirst = self.variability(self.dFrameOne)
        variabilityFirst = variabilityFirst
        variabilityTransformedIndex = list(variabilityFirst.keys())
        print(variabilityTransformedIndex)
        variabilityTransformed = {}

        columnsFirst = self.dFrameOne.columns
        columnsSecond = self.dFrameTwo.columns

        columnIntersection = [col for col in columnsFirst if col in columnsSecond]

        for column in self.dFrameOne:
            if column not in columnIntersection:
                self.dFrameOne=self.dFrameOne.drop([column], axis=1)

        for column in self.dFrameTwo:
            if column not in columnIntersection:
                self.dFrameTwo=self.dFrameTwo.drop([column], axis=1)

        logger.info("Column intersection found.")
        for key in variabilityFirst:
            if key not in columnIntersection:
                del variabilityFirst[key]

        print(f"Columns variability:\n{variabilityFirst}")
        for x in range(len(variabilityFirst)):
            variabilityTransformed[x] = variabilityTransformedIndex[x]
        for key in variabilityTransformed:
            print(f"{key} : {variabilityTransformed[key]}")

        userInput = input("Sort the dataframes by entering numbers corresponding with columns, separated by commas: ").split(',')
        userInput = [int(x) for x in userInput]
        columnsToSortBy = [variabilityTransformed[x] for x in userInput]

        self.dFrameOne=self.dFrameOne.sort_values(by=columnsToSortBy, ascending=False)
        self.dFrameTwo=self.dFrameTwo.sort_values(by=columnsToSortBy, ascending=False)

        logger.info("Dataframes sorted.")
    
    def comparison(self):
        filename = input("Enter name of the file to save your output:  ")

        comparedFrames = self.dFrameOne.reset_index(drop=True).compare(self.dFrameTwo.reset_index(drop=True))
        comparedFrames.to_csv(f"C:\\Users\\balazia\\Ukoly\\prepis\\Automation\\DataComparison\\Output\\customAnalysis\\{filename}_comp.csv", encoding='UTF-8')

    
    def task(self):
        self.columnSorting()
        print(self.dFrameOne.shape[0])
        print(self.dFrameTwo.shape[0])
        self.comparison()


#zmena na concurrent flow?




        
        



        

