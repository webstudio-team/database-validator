from customAnalysis import Analysis
from DataXtraction import DataXtractor
from Report import Initialiser
import os

"""
------------Settings------------
"""

config = ""
second_db = ""
path = r""
toAnalyze = os.listdir(path)

def analyze(listOfFiles:list=toAnalyze):
    task = Analysis(f"{path}\\{listOfFiles[0]}",f"{path}\\{listOfFiles[1]}")
    task.workflow()

    if listOfFiles:
        return analyze(listOfFiles[2:])

def importCsv():
    return DataXtractor(config, second_db).workflow()

def validate():
    return Initialiser(config, second_db).runTime()

def main():
    procedureDict = {
        'x': importCsv,
        'a': analyze,
        'v': validate
    }

    stepToProceed = input("Import tables as csv files [x], analyze csv files[a], validate data[v].")
    procedure = procedureDict[stepToProceed]
    procedure()
main()

