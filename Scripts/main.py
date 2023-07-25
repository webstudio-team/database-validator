from customAnalysis import Analysis
from DataXtraction import DataXtractor
from Report import Initialiser
import os

"""
------------Settings------------
"""

config = ['{SQL SERVER}', 'UZSSQL01', 'CovidDwhPROD', 'yes', 'MZNET\\uzubalazia' ]
second_db = '[AZURE SQL DATABASE].[syn-prod-coviddwh]'
path = r"C:\Users\balazia\Ukoly\Dashboard\prepis\Automation\DataComparison\Output\customAnalysis\Source"
toAnalyze = os.listdir(path)

def analyze(listOfFiles:list=toAnalyze):
    if listOfFiles:
        taskOne = Analysis(f"{path}\\{listOfFiles[0]}",f"{path}\\{listOfFiles[1]}")
        taskOne.workflow()

        return analyze(listOfFiles=listOfFiles[2:])
    
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

