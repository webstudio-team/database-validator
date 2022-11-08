def main(aList, path):
    worklist = aList
    if worklist:
        print(f"{worklist[0]}")
        obj = Analysis(f"{path}\\{worklist[0]}", f"{path}\\{worklist[1]}")
        obj.task()
        return main(aList[2:], r"C:\Users\balazia\Ukoly\prepis\Automation\DataComparison\Output\customAnalysis\Source")
worklist = os.listdir(r"C:\Users\balazia\Ukoly\prepis\Automation\DataComparison\Output\customAnalysis\Source")

main(worklist, r"C:\Users\balazia\Ukoly\prepis\Automation\DataComparison\Output\customAnalysis\Source")
