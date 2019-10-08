import pandas
import re

def getSchemas():
    print("Reading Data from CSV File")
    df = pandas.read_csv('redshiftTables.csv', index_col="schema")
    dfSort = df.sort_index()

    schemas = dfSort.index.unique()

    sumsList = []
    schemaList = []
    for schema in schemas:
        schemaSize = dfSort.loc[schema, "size"]
        schemaList.append(schema)
        try:
            print(sum(schemaSize.tolist()))
            sumsList.append(sum(schemaSize.tolist()))
        except:
            print(schemaSize)
            sumsList.append(schemaSize)

    print(schemaList)
    print(sumsList)

    file = open("redshiftTables_Schemas.csv","w")
    lines = ["Name,Current Size (GB),Notes - complete if schema still required\n"]
    totalSize = 0
    for i in range(len(schemaList)):
        sizeInGB = format((sumsList[i]/1000), '.2f')
        totalSize += sumsList[i]/1000
        lines.append(schemaList[i] + "," + sizeInGB + "\n")
    lines.append("," + format(totalSize, '.2f') + "\n")

    file.writelines(lines)
    file.close()
    print("Successfully created schemas csv file")


def getTablesTemp():
    print("Reading Data from CSV File")
    df = pandas.read_csv('redshiftTables.csv')

    tables = df["table"].tolist()
    schemas = df["schema"].tolist()
    sizes = df["size"].tolist()
  
    schema_temp = []
    tables_temp = []
    sizes_temp = []
    for i in range(len(tables)):
        if "temp" in tables[i]:
            tables_temp.append(tables[i])
            schema_temp.append(schemas[i])
            sizes_temp.append(sizes[i])

    print(tables_temp)
    print(schema_temp)
    print(sizes_temp)

    file = open("redshiftTables_Temp.csv","w")
    lines = ["Schema,Table,Current Size (GB),Notes - complete if schema still required\n"]
    totalSize = 0
    for i in range(len(tables_temp)):
        sizeInGB = format((sizes_temp[i]/1000), '.2f')
        totalSize += sizes_temp[i]/1000
        lines.append(schema_temp[i] + "," + tables_temp[i] + "," + sizeInGB + "\n")
    lines.append(",," + format(totalSize, '.2f') + "\n")

    file.writelines(lines)
    file.close()
    print("Successfully created temp csv file")


def getTablesTimestamps():
    print("Reading Data from CSV File")
    df = pandas.read_csv('redshiftTables.csv')

    tables = df["table"].tolist()
    schemas = df["schema"].tolist()
    sizes = df["size"].tolist()
  
    schema_timestamp = []
    tables_timestamp = []
    sizes_timestamp = []
    for i in range(len(tables)):
        if re.search("\d\d\d\d", tables[i]):
            tables_timestamp.append(tables[i])
            schema_timestamp.append(schemas[i])
            sizes_timestamp.append(sizes[i])

    print(tables_timestamp)
    print(schema_timestamp)
    print(sizes_timestamp)

    file = open("redshiftTables_Timestamp.csv","w")
    lines = ["Schema,Table,Current Size (GB),Notes - complete if schema still required\n"]
    totalSize = 0
    for i in range(len(tables_timestamp)):
        sizeInGB = format((sizes_timestamp[i]/1000), '.2f')
        totalSize += sizes_timestamp[i]/1000
        lines.append(schema_timestamp[i] + "," + tables_timestamp[i] + "," + sizeInGB + "\n")
    lines.append(",," + format(totalSize, '.2f') + "\n")

    file.writelines(lines)
    file.close()
    print("Successfully created timestamps csv file")


def getTablesTest():
    print("Reading Data from CSV File")
    df = pandas.read_csv('redshiftTables.csv')

    tables = df["table"].tolist()
    schemas = df["schema"].tolist()
    sizes = df["size"].tolist()
  
    schema_test = []
    tables_test = []
    sizes_test = []
    for i in range(len(tables)):
        if "test" in tables[i]:
            tables_test.append(tables[i])
            schema_test.append(schemas[i])
            sizes_test.append(sizes[i])

    print(tables_test)
    print(schema_test)
    print(sizes_test)

    file = open("redshiftTables_Test.csv","w")
    lines = ["Schema,Table,Current Size (GB),Notes - complete if schema still required\n"]
    totalSize = 0
    for i in range(len(tables_test)):
        sizeInGB = format((sizes_test[i]/1000), '.2f')
        totalSize += sizes_test[i]/1000
        lines.append(schema_test[i] + "," + tables_test[i] + "," + sizeInGB + "\n")
    lines.append(",," + format(totalSize, '.2f') + "\n")

    file.writelines(lines)
    file.close()
    print("Successfully created test csv file")


getSchemas()
getTablesTemp()
getTablesTimestamps()
getTablesTest()