"""
coding:utf-8
SPRING 2022 -CIS 493/593 – BIG DATA LAB ASSIGNMENT - 2
Transforming Big Data in JSON (JavaScript Object Notation) Format as Semi-Structured Data
Author: Sabareeswaran Shanmugam
•	Scripting language – python 3.7
•	Database – MySQL Workbench 8.0
•	IDE - PyCharm 2021.3.1 (Professional Edition)
    Runtime version: 11.0.13+7-b1751.21 x86_64
    VM: OpenJDK 64-Bit Server VM by JetBrains s.r.o.
    macOS 12.1
    GC: G1 Young Generation, G1 Old Generation
    M   emory: 2048M
    Cores: 16
    Non-Bundled Plugins:
    DBN (3.3.1249.0)
"""
import csv
import re
import os
from validate import *
from StoreDatatoMysql import bulkInsert
from StoreDatatoMysql_ExtraCredit import bulkinsert_ExraCredit

absolute_path = os.path.dirname(os.path.abspath(__file__))

#Logic: path to store the csv files based on the dataset
#100Json dataset under /CSV_Files
#~200,000 dataset under /CSV_Files_ExtraCredit Directory.
#Handling very big dataset is  organizing properly and meaningfully.

#Create CSV File and Stores 'BusinessMain' data.
def StoreBusinessStructuredData(text, dataset):
    matched=re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match =bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/BusinessDataMain_EC.csv'
    else:
        directory='/CSV_Files'
        CsvfileName ='/BusinessDataMain_100.csv'

    with open(absolute_path + directory + CsvfileName, 'a+',
              newline='') as myCsvFile:
        headerList= ["business_id", "full_address", "open", "city", "review_count", "name", "longitude", "state",
                   "stars",
                   "latitude", "type"]
        file_is_empty_1 = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, fieldnames=headerList)
        if file_is_empty_1:
            writer.writeheader()
        writer.writerow(text)
        #print('write')
        myCsvFile.close()
#Create CSV File and Stores 'Category' data.
def StoreCategoriesStructuredData(text,dataset):
    matched = re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match = bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/CategoriesData_EC.csv'
    else:
        directory = '/CSV_Files'
        CsvfileName = '/CategoriesData_100.csv'
    with open(absolute_path + directory + CsvfileName, 'a+', newline='') as myCsvFile:
        headerList = ["business_id", "categories"]
        file_is_empty_2 = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, delimiter=',', fieldnames=headerList)
        if file_is_empty_2:
            writer.writeheader()
        writer.writerow(text)
        myCsvFile.close()

#Create CSV File and Stores 'NeighBorhood' data.
def StoreNeighBorhoodStructuredData(text,dataset):
    matched = re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match = bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/NeighborhoodData_EC.csv'
    else:
        directory = '/CSV_Files'
        CsvfileName = '/NeighborhoodData_100.csv'

    with open(absolute_path + directory + CsvfileName, 'a+', newline='') as myCsvFile:
        headerList= ["business_id", "neighborhoods"]
        file_is_empty = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, fieldnames=headerList)
        if file_is_empty:
            writer.writeheader()
        writer.writerow(text)
        myCsvFile.close()

#Create CSV File and Stores 'Hours' data.
def StoreHoursStructuredData(text,dataset):
    matched = re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match = bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/BusinessHoursData_EC.csv'
    else:
        directory = '/CSV_Files'
        CsvfileName = '/BusinessHoursData_100.csv'
    with open(absolute_path + directory + CsvfileName, 'a+', newline='') as myCsvFile:
        headerList= ["business_id", "hours", "open", "close"]
        file_is_empty = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, fieldnames=headerList)
        if file_is_empty:
            writer.writeheader()
        writer.writerow(text)
        myCsvFile.close()


#Create CSV File and Stores 'Attributes' data.
def StoreAttributesStructuredData(text,dataset):
    matched = re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match = bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/BusinessAttributesData_EC.csv'
    else:
        directory = '/CSV_Files'
        CsvfileName = '/BusinessAttributesData_100.csv'
    with open(absolute_path + directory + CsvfileName, 'a+', newline='') as myCsvFile:
        headerList= ["business_id", "attribute", "value"]
        file_is_empty = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, fieldnames=headerList)
        if file_is_empty:
            writer.writeheader()
        writer.writerow(text)
        myCsvFile.close()

#Create CSV File and Stores 'Good_for_Attributes' data.
def StoreGoodForAttributeStructuredData(text,dataset):
    matched = re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match = bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/BusinessGoodForAttributesData_EC.csv'
    else:
        directory = '/CSV_Files'
        CsvfileName = '/BusinessGoodForAttributesData_100.csv'
    with open(absolute_path + directory + CsvfileName, 'a+', newline='') as myCsvFile:
        headerList = ["business_id", "Good For", "value"]
        file_is_empty = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, fieldnames=headerList)
        if file_is_empty:
            writer.writeheader()
        writer.writerow(text)
        myCsvFile.close()


#Create CSV File and Stores Parking' data.
def StoreParkingAttributeStructuredData(text,dataset):
    matched = re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match = bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/BusinessParkingAttributesData_EC.csv'
    else:
        directory = '/CSV_Files'
        CsvfileName = '/BusinessParkingAttributesData_100.csv'
    with open(absolute_path + directory + CsvfileName, 'a+', newline='') as myCsvFile:
        headerList = ["business_id", "Parking", "value"]
        file_is_empty = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, fieldnames=headerList)
        if file_is_empty:
            writer.writeheader()
        writer.writerow(text)
        myCsvFile.close()

#Create CSV File and Stores Ambience data.
def StoreAmbienceAttributeDict(text,dataset):
    matched = re.findall(r'Dataset_ExrtaCredit', dataset)
    is_match = bool(matched)
    if is_match == True:
        directory = '/CSV_Files_ExtraCredit'
        CsvfileName = '/BusinessAmbienceAttributesData_EC.csv'
    else:
        directory = '/CSV_Files'
        CsvfileName = '/BusinessAmbienceAttributesData_100.csv'

    with open(absolute_path + directory + CsvfileName, 'a+', newline='') as myCsvFile:
        headerList = ["business_id", "Ambience", "value"]
        file_is_empty = os.stat(absolute_path + directory + CsvfileName).st_size == 0
        writer = csv.DictWriter(myCsvFile, fieldnames=headerList)
        if file_is_empty:
            writer.writeheader()
        writer.writerow(text)
        myCsvFile.close()

# Json Parser - Parsing using Python dictionary Key Value -pairs.
# filename path string data is passed as argument to the SelectDataset
def SelectDataset(filepath):
    ''' Declaring python dictionary for each Multivalued Attributes seperately
        To Convert the Data into 1NormalForm and append data later'''
    BusinessDict = {}
    CategoriesDict = {}
    NeighBorhoodDict = {}
    HoursDict = {}
    AttributeDict = {}
    GoodForAttributeDict = {}
    ParkingAttributeDict = {}
    AmbienceAttributeDict = {}
    '''Parsing Nested Json
     Credits :https://bcmullins.github.io/parsing-json-python/
     '''
    with open(absolute_path + filepath, 'r') as f:
        data_dict = json.load(f)
        for (k, v) in data_dict.items():
            for values in v:
                for key in values:
                    if str(key) == 'business_id':
                        HoursDict[str(key)] = values[key]
                        CategoriesDict[str(key)] = values[key]
                        NeighBorhoodDict[str(key)] = values[key]
                        AttributeDict[str(key)] = values[key]
                        GoodForAttributeDict[str(key)] = values[key]
                        ParkingAttributeDict[str(key)] = values[key]
                        AmbienceAttributeDict[str(key)] = values[key]
                    tempDict = values[key]
                    if isinstance(tempDict, list):
                        for k in tempDict:
                            if key == 'categories':
                                CategoriesDict[key] = k
                                StoreCategoriesStructuredData(CategoriesDict,filepath)
                            elif key == 'neighborhoods':
                                NeighBorhoodDict[key] = k
                                StoreNeighBorhoodStructuredData(NeighBorhoodDict,filepath)
                    elif isinstance(tempDict, dict):
                        for k in tempDict:
                            tempDict2 = tempDict[k]
                            if isinstance(tempDict2, dict):
                                if key == 'hours':
                                    for i in tempDict2:

                                        HoursDict['hours'] = k
                                        HoursDict[i] = tempDict2[i]
                                        #Appending the data to csv file
                                    StoreHoursStructuredData(HoursDict,filepath)
                                elif key == 'attributes':
                                    if k == 'Good For':
                                        for i in tempDict2:
                                            GoodForAttributeDict['Good For'] = i
                                            GoodForAttributeDict['value'] = tempDict2[i]
                                            # Appending the data to csv file
                                            StoreGoodForAttributeStructuredData(GoodForAttributeDict,filepath)
                                    elif k == 'Parking':
                                        for i in tempDict2:
                                            ParkingAttributeDict['Parking'] = i
                                            ParkingAttributeDict['value'] = tempDict2[i]
                                            # Appending the data to csv file
                                            StoreParkingAttributeStructuredData(ParkingAttributeDict,filepath)
                                    elif k == 'Ambience':
                                        for i in tempDict2:
                                            AmbienceAttributeDict['Ambience'] = i
                                            AmbienceAttributeDict['value'] = tempDict2[i]
                                            # Appending the data to csv file
                                            StoreAmbienceAttributeDict(AmbienceAttributeDict,filepath)
                            else:

                                AttributeDict['attribute'] = k
                                AttributeDict['value'] = tempDict[k]
                                # Appending the data to csv file
                                StoreAttributesStructuredData(AttributeDict,filepath)
                    else:

                        BusinessDict[str(key)] = str(tempDict).replace('\n', ' ')
                        # Appending the data to csv file
                StoreBusinessStructuredData(BusinessDict,filepath)

if __name__ == "__main__":
    '''Parsing Yelp Json Dataset into 1NF semi-structured Data
        1) business100ValidForm.json - contains only 100 records
        2) ExtracreditData.json - contains ~200,000 Business records #NOTE:This JsonFile is valid
            Validate.py --> This helper function is used to validate big  ~200,000 Business records JsonFile
     '''

    SelectDataset('/Dataset/business100ValidForm.json')
    bulkInsert('yelpBusinessData100')

    # if the Source Database not exit is handled by try-except blocks
    try:
        print("Processing ExtraCredit Dataset")
        SelectDataset('/Dataset_ExrtaCredit/validFormat/ExtracreditData.json')
    except IOError:
        print('Not a ValidJSON to Parse!')
        print('\nExecuting Validating scrip :)')
        validateExtraCreditDataset(absolute_path)
        print('\nValidation Done! Now parsing the ExtraCredit Dataset')
        SelectDataset('/Dataset_ExrtaCredit/validFormat/ExtracreditData.json')

    bulkinsert_ExraCredit('ExtraCredit_YelpBusinessData')
    print("Program End")


"""
Reference:
1.https://bcmullins.github.io/parsing-json-python/
2.https://www.projectpro.io/recipes/connect-mysql-python-and-import-csv-file-into-mysql-and-create-table
3.https://stackoverflow.com/questions/10154633/load-csv-data-into-mysql-in-python
"""


