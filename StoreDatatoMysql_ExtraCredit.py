"""
SPRING 2022 -CIS 493/593 – BIG DATA LAB ASSIGNMENT - 2
Transforming Big Data in JSON (JavaScript Object Notation) Format as Semi-Structured Data
Author: Sabareeswaran Shanmugam

ExtraCredit Part Insertion to MYSQL
This helper python file is used to Import the CSV_File created from ~200,000 JsonData to 'ExtraCredit_YelpBusinessData' MYSQLDB.
"""

import mysql.connector
import os
import pandas as pd
from mysql.connector import Error

def bulkinsert_ExraCredit(Db2_name):
    # Database connection
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    try:
        conn = mysql.connector.connect(host='localhost',
                                       user='root',
                                       password='qwerty60',
                                       port='3306',
                                       database=Db2_name,
                                       auth_plugin='mysql_native_password')
        if conn.is_connected():
            cursor = conn.cursor(buffered=True)
            cursor.execute("select database();")
            print("Successfully connected to database: ")
            print("\n Creating Tables in ExtraCredit_YelpBusinessData")
            # Table creating automatically with respect to the Output CSV Files.
            Table_Create_BusinessDataMain_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessDataMain`( business_id VARCHAR(60) NOT NULL,full_address LONGTEXT NULL,' \
                                               'open VARCHAR(5) NULL, city VARCHAR(45) NULL, review_count VARCHAR(60) NULL,name VARCHAR(60) NULL,longitude VARCHAR(60) NULL,' \
                                               'state VARCHAR(45) NULL,stars VARCHAR(60) NULL,latitude VARCHAR(60) NULL,type VARCHAR(60) NULL,PRIMARY KEY (business_id))'

            Table_Create_BusinessAttributesData_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessAttributesData`(business_id VARCHAR(60),' \
                                                     'attribute VARCHAR(60), value VARCHAR(45) ,PRIMARY KEY (business_id, attribute ))'

            Table_Create_BusinessGoodForAttributesData_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessGoodForAttributesData`(business_id VARCHAR(60),' \
                                                            'good_for VARCHAR(60), value VARCHAR(45) ,PRIMARY KEY (business_id,good_for))'

            Table_Create_BusinessAmbienceAttributesData_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessAmbienceAttributesData`(business_id VARCHAR(60),' \
                                                             'ambience VARCHAR(60), value VARCHAR(45), PRIMARY KEY (business_id ,ambience))'

            Table_Create_BusinessHoursData_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessHoursData`(business_id VARCHAR(60),' \
                                                'hours VARCHAR(60), open VARCHAR(45) ,close VARCHAR(45) ,PRIMARY KEY (business_id,hours))'

            Table_Create_BusinessParkingAttributesData_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessParkingAttributesData`(business_id VARCHAR(60),' \
                                                            'Parking VARCHAR(60), value VARCHAR(45), PRIMARY KEY (business_id,parking))'

            Table_Create_CategoriesData_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessCategoriesData`(business_id VARCHAR(60),' \
                                             'categories VARCHAR(60), PRIMARY KEY (business_id,categories))'

            Table_Create_NeighborhoodData_EC = 'CREATE TABLE IF NOT EXISTS `ExtraCredit_YelpBusinessData`.`BusinessNeighborhoodData`(business_id VARCHAR(60),' \
                                               'Neighborhood VARCHAR(60) ,PRIMARY KEY (business_id, neighborhood))'

            # MYSQL Query for Table Creation
            cursor.execute(Table_Create_BusinessDataMain_EC)
            cursor.execute(Table_Create_BusinessAttributesData_EC)
            cursor.execute(Table_Create_BusinessGoodForAttributesData_EC)
            cursor.execute(Table_Create_BusinessAmbienceAttributesData_EC)
            cursor.execute(Table_Create_BusinessHoursData_EC)
            cursor.execute(Table_Create_BusinessParkingAttributesData_EC)
            cursor.execute(Table_Create_CategoriesData_EC)
            cursor.execute(Table_Create_NeighborhoodData_EC)

            # Using Pandas DataFrame ,Inserting data to the respective tables
            business_csv_data = pd.read_csv(absolute_path + '/CSV_Files_ExtraCredit/BusinessDataMain_EC.csv',
                                            header=None)
            business_csv_data = business_csv_data.where((pd.notnull(business_csv_data)), None)
            b_m = pd.DataFrame(business_csv_data)
            attribute_csv_data = pd.read_csv(absolute_path + '/CSV_Files_ExtraCredit/BusinessAttributesData_EC.csv',
                                             header=None)
            a_m = pd.DataFrame(attribute_csv_data)
            good_for_csv_data = pd.read_csv(
                absolute_path + '/CSV_Files_ExtraCredit/BusinessGoodForAttributesData_EC.csv', header=None)
            g_f = pd.DataFrame(good_for_csv_data)
            Ambience_csv_data = pd.read_csv(
                absolute_path + '/CSV_Files_ExtraCredit/BusinessAmbienceAttributesData_EC.csv', header=None)
            amb_a = pd.DataFrame(Ambience_csv_data)
            Hours_csv_data = pd.read_csv(absolute_path + '/CSV_Files_ExtraCredit/BusinessHoursData_EC.csv', header=None)
            hr_d = pd.DataFrame(Hours_csv_data)
            Parking_csv_data = pd.read_csv(
                absolute_path + '/CSV_Files_ExtraCredit/BusinessAmbienceAttributesData_EC.csv', header=None)
            p_d = pd.DataFrame(Parking_csv_data)
            Categories_csv_data = pd.read_csv(absolute_path + '/CSV_Files_ExtraCredit/CategoriesData_EC.csv',
                                              header=None)
            c_d = pd.DataFrame(Categories_csv_data)
            Neighboorhood_csv_data = pd.read_csv(absolute_path + '/CSV_Files_ExtraCredit/NeighborhoodData_EC.csv',
                                                 header=None)
            n_hood = pd.DataFrame(Neighboorhood_csv_data)

            # insert Business_Main_EC
            for i, row in b_m.iterrows():
                sqlQ1 = "INSERT IGNORE INTO BusinessDataMain VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sqlQ1, tuple(row))
                conn.commit()
            # insert attribute_Main_EC
            for i, row in a_m.iterrows():
                sqlQ2 = "INSERT IGNORE INTO BusinessAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ2, tuple(row))
                conn.commit()
            # insert good_for_data_EC
            for i, row in g_f.iterrows():
                sqlQ3 = "INSERT IGNORE INTO BusinessGoodForAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ3, tuple(row))
                conn.commit()
            # insert ambience_data_EC
            for i, row in amb_a.iterrows():
                sqlQ4 = "INSERT IGNORE INTO BusinessAmbienceAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ4, tuple(row))
                conn.commit()
            # insert Hours_data_EC
            for i, row in hr_d.iterrows():
                sqlQ5 = "INSERT IGNORE INTO BusinessHoursData VALUES(%s,%s,%s,%s)"
                cursor.execute(sqlQ5, tuple(row))
                conn.commit()
            # insert parking data_EC
            for i, row in p_d.iterrows():
                sqlQ6 = "INSERT IGNORE INTO BusinessParkingAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ6, tuple(row))
                conn.commit()
            # insert categories_data_EC
            for i, row in c_d.iterrows():
                sqlQ7 = "INSERT IGNORE INTO BusinessCategoriesData VALUES(%s,%s)"
                cursor.execute(sqlQ7, tuple(row))
                conn.commit()
            # insert Neighborhood_data_EC
            for i, row in n_hood.iterrows():
                sqlQ8 = "INSERT IGNORE INTO BusinessNeighborhoodData VALUES(%s,%s)"
                cursor.execute(sqlQ8, tuple(row))
                conn.commit()

            cursor.close()
            print("Inserting done Successfully")
    except Error as e:
        print("Error while connecting to MySQL", e)


"""
Reference:
1.https://www.projectpro.io/recipes/connect-mysql-python-and-import-csv-file-into-mysql-and-create-table
"""




