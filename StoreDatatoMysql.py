"""
SPRING 2022 -CIS 493/593 – BIG DATA LAB ASSIGNMENT - 2
Transforming Big Data in JSON (JavaScript Object Notation) Format as Semi-Structured Data
Author: Sabareeswaran Shanmugam

This helper python file is used to Import the CSV_File,First 100 Json Data to 'yelpBusinessData100' MYSQLDB.
"""

import mysql.connector
import os
import pandas as pd
from mysql.connector import Error

def bulkInsert(Db_name):
    # Database connection
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    try:
        conn = mysql.connector.connect(host='localhost',
                                       user='root',
                                       password='qwerty60',
                                       port='3306',
                                       database=Db_name,
                                       auth_plugin='mysql_native_password')
        if conn.is_connected():
            cursor = conn.cursor(buffered=True)
            cursor.execute("select database();")
            print("Successfully connected to database: ")
            print("\n Creating Tables in yelpBusinessData100")

            # Table creating automatically with respect to the Output CSV Files.
            Table_Create_BusinessDataMain = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessDataMain`( business_id VARCHAR(60) NOT NULL,full_address LONGTEXT NULL,' \
                                            'open VARCHAR(5) NULL, city VARCHAR(45) NULL, review_count VARCHAR(60) NULL,name VARCHAR(60) NULL,longitude VARCHAR(60) NULL,' \
                                            'state VARCHAR(45) NULL,stars VARCHAR(60) NULL,latitude VARCHAR(60) NULL,type VARCHAR(60) NULL,PRIMARY KEY (business_id))'

            Table_Create_BusinessAttributesData = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessAttributesData`(business_id VARCHAR(60),' \
                                                  'attribute VARCHAR(60), value VARCHAR(45) ,PRIMARY KEY (business_id, attribute ))'

            Table_Create_BusinessGoodForAttributesData = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessGoodForAttributesData`(business_id VARCHAR(60),' \
                                                         'good_for VARCHAR(60), value VARCHAR(45) ,PRIMARY KEY (business_id,good_for))'

            Table_Create_BusinessAmbienceAttributesData = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessAmbienceAttributesData`(business_id VARCHAR(60),' \
                                                          'ambience VARCHAR(60), value VARCHAR(45), PRIMARY KEY (business_id ,ambience))'

            Table_Create_BusinessHoursData = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessHoursData`(business_id VARCHAR(60),' \
                                             'hours VARCHAR(60), open VARCHAR(45) ,close VARCHAR(45) ,PRIMARY KEY (business_id,hours))'

            Table_Create_BusinessParkingAttributesData = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessParkingAttributesData`(business_id VARCHAR(60),' \
                                                         'Parking VARCHAR(60), value VARCHAR(45), PRIMARY KEY (business_id,parking))'

            Table_Create_CategoriesData = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessCategoriesData`(business_id VARCHAR(60),' \
                                          'categories VARCHAR(60), PRIMARY KEY (business_id,categories))'

            Table_Create_NeighborhoodData = 'CREATE TABLE IF NOT EXISTS `yelpBusinessData100`.`BusinessNeighborhoodData`(business_id VARCHAR(60),' \
                                            'Neighborhood VARCHAR(60) ,PRIMARY KEY (business_id, neighborhood))'

            # MYSQL Query for Table Creation
            cursor.execute(Table_Create_BusinessDataMain)
            cursor.execute(Table_Create_BusinessAttributesData)
            cursor.execute(Table_Create_BusinessGoodForAttributesData)
            cursor.execute(Table_Create_BusinessAmbienceAttributesData)
            cursor.execute(Table_Create_BusinessHoursData)
            cursor.execute(Table_Create_BusinessParkingAttributesData)
            cursor.execute(Table_Create_CategoriesData)
            cursor.execute(Table_Create_NeighborhoodData)

            # Using Pandas DataFrame ,Inserting data to the respective tables

            business_csv_data = pd.read_csv(absolute_path + '/CSV_Files/BusinessDataMain_100.csv', header=None)
            b_m = pd.DataFrame(business_csv_data)
            attribute_csv_data = pd.read_csv(absolute_path + '/CSV_Files/BusinessAttributesData_100.csv', header=None)
            a_m = pd.DataFrame(attribute_csv_data)
            good_for_csv_data = pd.read_csv(absolute_path + '/CSV_Files/BusinessGoodForAttributesData_100.csv',
                                            header=None)
            g_f = pd.DataFrame(good_for_csv_data)
            Ambience_csv_data = pd.read_csv(absolute_path + '/CSV_Files/BusinessAmbienceAttributesData_100.csv',
                                            header=None)
            amb_a = pd.DataFrame(Ambience_csv_data)
            Hours_csv_data = pd.read_csv(absolute_path + '/CSV_Files/BusinessHoursData_100.csv', header=None)
            hr_d = pd.DataFrame(Hours_csv_data)
            Parking_csv_data = pd.read_csv(absolute_path + '/CSV_Files/BusinessAmbienceAttributesData_100.csv',
                                           header=None)
            p_d = pd.DataFrame(Parking_csv_data)
            Categories_csv_data = pd.read_csv(absolute_path + '/CSV_Files/CategoriesData_100.csv', header=None)
            c_d = pd.DataFrame(Categories_csv_data)
            Neighboorhood_csv_data = pd.read_csv(absolute_path + '/CSV_Files/NeighborhoodData_100.csv', header=None)
            n_hood = pd.DataFrame(Neighboorhood_csv_data)

            # insert Business_Main
            for i, row in b_m.iterrows():
                sqlQ1 = "INSERT IGNORE INTO BusinessDataMain VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sqlQ1, tuple(row))
                conn.commit()
            # insert attribute_Main
            for i, row in a_m.iterrows():
                sqlQ2 = "INSERT IGNORE INTO BusinessAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ2, tuple(row))
                conn.commit()
            # insert good_for_data
            for i, row in g_f.iterrows():
                sqlQ3 = "INSERT IGNORE INTO BusinessGoodForAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ3, tuple(row))
                conn.commit()
            # insert ambience_data
            for i, row in amb_a.iterrows():
                sqlQ4 = "INSERT IGNORE INTO BusinessAmbienceAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ4, tuple(row))
                conn.commit()
            # insert Hours_data
            for i, row in hr_d.iterrows():
                sqlQ5 = "INSERT IGNORE INTO BusinessHoursData VALUES(%s,%s,%s,%s)"
                cursor.execute(sqlQ5, tuple(row))
                conn.commit()
            # insert parking data
            for i, row in p_d.iterrows():
                sqlQ6 = "INSERT IGNORE INTO BusinessParkingAttributesData VALUES(%s,%s,%s)"
                cursor.execute(sqlQ6, tuple(row))
                conn.commit()
            # insert categories_data
            for i, row in c_d.iterrows():
                sqlQ7 = "INSERT IGNORE INTO BusinessCategoriesData VALUES(%s,%s)"
                cursor.execute(sqlQ7, tuple(row))
                conn.commit()
            # insert Neighborhood_data
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




