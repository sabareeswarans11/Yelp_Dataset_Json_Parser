# Yelp_Dataset_Json_Parser
Transforming Big Data in JSON (JavaScript Object Notation) Format as Semi-Structured Data 

# Overall Procedure (JSON Parser)

Utilized Python Dictionary , to separate the each key-value sets in the JSON Dataset and store them with respect to 1NF CSV structure Tables and later scripted to insert the scraped CSV data to MYSQL Database.
	In the wake of getting the key-value sets. I'm bringing the settled qualities present in each worth of key-value pair .By founded on each key-Value pair settled, I am saving the information into individual csv record.
	Here I am saving the information of single key-value sets into BusinessDataMain csv document
	After that I am saving the settled qualities into the other individual csv records like BusinessAttributesData.csv, BusinessGoodForAttributesData.csv,BusinessParkingAttributesData.csv, BusinessAmbienceAttributesData.csv,CategoriesData.csv,NeighborhoodData.csv, BusinessHoursData.csv.
	Dividing the big table into different separate attribute tables as above to satisfy First Normalization structure. 
	Note : In the created CSV files the key like _100 – means it processed from business100ValidForm.json dataset and EC – means it processed from yelp_academic_dataset_business.json ~200,000 Dataset (ExtraCredit) . Moreover small Dataset (only 100) and big dataset processed results are stored in two different directories.
	To have an association for each table or csv document, saving the business id in each csv record as Primary Key . in some case I have used business_id and one of its attributes of that table  both together as a primary key to avoid duplicated entry in database.
	Used two separate python files for bulk insert into the database. I have used Padas DataFrame to wrap the csv data to MySQL .DataFrame handles big data files better than CSV_import. 

# Dataset 

Yelp Business Dataset of 100 and 200,000 records

<img width="1368" alt="Screenshot 2022-02-27 at 3 53 58 AM" src="https://user-images.githubusercontent.com/94094997/155875582-67959286-6d25-426d-9c16-f2b106cf807d.png">

# Link to download Dataset 
https://www.yelp.com/dataset/download

# Csv Files created
<img width="1792" alt="Screenshot 2022-02-27 at 1 02 12 AM" src="https://user-images.githubusercontent.com/94094997/155875609-fe9c1a53-4112-4cb4-b9bb-0bc2408c2b35.png">

<img width="1792" alt="Screenshot 2022-02-27 at 1 00 13 AM" src="https://user-images.githubusercontent.com/94094997/155875602-864edfd3-b3c4-43c8-b71d-f918ff3328e3.png">

# Business Data stored in Relational tables

<img width="1684" alt="Screenshot 2022-02-27 at 1 57 17 AM" src="https://user-images.githubusercontent.com/94094997/155875635-beaddda2-6a70-47c2-90b9-1e17ad226afd.png">

# Retrieving some  infromation from database

<img width="1740" alt="EC" src="https://user-images.githubusercontent.com/94094997/155875756-d9e9e8ff-4d9a-4d7f-a180-38b5b6269146.png">



