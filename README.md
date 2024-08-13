# <p align="center">Olist E-commerce Data LakeHouse Project</p>

In this project, we'll create and maintain a Data Lakehouse on the Azure Cloud Platform. This Data Lakehouse will consume data from an E-commerce company called Olist and perform metadata driven Extract, Transform & Load (ETL) processes on a daily basis. We'll utilize advanced technologies such as Azure Data Factory, ADLS Gen2 Storage Account and Azure Databricks for data ingestion, staging, storing, cleaning, transformation and loading. Additionally, we'll implement a Star schema data model, empowering business users and stakeholders to derive insights and answer key business questions more effectively.
<br>
<br>

## Pipeline Architecture Diagram

![Pipeline Architecture drawio](https://github.com/user-attachments/assets/33078b00-108f-4876-9eb9-c606d9b50b1c)

###### <p align="center">fig 1.1 Olist E-commerce Data LakeHouse Pipeline Architecture</p>
<br>

<img width="960" alt="1" src="https://github.com/user-attachments/assets/c6ed1af6-c77a-47da-80ff-e32191de96e7">

###### <p align="center">fig 1.2 Olist E-commerce Data LakeHouse Pipeline Diagram</p>
<br>

### LakeHouse Architecture

The data lakehouse is a open data management architecture combines the flexibility, cost-efficiency, and scale of data lakes with the data management and ACID transactions of data warehouses, enabling business intelligence (BI) and machine learning (ML) on all data.

The pipeline architecture is used to logically organize data in the lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables).

#### Bronze layer (raw data)
The Bronze layer is where we're going to perform data staging/data ingestion with the data from the external data source system/landing zone. 
The table structures in this layer correspond to the source system table structures "as-is," along with the following additional metadata columns "source_file_name", "process_id", "load_date" that capture the load date/time, process ID. 
The focus in this layer is to perform full load data ingestion from the source file system.

#### Silver layer (cleansed and conformed data)
In the Silver layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed, "just-enough" transformations and data cleansing rules are applied while loading the Silver layer.
Data transformations and data quality rules are applied here.

#### Gold layer (curated business-level tables)
Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" data.
The Gold layer is for reporting and uses more de-normalized and read-optimized data model with fewer joins.
We use Kimball style star schema-based data model in this Gold Layer of the lakehouse.

So you can see that the data is curated as it moves through the different layers of a lakehouse. 

The data is stored in delta lake data storage format in all the three layers.
<br>
<br>

## How it works

### Source Data System


In this project, our data source is a file system. We'll receive our data as CSV files for each application from the Olist E-commerce team. Please find below the entity relationship diagram of the data source.
<br>

![Olist E-commerce Source Data Entity Relationship Diagram drawio](https://github.com/user-attachments/assets/fe5ec1e7-9ec1-4be8-b891-819d144e31aa)

###### <p align="center">fig 1.3 Olist E-commerce Source Data Entity Relationship Diagram</p>
<br>

The data source team from Olist will send the CSV file for 7 different applications. The applications are as below,
1. Customers - This dataset has the information about the customers
2. Orders - This dataset has the order transactions information
3. Products - This dataset has the information about the products
4. Sellers - This dataset has the information about the sellers
5. Order Items - This dataset has the order items information
6. Order Payments - This dataset has the information about the payment transactions
7. Order Ratings - This dataset has the order ratings information

### Source File System

We're utilizing AWS S3 datalake as the source file system where the source data team will place the source files daily.
<br>

<img width="960" alt="2" src="https://github.com/user-attachments/assets/0a473adf-2800-4b00-81fd-ce4e08e7304b">
<br>

<img width="960" alt="3" src="https://github.com/user-attachments/assets/e658d605-109b-4ac2-8470-e73a585e0eff">
<br>
<br>

### ADLS Gen2 DataLakeHouse
### Source to Bronze Staging Layer
In this task, we'll perform data ingestion to the Bronze Staging layer by extracting data files one by one from the S3 datalake with the help of metadata stored in a metadata/control table called "bronze_cntl_table" in Azure SQL databse.
The metadata table consists of the informations like "sys_name", "app_name", "load_order", "inbound_bucket", "raw_file_path", "raw_file_name", "raw_file_format", "delimiter", "bronze_file_path", "bronze_table_name".
It's helps in reading the source files and performing schema validation then ingesting the data into Parquet files in Bronze Staging layer.

User defined ADF pipeline called "S3_to_ADLSGen2" is used to perform this task.

<img width="960" alt="4" src="https://github.com/user-attachments/assets/8aaa97cc-ec2a-42bd-9d64-8b2b7c9815ba">
<br>

<img width="960" alt="5" src="https://github.com/user-attachments/assets/287f7043-e9db-4e08-891d-d82d1a72225d">
<br>

<img width="960" alt="6" src="https://github.com/user-attachments/assets/5dcf8bdd-6ca0-4d47-bdd5-9edeba704691">
<br>

<img width="960" alt="7" src="https://github.com/user-attachments/assets/36c264f2-74f1-4e3d-903c-7632854155b2">
<br>
<br>


### Bronze Staging to Bronze Archive
In this task, we'll be archiving the data from Bronze Staging Parquet files to Bronze Archive Delta tables by reading the Parquet files in the Bronze Staging layer one by one with the help of metadata stored in a JSON file called "archive_metadata.json" in ADLS container.
The JSON consists of the source and target table informations like "app_nm", "file_name", "file_path", "columns", "table_name", "basePath".
It's helps in reading the Parquet files from the Bronze Staging layer and performing data append operation to Delta tables in Bronze Archive layer.
<br>
<br>


### Bronze Staging to Silver Layer
In this task, we'll be performing data load from Bronze to Silver layer by reading the Parquet files in the Bronze Staging layer one by one with the help of metadata stored in a JSON file called "silver_metadata.json" in ADLS container.
The JSON consists of the source and target table informations like "app_nm", "src_table_name", "src_table_path", "src_columns", "timestamp_format", "tgt_table_name", "basePath".
It's helps in reading the Parquet files from the Bronze Staging layer and performing data quality check, data deduplication, correcting data structure issues/data transformation then upsert/delta load to Delta tables in Silver layer.
<br>
<br>


### Silver to Gold Layer
In this task, we'll be using Kimball style star schema-based data model and perform data loading to SCD2 dimension tables & fact tables from Delta tables in the Silver layer.
Please find below the data model diagram for the Gold Layer. 
<br>

![Olist E-commerce Data LakeHouse Dimensional Modelling Diagram drawio](https://github.com/user-attachments/assets/4b078c5b-1a45-4a68-9ece-c9e9422530e7)

###### <p align="center">fig 1.4 Olist E-commerce Data LakeHouse Dimensional Modelling Diagram</p>
<br>

### Dimensions
Here, we need to perform data loading for the SCD2 dimension tables from Silver layer one by one with the help of metadata stored in a JSON file called "gold_dims_metadata.json" in ADLS container.
The JSON consists of the source and target table informations like "app_nm", "src_table_name", "src_table_path", "tgt_table_name", "basePath".
It's helps in reading the Silver Delta tables and performing data load for Delta Dimension tables in Gold layer.
<br>

### Facts
Here, we need to perform data loading for the Fact tables from Gold layer one by one with the help of metadata stored in a JSON file called "gold_facts_metadata.json" in ADLS container.
The JSON consists of the target table informations like "fact", "silver_src_tables", "dim_src_tables", "tgt_table_name", "basePath".
It's helps in reading the Gold Delta dimension & any source tables from Silver layer and performing data load for Delta Fact tables in Gold layer.

### Script Pattern used in Lakehouse for Data Processing & Loading
##### 1. custom_logging.py : 
This program helps us to perform custom logging and saves the log file in the Log Folder in ADLS Gen2 container.

##### 2. metadata.py : 
This program helps us to read the metadata stored as JSON in ADLS container with help of databricks utilities.

##### 3. spark_utils.py : 
This program is used for configuring methods which are used for reading the source & target tables, performing ransformations and loading with pre defined pyspark codes with respect to each layer and table.
We've used Factory method design pattern that allows an interface or a class to create an object, but lets subclasses decide which class or object to instantiate. Using the Factory method, we have the best ways to create an object.

##### 4. main.py : 
This is our main program, we'll be importing the above three programs as modules in this program. 
In this main script, it'll loop through the metadata list to perform the data transformation & loading for each table table one by one respectively.
<br>
<br>

## Orchestration

### ADF Triggers
Since it's a batch processing, we need to orchestrate the pipeline as a daily load. For orchestration we'll be using the Trigger feature.
<br>
<br>

### Project Screeshots

<img width="960" alt="8" src="https://github.com/user-attachments/assets/d2cc9726-ecb3-4aaf-a66a-a623cf06daeb">
<br>

<img width="960" alt="9" src="https://github.com/user-attachments/assets/613f32e4-0fb7-4e2c-97f6-342e3abda489">
<br>

<img width="960" alt="10" src="https://github.com/user-attachments/assets/19061787-b9f5-4094-b12c-dc4c75b8a235">
<br>

<img width="960" alt="11" src="https://github.com/user-attachments/assets/510df09b-2b72-4f24-98c1-9742b76863f4">
<br>

<img width="960" alt="12" src="https://github.com/user-attachments/assets/3dc0495d-8bdf-4969-b411-6d4fefed6ff2">
<br>

<img width="960" alt="13" src="https://github.com/user-attachments/assets/167903a9-9d6e-43df-9be2-d085963b7678">
<br>

<img width="960" alt="14" src="https://github.com/user-attachments/assets/56a39204-e160-459c-8bb4-f096c6e58a94">
<br>

<img width="960" alt="15" src="https://github.com/user-attachments/assets/635a741e-f3e4-4b67-adb0-e8c39be64b3f">
<br>

<img width="960" alt="16" src="https://github.com/user-attachments/assets/d26badb9-3648-4707-8b55-0c32aead1427">
<br>

<img width="960" alt="17" src="https://github.com/user-attachments/assets/3d16b097-e5b3-493d-aa0a-6691c381ed95">
<br>

<img width="960" alt="18" src="https://github.com/user-attachments/assets/607c73aa-baef-4c9e-88ab-dc5ae340803f">
<br>

<img width="960" alt="19" src="https://github.com/user-attachments/assets/785572fb-0109-4072-8b2b-da1921fecece">
<br>

<img width="960" alt="20" src="https://github.com/user-attachments/assets/804a0faf-31ff-4cb0-8e66-b6f4c48d575d">
<br>

<img width="960" alt="21" src="https://github.com/user-attachments/assets/02e7f61f-0c50-45a2-b7ff-3b9484eb7f7e">
<br>

<img width="960" alt="22" src="https://github.com/user-attachments/assets/16e2f06c-cb28-48dc-a58d-f6180c3629ff">
<br>

<img width="960" alt="23" src="https://github.com/user-attachments/assets/d155979d-dff4-4cce-bfdc-4bb49cbf143e">
<br>

<img width="960" alt="24" src="https://github.com/user-attachments/assets/6f5b0e08-26fe-44b6-a5e2-eb22f1c2dd54">
<br>
<br>
<br>

Please refer the scripts in the repository and also the video presentation for more understanding.

I've done this project with the working experience in my previous organisation and practical learning.

# <p align="center">Thank You</p>
