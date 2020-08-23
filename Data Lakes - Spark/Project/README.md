# Spark Data Lake - Sparkify
***
Project Description
A music streaming startup called Sparkify would like to get more comprehensible meannings of its customers behavior. <br>
To achieve this goal Sparkify decided to move their growing user and song databases, as well as their ETL processes onto the cloud.<br>
Their raw data resides in an S3 bucket, inside two objects (directories):  <br>
1. A directory of JSON logs on user activity inside the app (called events in this context)
2. A directory with JSON metadata describing the songs available in Sparkify app.

This repository contains the code that creates Sparkify analytical infrastructure on AWS as an S3 Data Lake moving its unstructured JSON data to a kind of Star Schema (Analytical schema) columnar storage in parquet files.

### 1. Data Lake
#### 1.1 Staging views
Staging refers to a step in between, an intermediate state. Spark offers the possibility to create temporary views using createOrReplaceTempView().
Using this function we load all the unstructured JSON files in Spark dataframes `songs_stg` and `logs_stg` so that it would be much easier to select data from
and create the OLAP parquet data lake.

The data loaded from song data and log data are structured on Spark temporary views as: 

`logs_stg`
- **artist**        StringType,
- **auth**          StringType,
- **firstName**     StringType,
- **gender**        StringType,
- **itemInSession** IntegerType,
- **lastName**      StringType,
- **length**        DoubleType,
- **level**         StringType,
- **location**      StringType,
- **method**        StringType,
- **page**          StringType,
- **registration**  DoubleType,
- **sessionId**     DoubleType,
- **song**          StringType,
- **status**        DoubleType,
- **ts**            DoubleType,
- **userAgent**     StringType,
- **userId**        StringType

`songs_stg`
- **num_songs**        IntegerType,
- **artist_id**        StringType,
- **artist_latitude**  DoubleType,
- **artist_longitude** DoubleType
- **artist_location**  StringType
- **artist_name**      StringType,
- **song_id**          StringType,
- **title**            StringType,
- **duration**         DoubleType,
- **year**             IntegerType


#### 1.2 Parquet S3 Data Lake
The Sparkify data lake was modeled following a Star Schema (https://en.wikipedia.org/wiki/Star_schema) because it is a well-known denormalized easy to use database design schema for OLAP.
The star schema is simple and perfectly meets Sparkify needs.

**Fact table:**<br>
`songplays`<br>
- **start_time**
- **user_id**   
- **level**       
- **song_id**     
- **artist_id**   
- **session_id**   
- **location**    
- **user_agent**  

**Dimension tables:**<br>
`users`<br>
- **user_id**    
- **first_name** 
- **last_name**  
- **gender**     
- **level**      

`songs`<br>
- **song_id**   
- **title**     
- **artist_id** 
- **year**      
- **duration**  

`artists`<br>
- **artist_id** 
- **name**      
- **location**  
- **latitude**   
- **longitude** 

`time`<br>
- **start_time** 
- **hour**       
- **day**        
- **week**       
- **month**      
- **year**       
- **weekday**    

## Project description
The logic behind this project is very simple, it uses Spark to process Sparkify JSON logs and song files.
Creating Spark temporary views of those files, it retrieves our target OLAP Star Schema Data lake in the form of Spark DataFrames (Spark RDD is no longer recommended) via SQL queries on those views. <br>
Then, again using Spark it cleans the DataFrames to keep only the necessary data and save it on S3 in Parquet Columnar format. Each table has its own folder within the directory. <br>
Songs table files are partitioned by year and then artist. <br>
Time table files are partitioned by year and month. <br>
Songplays table files are partitioned by year and month.

## How to Run
***
There are **three** possible execution modes:
1. **LOCAL**<br>
Runs locally using the data inside /data .zip files - this is important to debug the ETL business logic. <br><br>
2. **LOCAL_S3**<br>
Runs locally using S3 `udacity-dend` bucket data, downloads the data files, process locally and upload it back to user specific s3 bucket `dend-nano-spark-datalake`.<br><br>
3. **REMOTE** (***Recommended***)<br>
Launch an EMR cluster and execute this project totally on AWS, from S3 to EMR cluster than back saving the data lake into s3 `dend-nano-spark-datalake` bucket. This is the recommended mode because it runs on the full S3 udacity dataset but quickly, using EMR take around 20 minutes of Spark execution. This script shut down the EMR cluster after the execution.
Local S3 mode takes a long time to finish its execution.<br>

1. Choose your execution mode and change `dl.cfg` file adding the mode you choose:

`[EXECUTION_MODE]
mode=<>`

2. Still in the same `dl.cfg` file, if you choose to execute `LOCAL_S3` or `REMOTE` add your AWS credentials

`[AWS_ACCOUNT]
aws_access_key_id = <>
aws_secret_access_key = <>`

3. Launch a shell and run
`python3 etl.py`

## Project organization
***
```
project
│
└───data - Zip files, used to local debug 
│   bootstrap.sh - Bootstrap file used on EMR cluster launch, it basically install boto3 library into EMR nodes.
│   dl.cfg - Configuration file used throughout the project to consume global configurations.
│   etl.py - Extract information from S3 Sparkify files, load into the staging views and then copy only the analytical relevant data into sparkify OLAP parquet data lake.
│   README.md - This README file.
└───sql_queries.py - Contains all SQL queries used in this project.
```



















