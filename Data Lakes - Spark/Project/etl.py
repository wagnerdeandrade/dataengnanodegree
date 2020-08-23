import configparser
import os, shutil
import zipfile
import sys
import boto3
from datetime import datetime
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (col, year, month, dayofmonth, hour, 
            weekofyear, dayofweek, from_unixtime)
from sql_queries import (songplays_table_creation, songs_table_creation,
            artists_table_creation, users_table_creation, time_table_creation)

LOCAL_EXECUTION_MODE    = "LOCAL"
LOCAL_S3_EXECUTION_MODE = "LOCAL_S3"
REMOTE_EXECUTION_MODE   = "REMOTE"

def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def uncompress_raw_source(raw_path, stg_path):
    """
        Unzip raw_path .zip file to stg_path
        
        Arguments:
            raw_path - Path to input .zip file to be uncompressed
            stg_path - Path to store uncompressed files
    """
    with zipfile.ZipFile(raw_path, 'r') as zip_ref:
        zip_ref.extractall(stg_path)

def flatten_data(path):
    """
        Relevant only on LOCAL execution mode, recursively move the song JSON files
        to the root path directory
        
        Arguments:
            path - root path
    """
    for root, subdirs, files in os.walk(path):
        for file in files:
            if(file != '.DS_Store'):
                shutil.move(root + '/' + file, path)

def prepare_raw_data(config, execution_mode):
    """
        Depending on the different possible execution modes, determine
        where data input must come from as well as where to store the result
        
        Arguments:
            config - Configuration file describing data paths
            execution_mode - Describes the execution mode, Local, Local using
                         s3 input or total remote
    """
    if execution_mode == LOCAL_EXECUTION_MODE:
        zipped_song_path = config['RAW_FILE_NAME']['song_raw']
        zipped_log_path  = config['RAW_FILE_NAME']['log_raw']

        input_song_path  = config['DATA_PATH']['local_input_song']
        input_log_path   = config['DATA_PATH']['local_input_log']

        uncompress_raw_source(zipped_song_path, input_song_path)
        flatten_data(input_song_path)

        uncompress_raw_source(zipped_log_path, input_log_path)

        output_path      = config['DATA_PATH']['local_output']

    else:
        os.environ['AWS_ACCESS_KEY_ID']     = config['AWS_ACCOUNT']['aws_access_key_id']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_ACCOUNT']['aws_secret_access_key']

        input_song_path                     = config['DATA_PATH']['remote_input_song']
        input_log_path                      = config['DATA_PATH']['remote_input_log']

        output_path                         = config['DATA_PATH']['remote_output']
        
    return (input_song_path, input_log_path, output_path)

def create_songs_table(spark, output_path):
    """
        Create parquet songs dimension table
        
        Arguments:
            spark - Spark session
            output_path - where to write the parquet songs file table
    """

    # extract columns to create songs table
    songs_tableDF = spark.sql(songs_table_creation)

    # write songs table to parquet files partitioned by year and artist
    (songs_tableDF.write
        .mode("overwrite")
        .partitionBy("year", "artist_id")
        .parquet(output_path + "songs/")
    )

def create_artists_table(spark, output_path):
    """
        Create parquet artists dimension table
        
        Arguments:
            spark - Spark session
            output_path - where to write the parquet artists file table
    """
    # extract columns to create artists table
    artists_tableDF = spark.sql(artists_table_creation)

    # write artists table to parquet files
    (artists_tableDF.write
        .mode("overwrite")
        .parquet(output_path + "artists/")
    )

def create_users_table(spark, output_path):
    """
        Create parquet users dimension table
        
        Arguments:
            spark - Spark session
            output_path - where to write the parquet users file table
    """
    # extract columns for users table
    users_tableDF = spark.sql(users_table_creation)

    # write users table to parquet files
    (users_tableDF.write
        .mode("overwrite")
        .parquet(output_path + "users/")
    )

def create_timestamp_table(spark, output_path):
    """
        Create parquet time dimension table
        time - timestamps of records in songplays broken down into specific units
            start_time, hour, day, week, month, year, weekday
            
        Arguments:
            spark - Spark session
            output_path - where to write the parquet time file table
    """

    time_tableDF = spark.sql(time_table_creation)

    # extract columns to create time table
    time_tableDF = time_tableDF.withColumn("hour",    hour("start_time"))
    time_tableDF = time_tableDF.withColumn("day",     dayofmonth("start_time"))
    time_tableDF = time_tableDF.withColumn("week",    weekofyear("start_time"))
    time_tableDF = time_tableDF.withColumn("month",   month("start_time"))
    time_tableDF = time_tableDF.withColumn("year",    year("start_time"))
    time_tableDF = time_tableDF.withColumn("weekday", dayofweek("start_time"))

    # write time table to parquet files partitioned by year and month
    (time_tableDF.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path + "time/")
    )

def create_songplay_table(spark, output_path):
    """
        Create parquet songplay fact table
        
        Arguments:
            spark - Spark session
            output_path - where to write the parquet songplay file table
    """
    # extract columns from joined song and log datasets to create songplays table
    songplays_tableDF = spark.sql(songplays_table_creation)

    # to be able to effectivelly partition (also readable) songplays table by year and month
    # we need to add those columns to the dataframe
    songplays_tableDF = songplays_tableDF.withColumn("year", year("start_time"))
    songplays_tableDF = songplays_tableDF.withColumn("month", month("start_time"))
    
    # write songplays table to parquet files partitioned by year and month
    (songplays_tableDF.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path + "songplays/")
    )

def create_stg_views(spark, input_song, input_log):
    """
        Create Spark staging views from raw data, these staging views
        are tables that will be used as input to populate the final OLAP
        parquet data lake
        
        Arguments:
            spark - Spark session
            output_path - where to write the parquet songplay file table
    """
    # read song data file
    schema_song = (StructType()
      .add("num_songs",        IntegerType(), nullable=True)
      .add("artist_id",        StringType(),  nullable=False)
      .add("artist_latitude",  DoubleType(),  nullable=True)
      .add("artist_longitude", DoubleType(),  nullable=True)
      .add("artist_location",  StringType(),  nullable=True)
      .add("artist_name",      StringType(),  nullable=True)
      .add("song_id",          StringType(),  nullable=False)
      .add("title",            StringType(),  nullable=True)
      .add("duration",         DoubleType(),  nullable=True)
      .add("year",             IntegerType(), nullable=True))
    songs_stg_df = spark.read.schema(schema_song).json(input_song)

    # drop all duplicated values in raw data
    songs_stg_df = songs_stg_df.dropDuplicates()

    # creates a temporary songs_stg view to read data from
    songs_stg_df.createOrReplaceTempView("songs_stg")

    # read log data file
    schema_log = (StructType()
      .add("artist",        StringType(),  nullable=True)
      .add("auth",          StringType(),  nullable=True)
      .add("firstName",     StringType(),  nullable=True)
      .add("gender",        StringType(),  nullable=True)
      .add("itemInSession", IntegerType(), nullable=True)
      .add("lastName",      StringType(),  nullable=True)
      .add("length",        DoubleType(),  nullable=True)
      .add("level",         StringType(),  nullable=True)
      .add("location",      StringType(),  nullable=True)
      .add("method",        StringType(),  nullable=True)
      .add("page",          StringType(),  nullable=True)
      .add("registration",  DoubleType(),  nullable=True)
      .add("sessionId",     DoubleType(),  nullable=True)
      .add("song",          StringType(),  nullable=True)
      .add("status",        DoubleType(),  nullable=True)
      .add("ts",            DoubleType(),  nullable=True)
      .add("userAgent",     StringType(),  nullable=True)
      .add("userId",        StringType(),  nullable=True))
    logs_stg_df = spark.read.schema(schema_log).json(input_log)

    # drop all duplicated values in raw data
    logs_stg_df = logs_stg_df.dropDuplicates()

    # convert timestamp type, the only relevant type not inferred by Spark
    logs_stg_df = logs_stg_df.withColumn("ts", from_unixtime(col("ts").cast('bigint')/1000))

    # creates a temporary logs_stg view conataining only "NextSong" logs to read data from
    logs_stg_df.where(logs_stg_df["page"] == "NextSong").createOrReplaceTempView("logs_stg")

def create_olap_parquet_database(spark, output_path):
    """
        Given Spark staging tables, creates the OLAP Sparkify data lake
        
        Arguments:
            spark - Spark session
            output_path - where to write the parquet OLAP DB files
    """
    create_songplay_table(spark, output_path)
    create_users_table(spark, output_path)
    create_songs_table(spark, output_path)
    create_artists_table(spark, output_path)
    create_timestamp_table(spark, output_path)

def launch_emr_cluster(config, config_file):
    """
        When in REMOTE mode, store this project in an S3 bucket and launch
        an EMR Cluster to execute this project in.
        
        Arguments:
            config - Configuration object, used to read AWS credentials
            config_file - Configuration file path, used to store the environment setting
                so that the program knows that is been executed inside EMR cluster and
                not in a local machine
    """
    
    # write a tag ENV in config file so that the program identifies it was executing on EMR
    config['ENV'] = {'HOST':'EMR'}
    with open(config_file, 'w') as conf:
        config.write(conf)

    os.environ['AWS_ACCESS_KEY_ID']     = config['AWS_ACCOUNT']['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_ACCOUNT']['aws_secret_access_key']

    # uploading the project to an S3 bucket

    s3 = boto3.resource('s3')
    S3_BUCKET = 'dend-nano-spark-datalake'
    bucket = s3.Bucket(S3_BUCKET)
    if not bucket.creation_date:
        bucket = s3.create_bucket(Bucket=S3_BUCKET, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    S3_FILE_TO_EXECUTE = 'spark/etl.py'
    S3_FILE_SQL_AUX    = 'spark/sql_queries.py'
    S3_FILE_CONFIG     = 'spark/dl.cfg'
    S3_FILE_BOOTSTRAP  = 'spark/bootstrap.sh'

    s3_client = boto3.client('s3')
    s3_client.upload_file("etl.py",         S3_BUCKET, S3_FILE_TO_EXECUTE)
    s3_client.upload_file("dl.cfg",         S3_BUCKET, S3_FILE_CONFIG)
    s3_client.upload_file("sql_queries.py", S3_BUCKET, S3_FILE_SQL_AUX)
    s3_client.upload_file("bootstrap.sh",   S3_BUCKET, S3_FILE_BOOTSTRAP)

    s3_objects_exists_waiter = s3_client.get_waiter('object_exists')
    s3_objects_exists_waiter.wait(Bucket=S3_BUCKET,Key=S3_FILE_TO_EXECUTE)
    s3_objects_exists_waiter.wait(Bucket=S3_BUCKET,Key=S3_FILE_SQL_AUX)
    s3_objects_exists_waiter.wait(Bucket=S3_BUCKET,Key=S3_FILE_CONFIG)
    s3_objects_exists_waiter.wait(Bucket=S3_BUCKET,Key=S3_FILE_BOOTSTRAP)

    # after uploading the dl.cfg version that will be executed in EMR, remove the ENV tag for local version
    config.remove_section('ENV')
    with open(config_file, "w") as conf:
        config.write(conf)
    
    # URI of the files to be transfered to hadoop user in EMR cluster,
    # those will be the core of our project execution
    S3_FILE_TO_EXECUTE_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_FILE_TO_EXECUTE)
    S3_FILE_SQL_AUX_URI    = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_FILE_SQL_AUX)
    S3_FILE_CONFIG_URI     = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_FILE_CONFIG)
    S3_FILE_BOOTSTRAP_URI  = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_FILE_BOOTSTRAP)

    # run emr submitting this etl.py as a cluster job to Spark
    client = boto3.client('emr', region_name='us-west-2')
    response = client.run_job_flow(
        Name="dengnano_spark_datalake_project",
        LogUri='s3://dend-nano-spark-datalake/logs',
        ReleaseLabel='emr-6.0.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'InstanceCount': 4,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2KeyName': 'data_eng_nano_dl_with_spark',
        },
        Applications=[
            {
                'Name': 'Spark'
            }
        ],
        BootstrapActions = [{
            'Name': 'Install dependencies on Cluster machines',
            'ScriptBootstrapAction': {
                    'Path': S3_FILE_BOOTSTRAP_URI
                }
            }
        ],
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'setup - copy etl file',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', S3_FILE_TO_EXECUTE_URI, '/home/hadoop/']
                }
            },
            {
                'Name': 'setup - copy etl file',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', S3_FILE_SQL_AUX_URI, '/home/hadoop/']
                }
            },
            {
                'Name': 'setup - copy config file',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', S3_FILE_CONFIG_URI, '/home/hadoop/']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/etl.py']
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )

def main():

    config_file = os.path.join(os.path.dirname(__file__), 'dl.cfg')
    config = configparser.ConfigParser()
    config.read(config_file)

    # If the execution mode is totally remote then submit the job to a Spark application
    # running on AWS EMR
    # REMEMBER: IF YOU ARE RUNNING LOCAL, CHANGE d.cfg "EXECUTION_MODE" "mode" to "LOCAL"
    execution_mode = config['EXECUTION_MODE']['mode']
    if execution_mode == REMOTE_EXECUTION_MODE and not config.has_option('ENV', 'host'):
        launch_emr_cluster(config, config_file)
        print(
             "Executing remotely on AWS EMR as \"dengnano_spark_datalake_project\" cluster, " +\
             "check your EMR console for execution details. \n" + \
             "When EMR Spark execution finishes the results will be stored " + \
             "as parquet files in your \"dend-nano-spark-datalake\" S3 bucket."
            )
        sys.exit()

    # Normal execution flow, if execution mode is local below code runs directly, if remote
    # below code will run in an AWS EMR cluster
    
    # Determine where is the data to be processed and where to store the processed information
    input_song, input_log, output_path = prepare_raw_data(config, execution_mode)

    # Creates a Spark session that will be used to process Sparkify data
    spark = create_spark_session()
    
    # Creates staging views in spark using the raw data
    create_stg_views(spark, input_song, input_log)

    # Reading data from the staging views, create the analytical Sparkify datalake
    create_olap_parquet_database(spark, output_path)

    spark.stop()

if __name__ == "__main__":
    main()
