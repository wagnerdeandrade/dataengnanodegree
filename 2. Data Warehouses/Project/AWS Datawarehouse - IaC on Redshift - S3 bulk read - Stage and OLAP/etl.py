import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """Copy data from S3 buckets into Redshift staging tables using
    the queries in copy_table_queries

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    conn -- open connection to the database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Copy the relevant Sparkify analytical data from staging tables to
    a Star Schema analytical tables using the queries in insert_table_queries

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    conn -- open connection to the database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Read configuration data from dwh.cfg
    - Connect to AWS Datawarehouse resources
    - Populate staging tables using S3 data
    - Using the staging tables, populate the analytical tables of the Datawarehouse
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()