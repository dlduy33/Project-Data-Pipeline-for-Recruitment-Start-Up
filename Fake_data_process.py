from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import *
from datetime import *
import random
import cassandra
import time

spark = SparkSession.builder.config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()

def data_from_sql():
    host = 'localhost'
    port = '3306'
    db_name = 'DW'
    url = f'jdbc:mysql://{host}:{port}/{db_name}'
    driver = 'com.mysql.cj.jdbc.Driver'
    user = 'root'
    password = '1'
    sql_job = '''(SELECT id AS job_id, group_id, campaign_id FROM job) job'''
    job_data = spark.read.format('jdbc').options(url=url, driver=driver, user=user, password=password, dbtable=sql_job)\
                                        .load()
    sql_publisher = '''(SELECT id AS publisher_id FROM master_publisher) publisher'''
    publisher_data = spark.read.format('jdbc').options(url=url, driver=driver, user=user, password=password, dbtable=sql_publisher)\
                                        .load()
    return job_data, publisher_data

def cassandra_connect():
    keyspace = 'datalake'
    cluster = Cluster()
    session = cluster.connect(keyspace)
    return session

def grenarate_fake_data(session, n_records, job_data, publisher_data):
    job_id = job_data.select(collect_set('job_id')).first()[0]
    group_id = job_data.select(collect_set('group_id')).first()[0]
    campaign_id = job_data.select(collect_set('campaign_id')).first()[0]
    publisher_id = publisher_data.select(collect_set('publisher_id')).first()[0]
    i = 0
    fake_records = n_records
    while i <= fake_records:
        create_time = str(cassandra.util.uuid_from_time(datetime.now()))
        bid = random.randint(0, 1)
        campaign_id = random.choice(campaign_id)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact ,weights=(70,10,10,10))[0]
        group_id = random.choice(group_id)
        job_id = random.choice(job_id)
        publisher_id = random.choice(publisher_id)
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = f"""INSERT INTO tracking (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts) VALUES ('{create_time}', {bid}, {campaign_id}, '{custom_track}', {group_id}, {job_id}, {publisher_id}, '{ts}')"""
        print(sql)
        session.execute(sql)
        i+=1
    return print('Data grenerated successfully')

def main():
    job_data, publisher_data = data_from_sql()
    session = cassandra_connect()
    while True:
        n_records = random.randint(1, 10)
        grenarate_fake_data(session, n_records, job_data, publisher_data)
        time.sleep(5)

if __name__ == '__main__':
    main()