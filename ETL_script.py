from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from uuid import UUID
import time_uuid
import datetime
import time

spark = SparkSession.builder.config("spark.jars.packages",'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()

def clean_data(df, custom_track):
    print('----------------')
    print('Change timeuuid to new timestamp')
    print('----------------')
    amount_of_uuid = df.select('create_time').collect()
    new_timestamps = []
    for i in range(len(amount_of_uuid)):
        ts = time_uuid.TimeUUID(bytes=UUID(amount_of_uuid[i][0]).bytes).get_datetime().strftime(('%Y-%m-%d %H:%M:%S'))
        new_timestamps.append(ts)
    create_time_list = df.select(collect_list('create_time')).first()[0]
    time_data = spark.createDataFrame(zip(create_time_list, new_timestamps), ['create_time', 'ts'])
    df = df.join(time_data, 'create_time', 'full').drop(df.ts)
    print('----------------')
    print('Selecting data')
    print('----------------')
    data_table = df.filter((col('job_id').isNotNull()) & (col('custom_track') == f'{custom_track}'))\
                        .select('job_id', 'publisher_id', 'campaign_id', 'group_id', 'ts', 'custom_track', 'bid')
    data_table = data_table.withColumn('hour', date_format('ts',"h"))\
                            .withColumn('date', date_format('ts', 'yyyy-MM-dd'))
    return data_table

def groupby_data(df):
    print('----------------')
    print('Group Click data')
    print('----------------')
    clicks_data = clean_data(df, 'click')
    clicks_data = clicks_data.groupBy('job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id')\
                            .agg(avg('bid').alias('bid_set'),
                                sum('bid').alias('spend_hour'),
                                count(lit(1)).alias('clicks'))
    print('----------------')
    print('Group Conversion data')
    print('----------------')
    conversion_data = clean_data(df, 'conversion')
    conversion_data = conversion_data.groupBy('job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id')\
                                .agg(count(lit(1)).alias('conversions'))
    print('----------------')
    print('Group Qualified data')
    print('----------------')
    qualified_data = clean_data(df, 'qualified')
    qualified_data = qualified_data.groupBy('job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id')\
                                .agg(count(lit(1)).alias('qualified'))
    print('----------------')
    print('Group Unqualified data')
    print('----------------')
    unqualified_data = clean_data(df, 'unqualified')
    unqualified_data = unqualified_data.groupBy('job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id')\
                                .agg(count(lit(1)).alias('unqualified'))
    return clicks_data, conversion_data, qualified_data, unqualified_data

def retrive_company_data(host, port, db_name, url, driver, user, password):
    sql = '''(SELECT id AS job_id, company_id, group_id, campaign_id FROM job) test'''
    company_data = spark.read.format('jdbc').options(url=url, driver=driver, user=user, password=password, dbtable=sql).load()
    return company_data

def process_final_data(clicks_data, conversion_data, qualified_data, unqualified_data, company_data):
    full_data = clicks_data.join(conversion_data, ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id'], 'full')\
                            .join(qualified_data, ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id'], 'full')\
                            .join(unqualified_data, ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id'], 'full')\
                            .join(company_data, 'job_id', 'left').drop(company_data.group_id).drop(company_data.campaign_id)
    final_data = full_data.select('job_id', 'date', 'hour', 'publisher_id', 'company_id', 'campaign_id', 'group_id', 'unqualified',
                'qualified', 'conversions', 'clicks', 'bid_set', 'spend_hour').withColumn('source', lit('Cassandra'))
    final_data = final_data.fillna(0)
    return final_data

def import_to_mysql(final_data, host, port, db_name, url, driver, user, password):
    final_output = final_data.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversions','clicks','bid_set','spend_hour')
    final_output = final_output.withColumnRenamed('date','dates').withColumnRenamed('hour','hours')\
                                .withColumnRenamed('qualified','qualified_application')\
                                .withColumnRenamed('unqualified','disqualified_application')\
                                .withColumnRenamed('conversions','conversion')
    final_output = final_output.withColumn('sources',lit('Cassandra'))
    final_output.write.format('jdbc').options(url=url, driver=driver, user=user, password=password, dbtable='events')\
                        .mode('append').save()
    return print('Data imported successfully')

def retrieve_latest_cassandra_time():
    df_tracking = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="datalake").load()
    cassandra_latest_time = df_tracking.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def retrieve_latest_mysql_time(url,driver,user,password):
    sql = """(select max(last_updated_time) from events) test"""
    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load().take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1996-03-23 23:59:59'
    else:
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest

def main():
     host = 'localhost'
     port = '3306'
     db_name = 'DW'
     url = f'jdbc:mysql://{host}:{port}/{db_name}'
     driver = 'com.mysql.cj.jdbc.Driver'
     user = 'root'
     password = '1'
     time_start = datetime.datetime.now()
     while True:
          print("It is {} at the moment".format(time_start))
          mysql_time = retrieve_latest_mysql_time(url,driver,user,password)
          print('Latest time in MySQL is {}'.format(mysql_time))
          cassandra_time = retrieve_latest_cassandra_time()
          print('Latest time in Cassandra is {}'.format(cassandra_time))
          if cassandra_time > mysql_time:
               print('----------------')
               print('Read data from Cassandra DataLake')
               print('----------------')
               df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="datalake").load().where(col('ts') >= mysql_time)
               clicks_data, conversion_data, qualified_data, unqualified_data = groupby_data(df)
               print('----------------')
               print('Read data from MySQL DataWarehouse')
               print('----------------')
               company_data = retrive_company_data(host, port, db_name, url, driver, user, password)
               print('----------------')
               print('Processing final data')
               print('----------------')
               final_data = process_final_data(clicks_data, conversion_data, qualified_data, unqualified_data, company_data)
               import_to_mysql(final_data, host, port, db_name, url, driver, user, password)
          else:
               print('No New data found')
          time_end = datetime.datetime.now()
          print("It is {} at the moment".format(time_end))
          print("Job takes {} seconds to execute".format((time_end - time_start).total_seconds()))
          print("-------------------------------------------")
          time.sleep(30)

if __name__ == '__main__':
    main()