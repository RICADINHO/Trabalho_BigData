# Basic imports
import os, sys, time, json

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

# Create a Spark session
# To configurate the connection between Apache Kafka and Pyspark, it is necessary to run four jar files

def spark_initialize() -> SparkSession:
    scala_version = '2.12'
    spark_version = '3.3.1'
    packages = [f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
                f'org.apache.spark:spark-token-provider-kafka-0-10_{scala_version}:{spark_version}',
                f'org.apache.spark:spark-streaming-kafka-0-10_{scala_version}:{spark_version}',
                'org.apache.kafka:kafka-clients:3.3.1',
                'org.apache.commons:commons-pool2:2.8.0'
            ]
    spark = SparkSession.builder\
        .appName('Streaming')\
        .config('spark.jars.packages', ','.join(packages))\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") # INFO, WARN, ERROR
    return spark

def data_stream_spark(spark, brokers, topic, table) -> DataFrame:

    # Setup a streaming DataFrame to read data from kafka consumer
    # Subscribe to 1 topic, with headers
    df = ( spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("includeHeaders", "true")
        .option("startingOffsets", "earliest") # earliest latest
        .load()
        )

    # Just in case we want to start a table containing results but from scratch
    spark.sql(f'drop table if exists {table}')

    # Setup a streaming query
    # In this case we want to store in an in-memory table (the sink).
    # The query name will be the table name
    # After executing the code, the streaming computation will start in the backgro
    query = ( df
        .writeStream
        .queryName(f'{table}')
        .outputMode("append") # append, update
        .format("memory")
        .start()
    )
    return query

# Notice that in a production environment, we have to establish
# that the query is awaiting termination so to prevent the driver
# process from termination when the stream is ative

def await_termination(query):
    query.awaitTermination()

# Stopping the query process
def stop(query):
    query.stop()

# Show the status of the query
def show_status(spark, query):
    print(f'Active: {spark.streams.active[0].isActive}.')
    print(f'Status of query: {query.status}.')

# Figure out the tables we hold
def show_tables(spark):
    spark.sql("show tables").show(truncate=False)

# Check all the info stored, in the sink/table
def show_sink_table(spark, table):
    spark.sql(f'select * from {table}').show(truncate=False)


def udf_json_object(col_name, key):
    return F.get_json_object(json.loads(F.col(col_name)), key)

# Auxiliar udf function to deal with escaped characters in value
def value_json(value):
    return json.loads(value)

udf_value_json = F.udf(value_json, T.StringType())

# get DataFrame from table originated by Kafka
def get_table_dataframe(spark, table):
    df_kafka = spark.sql(f'select CAST(value AS STRING), topic, timestamp from {table}')
    
    # notice that value contains escaped characters e.g. \" 
    # "{\"ASIN\": \"1250150183\", \"GROUP\": \"book\", \"FORMAT\": \"hardcover\", 
    # \"TITLE\": \"The Swamp: Washington's Murky Pool of Corruption and Cronyism and How Trump Can Drain It\", 
    # \"AUTHOR\": \"Eric Bolling\", \"PUBLISHER\": \"St. Martin's Press\"}" 

    df_kafka = (df_kafka
                .withColumn('jsonvalue', udf_value_json(F.col('value')))
                .withColumn('Year', F.get_json_object(F.col('jsonvalue'), '$.Year'))
                .withColumn('Quarter', F.get_json_object(F.col('jsonvalue'), '$.Quarter'))
                .withColumn('Month', F.get_json_object(F.col('jsonvalue'), '$.Month'))
                .withColumn('DayOfWeek', F.get_json_object(F.col('jsonvalue'), '$.DayOfWeek'))
                .withColumn('Operated_or_Branded_Code_Share_Partners', F.get_json_object(F.col('jsonvalue'), '$.Operated_or_Branded_Code_Share_Partners'))
                .withColumn('Operating_Airline', F.get_json_object(F.col('jsonvalue'), '$.Operating_Airline'))
                .withColumn('Flight_Number_Operating_Airline', F.get_json_object(F.col('jsonvalue'), '$.Flight_Number_Operating_Airline'))
                .withColumn('CRSDepTime', F.get_json_object(F.col('jsonvalue'), '$.CRSDepTime'))
                .withColumn('DepTime', F.get_json_object(F.col('jsonvalue'), '$.DepTime'))
                .withColumn('TaxiOut', F.get_json_object(F.col('jsonvalue'), '$.TaxiOut'))
                .withColumn('WheelsOff', F.get_json_object(F.col('jsonvalue'), '$.WheelsOff'))
                .withColumn('WheelsOn', F.get_json_object(F.col('jsonvalue'), '$.WheelsOn'))
                .withColumn('TaxiIn', F.get_json_object(F.col('jsonvalue'), '$.TaxiIn'))
                .withColumn('CRSElapsedTime', F.get_json_object(F.col('jsonvalue'), '$.CRSElapsedTime'))
                .withColumn('ActualElapsedTime', F.get_json_object(F.col('jsonvalue'), '$.ActualElapsedTime'))
                .withColumn('AirTime', F.get_json_object(F.col('jsonvalue'), '$.AirTime'))
                .withColumn('Distance', F.get_json_object(F.col('jsonvalue'), '$.Distance'))
                .withColumn('DistanceGroup', F.get_json_object(F.col('jsonvalue'), '$.DistanceGroup'))
                .withColumn('OriginCityNameState', F.get_json_object(F.col('jsonvalue'), '$.OriginCityNameState'))
                .withColumn('DestCityNameState', F.get_json_object(F.col('jsonvalue'), '$.DestCityNameState'))
                .withColumn('Origin', F.get_json_object(F.col('jsonvalue'), '$.Origin'))
                .withColumn('Dest', F.get_json_object(F.col('jsonvalue'), '$.Dest'))

                .withColumn('DepDelay', F.get_json_object(F.col('jsonvalue'), '$.DepDelay'))
                .withColumn('CRSArrTime', F.get_json_object(F.col('jsonvalue'), '$.CRSArrTime'))
                .withColumn('ArrTime', F.get_json_object(F.col('jsonvalue'), '$.ArrTime'))
                .withColumn('ArrDelay', F.get_json_object(F.col('jsonvalue'), '$.ArrDelay'))
     )
    
    df_kafka = df_kafka.drop("value")
    df_kafka = df_kafka.drop("topic")
    df_kafka = df_kafka.drop("timestamp")
    df_kafka = df_kafka.drop("jsonvalue")

    df_kafka = (df_kafka
        .withColumn('Year', df_kafka["Year"].cast("integer"))
        .withColumn('Quarter', df_kafka["Quarter"].cast("integer"))
        .withColumn('Month', df_kafka["Month"].cast("integer"))
        .withColumn('DayOfWeek', df_kafka["DayOfWeek"].cast("integer"))
        .withColumn('Operated_or_Branded_Code_Share_Partners', df_kafka["Operated_or_Branded_Code_Share_Partners"].cast("string"))
        .withColumn('Operating_Airline', df_kafka["Operating_Airline"].cast("string"))
        .withColumn('Flight_Number_Operating_Airline', df_kafka["Flight_Number_Operating_Airline"].cast("integer"))
        .withColumn('Origin', df_kafka["Origin"].cast("string"))
        .withColumn('Dest', df_kafka["Dest"].cast("string"))
        .withColumn('CRSDepTime', df_kafka["CRSDepTime"].cast("integer"))
        .withColumn('DepTime', df_kafka["DepTime"].cast("integer"))
        .withColumn('TaxiOut', df_kafka["TaxiOut"].cast("double"))
        .withColumn('WheelsOff', df_kafka["WheelsOff"].cast("integer"))
        .withColumn('WheelsOn', df_kafka["WheelsOn"].cast("integer"))
        .withColumn('TaxiIn', df_kafka["TaxiIn"].cast("double"))
        .withColumn('CRSElapsedTime', df_kafka["CRSElapsedTime"].cast("double"))
        .withColumn('ActualElapsedTime', df_kafka["ActualElapsedTime"].cast("double"))
        .withColumn('AirTime', df_kafka["AirTime"].cast("double"))
        .withColumn('Distance', df_kafka["Distance"].cast("double"))
        .withColumn('DistanceGroup', df_kafka["DistanceGroup"].cast("integer"))
        .withColumn('OriginCityNameState', df_kafka["OriginCityNameState"].cast("string"))
        .withColumn('DestCityNameState', df_kafka["DestCityNameState"].cast("string"))

        .withColumn('DepDelay', df_kafka["DepDelay"].cast("integer"))
        .withColumn('CRSArrTime', df_kafka["CRSArrTime"].cast("integer"))
        .withColumn('ArrTime', df_kafka["ArrTime"].cast("integer"))
        .withColumn('ArrDelay', df_kafka["ArrDelay"].cast("integer"))

    )


    df_kafka = df_kafka.withColumn("Tem_ArrDelay", F.when(df_kafka["ArrDelay"]>5,1).otherwise(0))
    
    return df_kafka

