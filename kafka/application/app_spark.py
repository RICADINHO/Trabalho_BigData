import time
#from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplitModel
from pyspark.sql.functions import monotonically_increasing_id




# Importing critical functions that deal with data stream
from data_streaming import ( spark_initialize, data_stream_spark, 
            show_status, show_tables, show_sink_table, get_table_dataframe )

brokers = 'localhost:9092'
topic = 'flights'
table = 'flight'



# Showing results of data stream processing
def show_spark_results(spark, table):
    df = get_table_dataframe(spark, table)
    # cols_interest = ['timestamp','Asin','Group','Format','Title','Author','Publisher']
    
    sleeptime = 3
    maxiterations = 30

    modelo = TrainValidationSplitModel.load("../../modelo")
    print("modelo randomforest carregado")

    # Iterative update
    for i in range(maxiterations):
        time.sleep(sleeptime)
        print(f'Processing...  Iteration {i} with in-between delay of {sleeptime} second(s)')
        print(f'Number of records processed so far: {df.count()}.')

        #df.select(cols_interest).show(truncate=False)
        #df.show(5, truncate=False)
        #show_sink_table(spark, table)
        df.printSchema()
        #df.show(2)

        df_modelo = df.drop(*["DepDelay","ArrDelay","Tem_ArrDelay","CRSArrTime","ArrTime"])

        previsao = modelo.transform(df_modelo)
        #previsao.select("features", "prediction").show()
        #previsao.join(previsao,df_modelo["Tem_ArrDelay"])
        previsoes = previsao.select("features", "prediction").tail(10)

        for j in previsoes:
            print(j)

        #print('Aggregated information as it stands (top 20):')
        #df.groupBy('Year').count().orderBy('count', ascending=False).show(truncate=False)
        #df.groupBy('Operating_Airline').count().orderBy('count', ascending=False).show(truncate=False)
        #df.groupBy('Group', 'Format').count().show(truncate=False)
        

# Execution

spark = spark_initialize()
query = data_stream_spark(spark, brokers, topic, table)

show_status(spark, query)
show_tables(spark)
show_sink_table(spark, table)
show_spark_results(spark, table)
