import time
#from pyspark.sql import SparkSession
from pyspark.ml.tuning import TrainValidationSplitModel




# Importing critical functions that deal with data stream
from data_streaming import ( spark_initialize, data_stream_spark, 
            show_status, show_tables, show_sink_table, get_table_dataframe )

brokers = 'localhost:9092'
topic = 'flights'
table = 'flight'



# Showing results of data stream processing
def show_spark_results(spark, table):
    df = get_table_dataframe(spark, table)
    
    sleeptime = 3
    maxiterations = 5

    modelo = TrainValidationSplitModel.load("../../modelo")
    print("modelo randomforest carregado")
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    # Iterative update
    for i in range(maxiterations):
        time.sleep(sleeptime)
        print(f'\n\n\nIteração numero {i+1}/{maxiterations}:')
        print(f'Numero de dados processados até agora: {df.count()}\n')

        df_modelo = df.drop(*["DepDelay","ArrDelay","Tem_ArrDelay","CRSArrTime","ArrTime"])
        previsao = modelo.transform(df_modelo)

        previsoes = previsao.select("features", "prediction").tail(10)
        real = df.select("ArrDelay","Tem_ArrDelay").tail(10)

        print("Valores das previsões do delay para os novos dados lidos:\n")
        for j in range(0,len(previsoes)):
            print(f"Previsao: {previsoes[j][1]}, Valor real: {real[j][1]} ")

        
spark = spark_initialize()
query = data_stream_spark(spark, brokers, topic, table)

show_status(spark, query)
show_tables(spark)
show_sink_table(spark, table)
show_spark_results(spark, table)



print("\n\n\n\n\n\n\n\n\n")
print("+-------------------------------------------------------------------------+")
print("|                                                                         |")
print("|                           STREAMING TERMINADO                           |")
print("|                                                                         |")
print("+-------------------------------------------------------------------------+")
print("\n\n\n\n\n\n\n\n\n")