import streamlit as st
import time
import pandas as pd
from pyspark.ml.tuning import TrainValidationSplitModel
# Importing critical functions that deal with data stream (Spark/Kafka side)
from data_streaming import ( spark_initialize, data_stream_spark, 
                show_tables, show_status, get_table_dataframe )
from pyspark.sql.functions import monotonically_increasing_id

# Caching the function that will access the running 
# Spark/Kafka data query (a DataFrame)
@st.cache_resource
def get_data():
    return get_table_dataframe(st.session_state.spark, st.session_state.table)

# Showing results of data stream processing, 
# as long as there is a SparkSession running
def results():

    if 'spark' not in st.session_state:
        return
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    placeholder = st.empty()
    sleeptime = 2
    maxiterations = 30

    modelo = TrainValidationSplitModel.load("../../modelo")

    # Iterative update
    for i in range(maxiterations):
        time.sleep(sleeptime)
        # getting data at this point in time
        df = get_data()
        df_modelo = df.drop(*["DepDelay","ArrDelay","Tem_ArrDelay","CRSArrTime","ArrTime"])
        previsao = modelo.transform(df_modelo)

        status_text.warning(f'A processar...  Iteração numero {i+1}/{maxiterations}. Numero de dados processados até agora: {df.count()}.')



        df_author = df.groupBy('Tem_ArrDelay').count().orderBy('count', ascending=False).limit(20).toPandas()

        # trocar estes dois \/
        df_delay = df.groupBy('Tem_ArrDelay').count().orderBy('count', ascending=False).limit(20).toPandas()
        #df_prev = previsao.groupBy('prediction').count().orderBy('count', ascending=False).limit(20).toPandas()


        with placeholder.container():

            # Each chart in one column, so two columns required
            fig_col1, fig_col2 = st.columns(2)
            with fig_col1:
                st.markdown('### Delay entre voos')
                st.markdown(f'**Distribuição do delay dos aviões no dataset**')
                st.bar_chart(data=df_author, y='count', x='Tem_ArrDelay', use_container_width=True)

            with fig_col2:
                st.markdown('### Previsões do Delay')
                st.markdown('**Distribuição das previsões do delay feitas pelo modelo**')
                # trocar estes dois \/
                st.bar_chart(data=df_delay, y='count', x='Tem_ArrDelay', use_container_width=True)
                #st.bar_chart(data=df_prev, y='count', x='prediction', use_container_width=True)


            # Show the related dataframes
            st.markdown('### Detailed tables view')
            st.markdown('**Dados a ser recebidos**')
            st.dataframe(df)
            #st.markdown('**Dados recebidos**')
            #st.dataframe(df.select('Tem_ArrDelay').tail(5))
            st.markdown('**Previsoes recebidas em realtime**')
            st.dataframe(previsao.select('prediction').tail(5))
    
        progress_bar.progress(i)
  
    progress_bar.empty()
    status_text.success(f'Resultados finais ao fim de processar {df.count()} dados.')

# Page to hold results
def page_results():
    st.empty()
    st.header('Data streaming')
    st.subheader('Results')
    results()
    
# Page to hold information about the app
def page_about():
    st.empty()
    st.header('Projeto final de Algoritmos para Big Data')
    st.subheader('Realizado por:')
    st.write('Bruno Ramos, 127521 ')
    st.write('Sara Esmeraldo, 129233')
    st.write('Vicente Chã, 127688')
    
# Entry point
def main():
    
    # Page config
    st.set_page_config(
        page_title = 'Flights data streaming',
        initial_sidebar_state = 'expanded',
        layout = 'wide'
    )
    # App title
    st.title('Flights data streaming')
    st.divider()
    with st.sidebar:
        st.empty()
        st.header('Algoritmos para Big Data')

    brokers = 'localhost:9092'
    topic = 'flights'
    table = 'flights'

    # As code is running everytime the user interacts with, 
    # we must make sure that the spark side only starts once

    if 'spark' not in st.session_state:
        spark = spark_initialize()
        query = data_stream_spark(spark, brokers, topic, table)
        st.session_state.spark = spark
        st.session_state.table = table
        # just to check in the terminal
        show_status(spark, query)
        show_tables(spark)
        
    pages = [ st.Page(page_results, title='Results'),
              st.Page(page_about, title='About'),
            ]
    pg = st.navigation(pages)
    pg.run()

# Execution
if __name__ == "__main__":
    main()
