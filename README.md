# Trabalho final de Algoritmos para Big Data



## Inicializar o kafka:

### download

- pip install kafka-python

### no terminal da pasta kafka_2.13-4.0.0

- KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
- bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
- bin/kafka-server-start.sh config/server.properties

### no terminal da pasta kafka

- python3 kafka_producer.py

### no terminal da pasta application

- python3 app_spark.py


## Inicializar o streamlit:

### qualquer terminal:

- pip install streamlit

### no terminal da pasta application

- streamlit run app_streamlit.py