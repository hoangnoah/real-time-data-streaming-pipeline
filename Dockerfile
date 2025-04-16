FROM bitnami/spark:latest

RUN pip install cassandra-driver kafka-python

COPY spark_stream.py /opt/bitnami/spark/