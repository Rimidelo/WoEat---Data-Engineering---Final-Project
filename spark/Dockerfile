FROM tabulario/spark-iceberg

# Download Spark-Kafka connector and Kafka client JARs (matching Spark 3.3.0)
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.0/spark-token-provider-kafka-0-10_2.12-3.3.0.jar /opt/spark/jars/

# Add missing commons-pool2 dependency for Kafka streaming
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# Install Python requirements for streaming
RUN pip install kafka-python faker

# Set Spark UI port to 4050 (internal), Docker will map it to 4080 externally
ENV SPARK_UI_PORT=4050
RUN echo "spark.ui.port 4050" >> /opt/spark/conf/spark-defaults.conf 