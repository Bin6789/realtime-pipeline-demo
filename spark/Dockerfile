FROM bitnami/spark:3.5.1

USER root

# ================================
# Cài Python + pip + wget
# ================================
RUN apt-get update && apt-get install -y python3-pip wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ================================
# Thư viện Python cần thiết
# ================================
RUN pip3 install --no-cache-dir requests \
    elasticsearch==8.7.0 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.6 

# ================================
# JAR cho Kafka, PostgreSQL, MinIO/S3
# (đặt đúng thư mục /opt/bitnami/spark/jars)
# ================================
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.1/snappy-java-1.1.10.1.jar


USER 1001
