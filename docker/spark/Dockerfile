# Ghim phiên bản: Spark 3.5.3 (Scala 2.12) + Delta 3.2.1 + hadoop-aws 3.3.4 + spark-sql-kafka 3.5.3
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MVN}/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

USER root
# --no-deps: pyspark đã có sẵn trong image
RUN pip install --no-cache-dir --no-deps delta-spark==3.2.1 \
 && pip install --no-cache-dir pyyaml==6.0.2 prometheus-client==0.21.0
USER spark
