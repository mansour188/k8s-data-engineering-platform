FROM spark:4.0.4

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o /opt/spark/jars/hadoop-aws-3.4.1.jar

RUN curl -fL \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    -o /opt/spark/jars/bundle-2.24.6.jar

RUN curl -fL \
    https://jdbc.postgresql.org/download/postgresql-42.7.8.jar \
    -o /opt/spark/jars/postgresql-42.7.8.jar

COPY jobs/bronze_to_silver.py /opt/spark/jobs/bronze_to_silver.py
COPY jobs/silver_to_gold.py /opt/spark/jobs/silver_to_gold.py
COPY jobs/gold_to_postgres.py /opt/spark/jobs/gold_to_postgres.py
