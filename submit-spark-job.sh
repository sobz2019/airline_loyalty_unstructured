#!/bin/bash

# Get AWS credentials from .env file
source .env

docker exec -it airline_loyalty_unstructured-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.security.authentication=simple \
  --conf spark.hadoop.security.authorization=false \
  --conf spark.hadoop.user.name=hadoop \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_KEY} \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf "spark.driver.extraJavaOptions=-Divy.home=/opt/bitnami/spark/.ivy2" \
  --conf "spark.executor.extraJavaOptions=-Divy.home=/opt/bitnami/spark/.ivy2" \
  --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
  --repositories https://repo1.maven.org/maven2/ \
  /opt/bitnami/spark/jobs/main.py