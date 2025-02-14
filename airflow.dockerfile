# Use the Airflow 2.7.2 image with Python 3.9 as the base
FROM apache/airflow:2.7.2-python3.9

# Switch to root user to install additional packages
USER root

# Install Java and required dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch to the Airflow user
USER airflow

# Install the Apache Airflow Spark provider
RUN pip install pyspark==3.3.2
RUN pip install apache-airflow-providers-apache-spark==4.0.1

# Ensure compatibility with Airflow's environment
RUN airflow db check

# Expose ports for Airflow
EXPOSE 8080
