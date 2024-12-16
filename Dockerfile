# Use an official Spark image with Hadoop and Python support
FROM bitnami/spark:latest

# Set environment variables for Spark
ENV SPARK_HOME /opt/bitnami/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Copy the PySpark script into the container
COPY anonymize_large_data.py /opt/bitnami/spark/

# COPY anonymize_test_data.py /opt/bitnami/spark/

# Default command to run Spark shell (can be overridden by docker-compose or CLI)
CMD ["spark-submit", "/opt/bitnami/spark/anonymize_large_data.py"]


# CMD ["spark-submit", "/opt/bitnami/spark/anonymize_test_data.py"]