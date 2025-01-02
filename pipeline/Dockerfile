# Use the official Apache Spark image with Python 3 support as the base image
FROM python:3.9-slim

# Set environment variables for Spark and Python
ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYTHONHASHSEED=0

# Define environment variables for paths
ENV DB_PATH=/opt/db \
    CATH_FOLDCASSDB_PATH=/opt/db/cath_foldclassdb \
    PPL_PATH=/opt/pipeline \
    MATPLOTLIB_CACHE=/tmp/matplotlib

USER root
RUN apt-get update && apt-get install -y git wget && rm -rf /var/lib/apt/lists/*

# Create DB directory with mode 0755
RUN mkdir -p "$DB_PATH" && \
    chmod 0755 "$DB_PATH"

# Download CATH Foldclass DB
RUN wget  http://bioinfadmin.cs.ucl.ac.uk/downloads/merizo_search/cath_foldclassdb.tar.gz -O "$DB_PATH/cath_foldclassdb.tar.gz"

# Create CATH Foldclass DB directory with mode 0777
RUN mkdir -p "$CATH_FOLDCASSDB_PATH" && \
    chmod 0777 "$CATH_FOLDCASSDB_PATH"

# Unpack CATH Foldclass DB archive
RUN tar -xzf "$DB_PATH/cath_foldclassdb.tar.gz" -C "$CATH_FOLDCASSDB_PATH" && \
    rm "$DB_PATH/cath_foldclassdb.tar.gz"

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Copy requirements.txt to the Docker image
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Remove the requirements.txt after installation to reduce image size
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Remove the requirements.txt after installation to reduce image size
RUN rm /tmp/requirements.txt

# Copy merizo_search repository
# Clone the merizo_search repository
RUN git clone https://github.com/psipred/merizo_search.git /opt/merizo_search
RUN chmod -R 0755 /opt/merizo_search

# Ensure Matplotlib cache directory exists with mode 0777
RUN mkdir -p "$MATPLOTLIB_CACHE" && \
    chmod 0777 "$MATPLOTLIB_CACHE"

# Set working directory to PPL_PATH
WORKDIR "$PPL_PATH"

# [almalinux@ucabc46-worker-04-e24f991595 pipeline]$ docker images
# REPOSITORY                TAG       IMAGE ID       CREATED          SIZE
# custom-pyspark-executor   latest    b8ad63b04318   23 seconds ago   5.82GB