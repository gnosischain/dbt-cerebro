FROM python:3.11-slim

# Update and install system dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends --yes \
    gcc libpq-dev python3-dev git curl iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup the working directory
WORKDIR /app

# Copy the Python requirements and install them
COPY /requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Set environment variable to specify the DBT project path
ENV DBT_PROJECT_PATH /app/src

# Optionally expose a port for dbt docs if needed
EXPOSE 8080