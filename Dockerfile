FROM python:3.11-slim

# Update and install system dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends --yes \
    gcc libpq-dev python3-dev git curl iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN addgroup --gid $GROUP_ID appgroup && \
    adduser --disabled-password --gecos '' --uid $USER_ID --gid $GROUP_ID appuser

# Setup the working directory
WORKDIR /app
RUN chown -R appuser:appgroup /app && chmod -R 755 /app

# Switch to non-root user
USER appuser

# Copy the Python requirements and install them
COPY requirements.txt /app/requirements.txt
RUN pip install --user -r /app/requirements.txt

# Copy dbt project
COPY dbt_project.yml /app/dbt_project.yml

# Copy macros, models and seeds
COPY /macros /app/macros
COPY /models /app/models
COPY /seeds /app/seeds

RUN chmod +x cron.sh
COPY cron.sh /app/cron.sh

RUN chmod +x forever.sh
COPY forever.sh /app/forever.sh

# Set environment variable to specify the DBT project path
ENV DBT_PROJECT_PATH /app/src

# Optionally expose a port for dbt docs if needed
EXPOSE 8080

# Set PATH to include user-level binaries
ENV PATH=/home/appuser/.local/bin:$PATH
