FROM python:3.11-slim

# Install essential dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends --yes \
    gcc libpq-dev python3-dev git curl iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
ARG USER_ID=1000
ARG GROUP_ID=1000

RUN groupadd -g ${GROUP_ID} appgroup && \
    useradd -m -u ${USER_ID} -g appgroup appuser

# Setup working directory
WORKDIR /app

# Copy and install Python requirements first (better caching)
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Copy remaining files
COPY --chown=appuser:appgroup . /app/

# Permissions for DBT directories
RUN mkdir -p /app/dbt_packages /app/logs /app/target && \
    chown -R appuser:appgroup /app && \
    chmod -R 755 /app

# Setup .dbt profiles directory
RUN mkdir -p /home/appuser/.dbt && \
    cp /app/profiles.yml /home/appuser/.dbt/profiles.yml && \
    chown -R appuser:appgroup /home/appuser/.dbt

# DBT project path
ENV DBT_PROJECT_PATH /app

EXPOSE 8000

USER appuser

CMD ["/bin/bash", "-c", "exec python -m http.server 8000 --directory logs & tail -f /dev/null"]
