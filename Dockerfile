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
RUN pip install -r /app/requirements.txt && \
    chown -R ${USER_ID}:${GROUP_ID} /usr/local/lib/python3.11/site-packages/elementary/monitor/dbt_project/

# Install dbt packages at build time (not runtime)
COPY packages.yml dbt_project.yml profiles.yml /app/
COPY macros/ /app/macros/
COPY seeds/ /app/seeds/
RUN dbt deps --profiles-dir /app --project-dir /app

# Copy remaining files
COPY --chown=appuser:appgroup . /app/

# Permissions for DBT directories
RUN mkdir -p /app/dbt_packages /app/www && \
    chown -R appuser:appgroup /app && \
    chmod -R 755 /app

# Setup .dbt profiles directory (symlink so bind-mount changes are picked up)
RUN mkdir -p /home/appuser/.dbt && \
    ln -sf /app/profiles.yml /home/appuser/.dbt/profiles.yml && \
    chown -R appuser:appgroup /home/appuser/.dbt

# Writable runtime directory structure — symlinked from /app so dbt/edr
# write to a mountable path, enabling read_only_root_filesystem in k8s
ENV RUNTIME_DATA_DIR=/data
RUN rm -rf /app/logs /app/reports /app/target /app/edr_target && \
    mkdir -p /data/logs /data/reports /data/target /data/edr_target && \
    ln -sfn /data/logs /app/logs && \
    ln -sfn /data/reports /app/reports && \
    ln -sfn /data/target /app/target && \
    ln -sfn /data/edr_target /app/edr_target && \
    ln -sfn /data/logs /app/www/logs && \
    ln -sfn /data/reports /app/www/reports && \
    chown -R appuser:appgroup /data

# DBT project path
ENV DBT_PROJECT_PATH=/app

EXPOSE 8000

USER appuser

CMD ["python", "app/observability_server.py"]
