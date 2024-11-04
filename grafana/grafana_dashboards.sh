#!/bin/bash

# Load environment variables from .env file
source .env

# Create a folder to store your backups
BACKUP_DIR="./grafana_dashboards_backup"
mkdir -p $BACKUP_DIR

# Get the list of all dashboards
curl -H "Authorization: Bearer $API_TOKEN" "$GRAFANA_URL/api/search?query=&" | jq '.[] | .uid' | sed 's/"//g' | while read dashboard_uid
do
    # Download the dashboard by UID
    echo "Downloading dashboard UID: $dashboard_uid"
    curl -H "Authorization: Bearer $API_TOKEN" "$GRAFANA_URL/api/dashboards/uid/$dashboard_uid" | jq '.' > "$BACKUP_DIR/$dashboard_uid.json"
done
