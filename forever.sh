#!/bin/bash

mkdir -p /app/www
ln -sfn /app/logs /app/www/logs
ln -sfn /app/reports /app/www/reports
python -m http.server 8000 --directory /app/www & tail -f /dev/null
