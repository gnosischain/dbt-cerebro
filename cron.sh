#!/bin/bash

dbt run -s execution_power

python -m http.server 8000 --directory logs & tail -f /dev/null



