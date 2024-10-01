#!/bin/bash

python -m http.server 8000 --directory logs & tail -f /dev/null