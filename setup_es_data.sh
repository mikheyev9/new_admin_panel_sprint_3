#!/bin/bash

# This script sets up the required directory and permissions for Elasticsearch data
# Elasticsearch runs as user with UID 1000 and requires specific permissions to access data directory

# Create elasticsearch data directory (-p flag creates parent directories if they don't exist)
sudo mkdir -p ./data/es_data

# Change ownership of the directory to UID 1000 (default elasticsearch user)
# Format: chown user:group directory
sudo chown -R 1000:1000 ./data/es_data

# Set full read/write/execute permissions (777) for the directory
# This ensures elasticsearch can fully access the directory
sudo chmod -R 777 ./data/es_data