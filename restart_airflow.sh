#!/bin/bash

# Stop and remove all containers
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

# Remove all containers, volumes and images related to the environment
#docker-compose down --volumes --rmi all

# Remove all process listening to port 8080
#lsof -i tcp:8080 | grep root | awk '{print $2}' | xargs kill -9

# Deploy airflow
docker-compose up -d