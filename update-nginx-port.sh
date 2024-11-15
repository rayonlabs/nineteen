#!/bin/bash

# replace port in docker-compose.yml
sed -i "s/"80:6919"/"${ORGANIC_PORT}:6919"/" docker-compose.yml

# replace port in nginx.conf
sed -i "s/listen 80/listen ${ORGANIC_PORT}/" nginx/nginx.conf

# update healthcheck ports
sed -i "s/:6919\/docs/:$ORGANIC_SERVER_PORT\/docs/" docker-compose.yml
sed -i "s/:6919\/health/:$ORGANIC_SERVER_PORT\/health/" docker-compose.yml