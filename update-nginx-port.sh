#!/bin/bash

# replace port in docker-compose.yml
sed -i "s/\"80:6919\"/\"80:$ORGANIC_SERVER_PORT\"/" docker-compose.yml
sed -i "s/- 6919/- $ORGANIC_SERVER_PORT/" docker-compose.yml

# replace port in nginx.conf
sed -i "s/server hybrid_node:6919/server hybrid_node:$ORGANIC_SERVER_PORT/" nginx/nginx.conf
sed -i "s/listen 6919/listen $ORGANIC_SERVER_PORT/" nginx/nginx.conf
sed -i "s/localhost:6919/localhost:$ORGANIC_SERVER_PORT/" nginx/nginx.conf

# update healthcheck ports
sed -i "s/:6919\/docs/:$ORGANIC_SERVER_PORT\/docs/" docker-compose.yml
sed -i "s/:6919\/health/:$ORGANIC_SERVER_PORT\/health/" docker-compose.yml