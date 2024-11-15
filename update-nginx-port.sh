#!/bin/bash

sed -i "s/80:6919/${ORGANIC_PORT}:6919/" docker-compose.yml
if [ $? -eq 0 ]; then
    echo "docker-compose.yml updated successfully."
else
    echo "Failed to update docker-compose.yml. Please check the file or ORGANIC_PORT value."
fi

echo "Replacing port in nginx/nginx.conf..."
sed -i "s/listen 80;/listen ${ORGANIC_PORT};/" nginx/nginx.conf
if [ $? -eq 0 ]; then
    echo "nginx.conf updated successfully."
else
    echo "Failed to update nginx/nginx.conf. Please check the file or ORGANIC_PORT value."
fi