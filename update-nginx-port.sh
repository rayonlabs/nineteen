#!/bin/bash

PORT="80"

while getopts "p:" opt; do
    case $opt in
        p) PORT="$OPTARG";;
        \?) echo "Invalid option: -$OPTARG" >&2; exit 1;;
    esac
done

if [ -z "$PORT" ]; then
    echo "Error: Port number is required. Usage: $0 -p <port_number>"
    exit 1
fi

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    echo "Error: Invalid port number. Port must be between 1 and 65535"
    exit 1
fi

sed -i "s/80:80/${PORT}:${PORT}/" docker-compose.yml
sed -i "s/localhost:80/localhost:${PORT}/" docker-compose.yml
if [ $? -eq 0 ]; then
    echo "docker-compose.yml updated successfully."
else
    echo "Failed to update docker-compose.yml. Please check the file or port value."
    exit 1
fi

echo "Replacing port in nginx/nginx.conf..."
sed -i "s/listen 80;/listen ${PORT};/" nginx/nginx.conf
if [ $? -eq 0 ]; then
    echo "nginx.conf updated successfully."
else
    echo "Failed to update nginx/nginx.conf. Please check the file or port value."
    exit 1
fi

exit 0
