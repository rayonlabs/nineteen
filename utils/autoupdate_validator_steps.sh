#!/bin/bash

# THIS FILE CONTAINS THE STEPS NEEDED TO AUTOMATICALLY UPDATE THE REPO ON A TAG CHANGE
# THIS FILE ITSELF MAY CHANGE FROM UPDATE TO UPDATE, SO WE CAN DYNAMICALLY FIX ANY ISSUES


docker compose --env-file .vali.env -f docker-compose.yml run -e LOCALHOST=false --entrypoint "python src/migration.py" control_node
./utils/launch_validator.sh
echo "Autoupdate steps complete :)"
