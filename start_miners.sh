for env_file in .*.env; do
    if grep -q "NODE_PORT" "$env_file"; then
        node_port=$(grep "NODE_PORT" "$env_file" | cut -d '=' -f2)
        service_name="miner_$(echo $env_file | sed 's/\.//g' | sed 's/env//')"
        pm2 delete $service_name
        pm2 start --name "$service_name" "uvicorn miner.server:app --reload --host 0.0.0.0 --port $node_port --env-file $env_file --log-level debug"
    fi
done