# Full instructions for setup

Welcome to S19 Mining 🔥


## Contents:

- [Worker server setup](#worker-server-setup)
- [Proxy server setup](#proxy-server-setup)
- [LLM server configs](#model-configs)
- [Recommended compute](./recommended-compute)


# Overview
A miner consists of several parts, fitting into two categories:

- Proxy
- Workers

The proxy server is the server which has your hotkey, and spins up the NODE (should probably be on a CPU close to your GPU worker servers). The worker servers are the GPU workers which perform the tasks!

# Worker server setup
Documentation for all the workers is contained here https://github.com/namoray/vision-workers

# Proxy server setup

Get a CPU VM (Digital Ocean Droplet, OVH, Vultr, etc)  - make sure you have an open port if you want to run a organic API server.

## Setup environment


### Clone the repo
```bash
git clone https://github.com/namoray/nineteen.git
cd nineteen
```

### Install system dependencies
```bash
NO_LAUNCH=1 sudo -E ./bootstrap.sh
source $HOME/.bashrc
pip install git+https://github.com/namoray/fiber.git
``` 

### Get hot and coldkeys onto your machine
Securely move them onto your machine as usual. Either with the btcli or with a secure method of your choosing.

## Create the config
```bash
python core/create_config.py --miner
```

## Post IP's to chain
Example command:
```bash
fiber-post-ip --netuid 176 --subtensor.network test --external_port 1234 --wallet.name default --wallet.hotkey default --external_ip 0.0.0.0
```

## Start miners
Example command
```bash
uvicorn miner.server:app --reload --host 0.0.0.0 --port 1234 --env-file .default.env --log-level debug &
```

Use the process manager of your choice
