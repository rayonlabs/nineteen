# Full instructions for setup - ninteen.ai

Welcome to SN19 Validating 🔥


## Contents:

- [Proxy server setup](#proxy-server-setup)
- [Orchestrator setup](#orchestrator-setup)
- [Managing organic access](#managing-organic-access)
- [Recommended compute](./recommended-compute)

See [helpful commands](./helpful-commands.md) for more information on the docker commands

# Overview

Validating on ninteen is special.

Not only do you validate miners are behaving, set some weights and get some tao - you also get to sell your access to these miners 🤩


A Validator consists of two parts:

- Proxy API server
- Orchestrator server

The proxy server is the server which has your hotkey,  spins up the NODE, allows you to sell your bandwidth, etc. 

The Orchestrator performs the checking tasks, to make sure the miners are behaving 🫡


# Proxy server setup

Get a CPU VM (Digital Ocean Droplet, OVH, Vultr, etc)  - make sure you have an open port if you want to run a organic API server.

**Clone the repo**
```bash
git clone https://github.com/namoray/nineteen.git
cd nineteen
```

## Install system dependencies

If you are in a container, run these:

**With autoupdates (recommended, especially now we have child hotkeys)**

```bash
sudo -E ./bootstrap.sh
source $HOME/.bashrc
source $HOME/.venv/bin/activate
```
Your server will now automatically be running - but it wont work until the config has been created

**Without autoupdates :-(**

If you are concerned about running autoupdates because of security, please message me and I'm sure we can work something out!
```bash
WITH_AUTOUPDATES=0 sudo -E ./bootstrap.sh 
```


## Get hot and coldkeys onto your machine

Securely move them onto your machine as usual. Either with the btcli or with a secure method of your choosing.


## Create the necessary config

(Add --dev flag if you are a developer on nineteen)
```bash
python core/create_config.py
```


If you're running the autoupdater, then you should be running!

Check your autoupdater to see the docker containers building (might take a few minutes on first build)
```bash
pm2 logs validator_autoupdater
```

## Start the services if you don't have autoupdates

(If you are a dev, go to dev.md docs now)

```bash
./utils/launch_validator.sh
```

See [helpful commands](./helpful-commands.md) for more information on the docker commands

## If you ever need to set weights manually to stop dereg etc

Run 
```bash
task set_weights
```

If you don't have any values in the db it will use the metagraph. It warns you and gives you time to stop this, if that is not something you want to do

## Trouble shooting

**Problem**:

Error: pq: password authentication failed for user "user"

**Solution**:

```bash
sudo rm -rf postgres_data
docker compose --env-file .vali.env -f docker-compose.yml  up -d --build
```


# Managing organic access

**Note this is optional - only if you want to sell your bandwidth. If you don't, you are done!**

During the config setup, add an 'ORGANIC_SERVER_PORT' when asked for it.

Then use 
```bash
./cli.sh --help
``` 
To manage all your api keys and similar stuff. Your service will now start up on the port you specified :)

Go to http://ip_address:port/docs to see all the swagger docs


# Choosing your own custom config

**You can now choose any hugging-face text2text model you like (LLM's), as well as any hugging face Text-to-image model you like!**

Miners will be posted of the details during every handshake, and you will be able to view the settings of each validator on https://nineteen.ai/marketplace

See the file [example_config.py](../core/example_config.py) to see how to extend the default config. Instructions are
at the top of the file!

# Orchestrator setup

## Starting the Orchestrator server

Currently, a bare metal GPU is necessary for validating the GPU models. Please see here for the full instructions!(https://github.com/namoray/vision-workers/tree/main/validator_orchestrator#readme)

Once this is done, make a note of the IP address of that machine, and the port the orchestrator is running on (the default is 6920, if you didn't change anything)

