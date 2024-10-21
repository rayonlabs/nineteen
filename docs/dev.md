# Instructions to help devs (Mainly Notes to self)

## Get started with bittensor

Use these https://docs.bittensor.com/ to help you with most steps
1. Create a wallet with a coldkey and 2 hotkeys - one for mining and one for validating.
    - Call the validator hotkey "vali"
    - Call the miner hotkey "1"
2. Have some testtao transferred to your coldkey from someone you know that has testtao (you can't use the faucet)
3. Register your hotkey for validating with the network (you need to do this before you can validate)
4. Register your hotkey for mining with the network (you need to do this before you can mine)
5. Follow the validating.md / mining.md docs first until you get to the notes telling you to come back here
    - Use port 4001 for the miner port


## For dev without docker:

1. Optional if you need a venv
```bash
python -m venv .venv || python3 -m venv .venv
```

2. Set up the venv and what not (dont worry about bittensor incompatibility warnings)
```bash
source .venv/bin/activate
find . -path "./venv" -prune -o -path "./.venv" -prune -o -name "requirements.txt" -exec pip install -r {} \;
pip install --no-cache-dir "git+https://github.com/rayonlabs/fiber.git@1.0.0#egg=fiber[full]"
task dev_setup
task control_node_dev  # For example for the validator control node
task m1_dev  # For example for the miner 1
```


3. **If you want to dev with fiber locally too [optional - probably ignore if you are not working on fiber]**
```bash
cd ..
git clone https://github.com/rayonlabs/fiber.git
cd fiber
git pull
pip install -e .
cd ..
cd nineteen
```

## To run the whole system with docker
```bash
docker compose --env-file .vali.env -f docker-compose.yml -f docker-compose.dev.yml --profile entry_node_profile up -d --build
```