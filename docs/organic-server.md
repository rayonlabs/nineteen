
# How to convert a synthetic server to an organic server

1. Add Organic port to your .vali.env file
```bash
echo "ORGANIC_SERVER_PORT=8090" >> .vali.env
```

Feel free to change the port to whatever you want.

2. Restart the validator - and start the organic server (entry node)
```bash
./utils/launch_validator.sh
```

3. Manage your API keys and similar stuff with the cli
```bash
./cli.sh --help
```