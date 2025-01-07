# Guide to use SN19 with Eliza

## **Setup machine**
### nvm & node
```
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
source ~/.bashrc
nvm install 23.3.0
nvm use 23.3.0
nvm alias default 23.3.0
```

### npmp
```
curl -fsSL https://get.pnpm.io/install.sh | sh -
source ~/.bashrc
```

## **Clone repo**

```
git clone https://github.com/elizaos/eliza.git
cd eliza
git checkout $(git describe --tags --abbrev=0)
cp .env.example .env
rm -rf node_modules
rm -rf agent/node_modules
rm -rf packages/*/node_modules
rm pnpm-lock.yaml
pnpm install --no-frozen-lockfile
```


## **Edit env**
```
# AI Model API Keys
OPENAI_API_KEY=rayon_XXXXXX             # Get the key from https://nineteen.ai/app/api
OPENAI_API_URL=https://api.nineteen.ai/v1                
SMALL_OPENAI_MODEL=unsloth/Llama-3.2-3B-Instruct      
MEDIUM_OPENAI_MODEL=unsloth/Meta-Llama-3.1-8B-Instruct
LARGE_OPENAI_MODEL=hugging-quants/Meta-Llama-3.1-70B-Instruct-AWQ-INT4             
EMBEDDING_OPENAI_MODEL=        
IMAGE_OPENAI_MODEL=            
```

## **Set model to OpenAI**
- Go to `packages/core/src/defaultCharacter.ts`
- Edit `modelProvider` to default to `ModelProviderName.OPENAI`

## **Start server**
```
pnpm build
pnpm start
```

## **Start client**
- Open new terminal, navigate to Eliza folder and run `pnpm start:client` 
- Navigate to UI on port 5173 and chat with agent




## Debugs:

- Update `scripts/start.sh` to include `if ! pnpm install --no-frozen-lockfile; then` in case `if ! pnpm install; then` causes issues 