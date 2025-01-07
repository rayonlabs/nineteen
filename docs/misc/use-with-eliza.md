# Guide to use SN19 with Eliza

### **Clone repo**

```
git clone https://github.com/elizaos/eliza.git
git checkout $(git describe --tags --abbrev=0)
cp .env.example .env
```


### **Edit env**
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

### **Set model to OpenAI**
- Go to `packages/core/src/defaultCharacter.ts`
- Edit `modelProvider` to default to `ModelProviderName.OPENAI`

### **Start server**
`sh scripts/start.sh` and wait for setup to finish

Refer to debugs at end of file for known fixes


### **Start client**
- Open new terminal, navigate to Eliza folder and run `pnpm start:client` 
- Navigate to UI on port 5173 and chat with agent




## Debugs:

- Update `scripts/start.sh` to include `if ! pnpm install --no-frozen-lockfile; then` in case `if ! pnpm install; then` causes issues 