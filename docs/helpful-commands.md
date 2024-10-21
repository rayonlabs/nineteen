Helpful Docker Commands

## Docker

### Start services
```bash
docker compose --env-file .vali.env -f docker-compose.yml up -d
```

### View logs
```bash
docker compose --env-file .vali.env -f docker-compose.yml logs -f
```

### Stop services
```bash
docker compose --env-file .vali.env -f docker-compose.yml down
```

### Rebuild and start services
```bash
docker compose --env-file .vali.env -f docker-compose.yml up --build
```

### View running containers
```bash
docker ps
```