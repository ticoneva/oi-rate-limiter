#!/bin/bash
# Launch Open Webui with Docker, backed by postgresql and redis.

# Environment variables
REDIS_URL="redis://redis-valkey:6379/0"
ENABLE_WEBSOCKET_SUPPORT="true"
WEBSOCKET_MANAGER="redis"
WEBSOCKET_REDIS_URL="redis://redis-valkey:6379/1"
CORS_ALLOW_ORIGIN="https://scrp-chat.econ.cuhk.edu.hk;https://scrp-chat-2.econ.cuhk.edu.hk"

# Get IP of Redis
#redis_ip=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' redis-valkey)
#echo $redis_ip

# Launch WebUI
docker run -d \
  --name open-webui-v0.8.10 \
  --log-driver json-file \
  --log-opt max-size=10m \
  --log-opt max-file=5 \
  --network openwebui-network \
  -p 8086:8080 \
  -v /opt/network/scrp-chat/open-webui-v0.8.10-data/:/app/backend/data \
  -e JWT_EXPIRES_IN="1h" \
  -e WEBUI_SECRET_KEY="32f23-23jnf8932rfjneq-pys012n" \
  -e REDIS_URL="redis://redis-valkey:6379/0" \
  -e ENABLE_WEBSOCKET_SUPPORT="true" \
  -e WEBSOCKET_MANAGER="redis" \
  -e WEBSOCKET_REDIS_URL="redis://redis-valkey:6379/1" \
  -e REDIS_KEY_PREFIX="open-webui" \
  -e CORS_ALLOW_ORIGIN="https://scrp-chat.econ.cuhk.edu.hk;https://scrp-chat-2.econ.cuhk.edu.hk" \
  -e DATABASE_URL=postgresql://openwebui_user:scrp-postgresql-pwd@scrp-control-2:5432/openwebui_test \
  -e DATABASE_POOL_SIZE=15 \
  -e DATABASE_POOL_MAX_OVERFLOW=20 \
  -e UVICORN_WORKERS=4 \
  -e VECTOR_DB=chroma \
  -e CHROMA_HTTP_HOST="chroma-server" \
  -e CHROMA_HTTP_PORT=8000 \
  ghcr.io/open-webui/open-webui:v0.8.10

