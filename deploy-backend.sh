#!/bin/bash
set -e

echo "==> Wechsel in Projektordner"
cd /home/ubuntu/halloch-backend

echo "==> Installiere Abhaengigkeiten"
npm ci

echo "==> Baue Backend"
npm run build

echo "==> Starte Backend mit PM2 neu"
pm2 restart ecosystem.config.cjs --only halloch-backend --update-env

echo "==> Warte kurz"
sleep 3

echo "==> Pruefe lokalen Health Endpoint"
curl -fsS http://127.0.0.1:3001/health

echo ""
echo "Backend-Deployment erfolgreich"
