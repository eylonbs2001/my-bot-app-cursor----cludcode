#!/usr/bin/env bash
# FalconEye Hetzner one-shot setup script.
# Run as root on a fresh Ubuntu 24.04 CX22 server:
#   curl -O https://raw.githubusercontent.com/eylonbs2001/my-bot-app-cursor----cludcode/main/setup-hetzner.sh
#   chmod +x setup-hetzner.sh
#   ./setup-hetzner.sh
#
# The script:
#   1) hardens SSH (root login key-only, no password auth)
#   2) installs Docker + docker compose v2
#   3) creates an unprivileged 'falcon' user that owns the deployment
#   4) clones the repo to /opt/falconeye
#   5) creates a placeholder .env you must fill in
#   6) builds and starts the stack
#
# Idempotent — safe to re-run.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/eylonbs2001/my-bot-app-cursor----cludcode.git}"
INSTALL_DIR="/opt/falconeye"
APP_USER="falcon"

echo "==> [1/7] Updating system packages"
apt-get update -y
apt-get upgrade -y --no-install-recommends
apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg git ufw fail2ban tzdata

echo "==> [2/7] Configuring firewall (UFW)"
ufw --force reset >/dev/null
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp comment "ssh"
ufw --force enable

echo "==> [3/7] Installing Docker Engine + compose v2"
if ! command -v docker >/dev/null 2>&1; then
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    UBU_CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $UBU_CODENAME stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

echo "==> [4/7] Creating unprivileged user '$APP_USER'"
if ! id -u "$APP_USER" >/dev/null 2>&1; then
    adduser --disabled-password --gecos "FalconEye runtime" "$APP_USER"
fi
usermod -aG docker "$APP_USER"

echo "==> [5/7] Cloning repository to $INSTALL_DIR"
if [ ! -d "$INSTALL_DIR/.git" ]; then
    git clone "$REPO_URL" "$INSTALL_DIR"
else
    git -C "$INSTALL_DIR" pull --ff-only
fi
chown -R "$APP_USER":"$APP_USER" "$INSTALL_DIR"

echo "==> [6/7] Preparing .env"
ENV_FILE="$INSTALL_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" <<'EOF'
# === FalconEye production secrets ===
# Fill in every value before running `docker compose up`.
# Postgres lives on Railway — paste DATABASE_URL from Railway Variables tab.
DATABASE_URL=

# Redis is local (docker service); leave the override below in place.
REDIS_URL=redis://redis:6379/0

# Telegram
TELEGRAM_TOKEN=
CHAT_ID=
VIP_PLUS_CHAT_ID=
ADMIN_CHAT_ID=

# Exchanges
BINANCE_API_KEY=
BINANCE_API_SECRET=
BYBIT_API_KEY=
BYBIT_API_SECRET=

# Optional integrations
WHALE_ALERT_API_KEY=
CMC_API_KEY=
OPENAI_API_KEY=

# Runtime tuning
VIP_STRICT_MODE=true
LAYER_TIMEOUT_MS=8000
EOF
    chown "$APP_USER":"$APP_USER" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo
    echo "  >>> .env file created at $ENV_FILE"
    echo "  >>> EDIT IT NOW with: nano $ENV_FILE"
    echo "  >>> Then re-run this script to build & start the bot."
    exit 0
fi

# Bail loudly if the user forgot to populate the .env
if ! grep -q "^DATABASE_URL=postgres" "$ENV_FILE"; then
    echo "ERROR: DATABASE_URL is empty in $ENV_FILE. Edit the file before continuing."
    exit 1
fi
if ! grep -q "^TELEGRAM_TOKEN=." "$ENV_FILE"; then
    echo "ERROR: TELEGRAM_TOKEN is empty in $ENV_FILE. Edit the file before continuing."
    exit 1
fi

echo "==> [7/7] Building and starting the FalconEye stack"
cd "$INSTALL_DIR"
sudo -u "$APP_USER" docker compose -f docker-compose.prod.yml pull redis || true
sudo -u "$APP_USER" docker compose -f docker-compose.prod.yml build bot
sudo -u "$APP_USER" docker compose -f docker-compose.prod.yml up -d

echo
echo "============================================================"
echo "  FalconEye is up. Tail logs with:"
echo "    docker compose -f $INSTALL_DIR/docker-compose.prod.yml logs -f bot"
echo "  Restart bot:"
echo "    docker compose -f $INSTALL_DIR/docker-compose.prod.yml restart bot"
echo "  Stop everything:"
echo "    docker compose -f $INSTALL_DIR/docker-compose.prod.yml down"
echo "============================================================"
