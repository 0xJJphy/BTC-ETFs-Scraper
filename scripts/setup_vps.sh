#!/bin/bash
# ============================================================
# VPS Installation Script - Anonymized
# ============================================================

set -e

# Detect current project path (always points to the parent directory of this script)
REAL_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Detect the correct user (owner of the project directory)
USER_NAME="$(stat -c '%U' "$REAL_PATH")"

echo "=================================================="
echo "🔧 Configuring BTC ETF Scraper for this VPS"
echo "Project Path: $REAL_PATH"
echo "User: $USER_NAME"
echo "=================================================="

# Create local systemd service from template or direct write
# We do this locally so the absolute path is NOT in the repo
SERVICE_FILE="/etc/systemd/system/btc-etf-scraper.service"
TIMER_FILE="/etc/systemd/system/btc-etf-scraper.timer"

echo "📝 Creating systemd service file..."
sudo bash -c "cat > $SERVICE_FILE" <<EOF
[Unit]
Description=BTC ETF Scraper Service
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
User=$USER_NAME
WorkingDirectory=$REAL_PATH
ExecStart=/bin/bash $REAL_PATH/scripts/vps_run.sh
StandardOutput=append:$REAL_PATH/scraper.log
StandardError=append:$REAL_PATH/scraper.log

[Install]
WantedBy=multi-user.target
EOF

echo "📝 Creating systemd timer file..."
sudo bash -c "cat > $TIMER_FILE" <<EOF
[Unit]
Description=Daily BTC ETF Scraper Timer
Wants=btc-etf-scraper.service

[Timer]
OnCalendar=*-*-* 06:00:00 UTC
RandomizedDelaySec=5m
Persistent=true

[Install]
WantedBy=timers.target
EOF

# Set permissions
chmod +x "$REAL_PATH/scripts/vps_run.sh"

# Reload systemd
echo "🔄 Reloading systemd daemon..."
sudo systemctl daemon-reload

echo ""
echo "✅ Configuration complete!"
echo "To start the timer, run:"
echo "sudo systemctl enable --now btc-etf-scraper.timer"
echo ""
echo "To check status:"
echo "systemctl status btc-etf-scraper.timer"
echo "=================================================="
