#!/bin/bash
# ============================================================
# VPS Runner Script for BTC ETF Scraper
# ============================================================

set -e
# set -x # Descomenta para debug profundo si falla

# Detectar ruta de docker
DOCKER_BIN=$(which docker || echo "/usr/bin/docker")

# Project directory (automatically detects path relative to script)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Detect the UID and GID of the project owner (should be jjphy)
# This ensures Docker runs as the correct user even if started by pedro
export APP_UID=$(stat -c '%u' "$PROJECT_DIR")
export APP_GID=$(stat -c '%g' "$PROJECT_DIR")

echo "=================================================="
echo "🚀 Starting BTC ETF Scraper at $(date)"
echo "ℹ️ Running container as host user ID: $APP_UID ($APP_GID)"
echo "=================================================="

# Ensure output directories exist
echo "🗂️ Preparing output directories..."
mkdir -p ./etfs_data/csv ./etfs_data/json ./etfs_data/etfs_completo

# Silent chmod - if it fails, it's usually because files are owned by root.
# The user can fix this by running scripts/setup_vps.sh (which automates the chown).
chmod -R 777 ./etfs_data 2>/dev/null || true

# Ensure logs directory exists
mkdir -p "$PROJECT_DIR/logs"
LOG_FILE="$PROJECT_DIR/logs/btc-etf-scraper_$(date +%Y%m%d).log"

# Define the runner logic in a function to easily pipe all output
run_scraper() {
    # Build/update the image if needed (ensures latest code)
    echo "🔨 Building/Updating Docker image (using $DOCKER_BIN)..."
    $DOCKER_BIN compose build

    # Run the scraper
    echo "🏃 Running scraper..."
    $DOCKER_BIN compose run --rm \
      -e ETF_SAVE_FORMAT=csv \
      -e ETF_DRIVER_MODE=undetected \
      -e ETF_REQUEST_DELAY=1.5 \
      scraper python main.py --all --save-files

    echo "=================================================="
    echo "✅ Scraping completed at $(date)"
    echo "=================================================="
}

# Execute and pipe EVERYTHING to the log file AND the console/journal
run_scraper 2>&1 | tee -a "$LOG_FILE"
