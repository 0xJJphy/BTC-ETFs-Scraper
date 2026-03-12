#!/bin/bash
# ============================================================
# VPS Runner Script for BTC ETF Scraper
# ============================================================

set -e

# Project directory (automatically detects path relative to script)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=================================================="
echo "🚀 Starting BTC ETF Scraper at $(date)"
echo "=================================================="

cd "$PROJECT_DIR"

# Build/update the image if needed (ensures latest code)
echo "🔨 Building/Updating Docker image..."
docker compose build

# Run the scraper
# We use --save-files to match the behavior of the GitHub Action
echo "🏃 Running scraper..."
docker compose run --rm \
  -e ETF_SAVE_FORMAT=csv \
  -e ETF_DRIVER_MODE=undetected \
  -e ETF_REQUEST_DELAY=1.5 \
  scraper python main.py --all --save-files

echo "=================================================="
echo "✅ Scraping completed at $(date)"
echo "=================================================="
