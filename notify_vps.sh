#!/bin/bash
# notify_vps.sh - Sends Telegram notifications for different events
# Usage: ./notify_vps.sh <START|SUCCESS|FAILURE> <SERVICE_NAME> [DURATION]

EVENT_TYPE=${1:-"FAILURE"}
UNIT=${2:-"btc-etf-scraper"}
DURATION=${3:-""}

# Load environment variables
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if [ -f "$DIR/.env" ]; then
    # Filter to avoid issues with specialized chars in env
    export $(grep -E "^(TELEGRAM|HEALTHCHECKS)" "$DIR/.env" | xargs)
fi

# Exit if no Telegram config
if [ -z "$TELEGRAM_BOT_TOKEN" ] || [ -z "$TELEGRAM_CHAT_ID" ]; then
    exit 0
fi

HOST=$(hostname)
TIME=$(date "+%Y-%m-%d %H:%M:%S")

# Read summary if exists
SUMMARY=""
if [ -f "$DIR/etfs_data/last_run_summary.txt" ]; then
    SUMMARY=$(cat "$DIR/etfs_data/last_run_summary.txt")
    # Clean up for next run
    rm "$DIR/etfs_data/last_run_summary.txt"
fi

case $EVENT_TYPE in
    START)
        MESSAGE="🚀 *BTC-ETF-Scraper ha empezado a ejecutarse*
*Host:* $HOST
*Hora:* $TIME"
        ;;
    SUCCESS)
        MESSAGE="✅ *BTC-ETF-Scraper ha terminado correctamente*
*Resumen:* ${SUMMARY:-"Sin detalles"}
*Duración:* $DURATION
*Host:* $HOST
*Hora:* $TIME"
        ;;
    FAILURE)
        CUSTOM_LOGS=$4
        # Check if we are in Docker (no journalctl)
        if [ ! -z "$CUSTOM_LOGS" ]; then
            LOGS=$CUSTOM_LOGS
        elif command -v journalctl >/dev/null 2>&1; then
            LOGS=$(journalctl -u "$UNIT" -n 15 --no-pager | sed 's/`//g')
        else
            LOGS="Logs no disponibles (entorno Docker/Container)"
        fi

        MESSAGE="⚠️ *BTC-ETF-Scraper ha FALLADO* ⚠️
*Resumen:* ${SUMMARY:-"Error crítico antes del scraping"}
*Unit:* $UNIT
*Host:* $HOST
*Hora:* $TIME

*Logs:*
\`\`\`
$LOGS
\`\`\`"
        ;;
    *)
        MESSAGE="ℹ️ *Notificación de BTC-ETF-Scraper*
*Evento:* $EVENT_TYPE
*Host:* $HOST"
        ;;
esac

curl -s -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/sendMessage" \
    -d chat_id="$TELEGRAM_CHAT_ID" \
    -d text="$MESSAGE" \
    -d parse_mode="Markdown" > /dev/null
