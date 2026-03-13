#!/bin/bash
# ============================================================
# Docker Entrypoint - Inicia Xvfb y ejecuta el comando
# ============================================================

set -e

echo "=================================================="
echo "  BTC ETF Scraper - Docker Container"
echo "=================================================="
echo ""

# Limpiar display anterior si existe (silenciar errores si no tenemos permiso)
rm -f /tmp/.X99-lock 2>/dev/null || true

echo "🖥️  Iniciando Xvfb en display :99..."

# Iniciar Xvfb en background con configuración óptima
Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
XVFB_PID=$!

# Esperar a que Xvfb esté listo
sleep 2

# Verificar que Xvfb está corriendo
if ! kill -0 $XVFB_PID 2>/dev/null; then
    echo "❌ Error: Xvfb no pudo iniciar"
    exit 1
fi

echo "✅ Xvfb iniciado (PID: $XVFB_PID)"
echo "📍 DISPLAY=$DISPLAY"
echo ""

# Mostrar info del entorno
echo "🔧 Entorno:"
echo "   - Python: $(python --version 2>&1)"
echo "   - Chrome: $(google-chrome --version 2>&1 || echo 'No disponible')"
echo "   - ETF_SAVE_FORMAT: ${ETF_SAVE_FORMAT:-csv}"
echo "   - ETF_DRIVER_MODE: ${ETF_DRIVER_MODE:-undetected}"
echo ""

# Capture Start Time
START_TIME=$(date +%s)
bash "/app/notify_vps.sh" START "btc-etf-scraper"

echo "🚀 Ejecutando: $@"
echo "=================================================="
echo ""

# Ejecutar el comando pasado (sin exec para capturar el código de salida)
"$@"
EXIT_CODE=$?

# Capture End Time and Calculate Duration
END_TIME=$(date +%s)
DURATION_SEC=$((END_TIME - START_TIME))
DURATION_HUMAN=$(printf '%dh %dm %ds\n' $((DURATION_SEC/3600)) $((DURATION_SEC%3600/60)) $((DURATION_SEC%60)))

echo ""
echo "=================================================="
echo "🏁 Finalizado (Código: $EXIT_CODE, Duración: $DURATION_HUMAN)"
echo "=================================================="

if [ $EXIT_CODE -eq 0 ]; then
    bash "/app/notify_vps.sh" SUCCESS "btc-etf-scraper" "$DURATION_HUMAN"
else
    bash "/app/notify_vps.sh" FAILURE "btc-etf-scraper"
fi

# Cleanup
echo "🛑 Limpiando procesos..."
kill $XVFB_PID 2>/dev/null || true

exit $EXIT_CODE
