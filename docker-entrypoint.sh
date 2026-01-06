#!/bin/bash
# ============================================================
# Docker Entrypoint - Inicia Xvfb y ejecuta el comando
# ============================================================

set -e

echo "=================================================="
echo "  BTC ETF Scraper - Docker Container"
echo "=================================================="
echo ""

# Limpiar display anterior si existe
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

echo "🚀 Ejecutando: $@"
echo "=================================================="
echo ""

# Manejar señales para cleanup limpio
cleanup() {
    echo ""
    echo "🛑 Señal recibida, limpiando..."
    kill $XVFB_PID 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

# Ejecutar el comando pasado
exec "$@"
