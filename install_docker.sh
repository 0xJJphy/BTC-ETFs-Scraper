#!/bin/bash
# ============================================================
# Script de instalación - BTC ETF Scraper Docker
# ============================================================
# 
# Este script copia los archivos Docker al proyecto existente
# 
# Uso:
#   chmod +x install_docker.sh
#   ./install_docker.sh /ruta/a/tu/proyecto
#
# ============================================================

set -e

TARGET_DIR="${1:-.}"

echo "=================================================="
echo "  BTC ETF Scraper - Docker Installation"
echo "=================================================="
echo ""
echo "Target directory: $TARGET_DIR"
echo ""

# Verificar que el directorio existe
if [ ! -d "$TARGET_DIR" ]; then
    echo "❌ Error: Directory '$TARGET_DIR' does not exist"
    exit 1
fi

# Verificar que es el proyecto correcto
if [ ! -f "$TARGET_DIR/main.py" ]; then
    echo "⚠️  Warning: main.py not found in $TARGET_DIR"
    echo "   Make sure this is the correct project directory"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "📁 Creating directories..."
mkdir -p "$TARGET_DIR/.github/workflows"

echo "📄 Copying Docker files..."

# Copiar archivos principales
for file in Dockerfile docker-entrypoint.sh docker-compose.yml .dockerignore; do
    if [ -f "$file" ]; then
        cp "$file" "$TARGET_DIR/"
        echo "   ✓ $file"
    fi
done

# Copiar workflow de GitHub Actions
if [ -f ".github/workflows/scraper.yml" ]; then
    cp ".github/workflows/scraper.yml" "$TARGET_DIR/.github/workflows/"
    echo "   ✓ .github/workflows/scraper.yml"
fi

# Hacer ejecutable el entrypoint
chmod +x "$TARGET_DIR/docker-entrypoint.sh"

echo ""
echo "📦 Updating requirements.txt..."

# Verificar si undetected-chromedriver ya está en requirements
if ! grep -q "undetected-chromedriver" "$TARGET_DIR/requirements.txt" 2>/dev/null; then
    echo "undetected-chromedriver>=3.5.5" >> "$TARGET_DIR/requirements.txt"
    echo "   ✓ Added undetected-chromedriver to requirements.txt"
else
    echo "   ✓ undetected-chromedriver already in requirements.txt"
fi

echo ""
echo "📝 Updating helpers.py..."
echo "   ⚠️  You need to manually update core/utils/helpers.py"
echo "   The updated version is in: docker-files/core/utils/helpers.py"
echo ""

echo "=================================================="
echo "  ✅ Installation Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Update core/utils/helpers.py with the new version"
echo "   (adds undetected-chromedriver and Xvfb support)"
echo ""
echo "2. Build the Docker image:"
echo "   cd $TARGET_DIR"
echo "   docker-compose build"
echo ""
echo "3. Run the scraper:"
echo "   docker-compose run --rm scraper"
echo ""
echo "4. (Optional) Push to GitHub to enable Actions"
echo ""
