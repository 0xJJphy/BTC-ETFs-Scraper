# ============================================================
# BTC ETF Scraper - Docker Image
# Chrome con Xvfb para evitar detección de headless
# ============================================================

FROM python:3.11-slim

# Evitar prompts interactivos
ENV DEBIAN_FRONTEND=noninteractive

# Variables de entorno para display virtual
ENV DISPLAY=:99
ENV DBUS_SESSION_BUS_ADDRESS=/dev/null

# Variables de la aplicación
ENV ETF_SAVE_FORMAT=csv
ENV ETF_DRIVER_MODE=undetected
ENV ETF_REQUEST_DELAY=3.0
ENV ETF_REQUEST_JITTER=2.0
ENV PYTHONUNBUFFERED=1

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Xvfb para display virtual
    xvfb \
    # Dependencias de Chrome
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    fonts-noto-color-emoji \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    # Utilidades
    curl \
    unzip \
    procps \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Instalar Google Chrome estable
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/* \
    && google-chrome --version

# Crear usuario no-root (mejor práctica de seguridad)
RUN useradd -m -s /bin/bash scraper \
    && mkdir -p /app/etfs_data/csv /app/etfs_data/json /app/etfs_data/etfs_completo \
    && chown -R scraper:scraper /app

WORKDIR /app

# Copiar requirements primero (mejor cache de Docker)
COPY --chown=scraper:scraper requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY --chown=scraper:scraper . .

# Script de entrada que inicia Xvfb
COPY --chown=scraper:scraper docker-entrypoint.sh /docker-entrypoint.sh
RUN dos2unix /docker-entrypoint.sh && chmod +x /docker-entrypoint.sh

USER scraper

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -x Xvfb || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["python", "main.py", "--all"]
