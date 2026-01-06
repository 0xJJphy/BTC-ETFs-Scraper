# Bitcoin ETF Scraper & Aggregator 🪙

A robust, modular Python pipeline for scraping, processing, and aggregating Bitcoin ETF data (NAV, Market Price, Shares, Flows, and Holdings) from multiple global providers. Supports PostgreSQL/Supabase for persistent storage.

## 🐳 Docker Setup (Recommended)

### Quick Start

```bash
# Build the Docker image
docker-compose build

# Run the complete pipeline (database only)
docker-compose run --rm scraper

# Run with CSV/JSON file backup
docker-compose run --rm scraper python main.py --all --save-files

# Run specific phases
docker-compose run --rm scraper python main.py --sites  # ETF sites only
docker-compose run --rm scraper python main.py --cmc    # CoinMarketCap only
docker-compose run --rm scraper python main.py --build  # Data builder only

# Debug mode (interactive shell)
docker-compose run --rm scraper bash
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | PostgreSQL connection string |
| `ETF_SAVE_FORMAT` | `csv` | Output format (`csv` or `xlsx`) |
| `ETF_DRIVER_MODE` | `undetected` | Driver type (`undetected` or `standard`) |
| `ETF_REQUEST_DELAY` | `3.0` | Base delay between requests (seconds) |
| `ETF_REQUEST_JITTER` | `2.0` | Random jitter added to delay |
| `ETF_MAX_RETRIES` | `5` | Max retries for failed downloads |

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--all` | Run all pipeline phases (default) |
| `--sites` | Run individual ETF site scrapers only |
| `--cmc` | Run CoinMarketCap flows scraper only |
| `--build` | Run Data Builder & Aggregator only |
| `--save-files` | Save CSV/JSON files (default: off when DB enabled) |
| `--headless` | Run browser in headless mode |
| `--no-headless` | Run browser with visible window |

---

## 🗄️ Database Schema

The scraper supports PostgreSQL/Supabase for persistent data storage.

### Entity Relationship Diagram

```mermaid
erDiagram
    providers ||--o{ etfs : "has"
    etfs ||--o{ etf_daily_data : "has"
    etfs ||--o{ etf_flows : "has"
    btc_prices ||--o| etf_daily_data : "referenced by"
    
    providers {
        int id PK
        varchar name UK
        varchar website
        varchar country
        timestamp created_at
    }
    
    etfs {
        int id PK
        varchar ticker UK
        varchar name
        int provider_id FK
        varchar market
        date launch_date
        boolean is_active
        timestamp created_at
    }
    
    etf_daily_data {
        bigint id PK
        int etf_id FK
        date date
        decimal nav
        decimal market_price
        bigint shares_outstanding
        decimal holdings_btc
        bigint volume
        decimal premium_discount
        timestamp created_at
        timestamp updated_at
    }
    
    etf_flows {
        bigint id PK
        date date
        int etf_id FK
        decimal flow_btc
        decimal flow_usd
        varchar source
        timestamp created_at
    }
    
    btc_prices {
        bigint id PK
        date date UK
        decimal price_usd
        varchar source
        timestamp created_at
    }
    
    scrape_logs {
        bigint id PK
        timestamp started_at
        timestamp finished_at
        varchar status
        int etfs_processed
        int etfs_failed
        text error_message
        int execution_time_seconds
    }
```

### Tables Overview

| Table | Description | Records |
|-------|-------------|---------|
| `providers` | ETF issuers (Grayscale, BlackRock, etc.) | 13 |
| `etfs` | Bitcoin ETF products | 14 |
| `etf_daily_data` | Daily NAV, price, holdings per ETF | Growing |
| `etf_flows` | Daily BTC flows from CoinMarketCap | Growing |
| `btc_prices` | Historical BTC prices (USD) | Growing |
| `scrape_logs` | Scraper execution history | Growing |

### Views

- `v_etf_summary` - Daily summary of all ETFs with calculated AUM
- `v_etf_latest` - Most recent data for each ETF

### Duplicate Prevention

All tables use `UPSERT` with `ON CONFLICT` to prevent duplicates:
- `etf_daily_data`: Unique constraint on `(etf_id, date)`
- `etf_flows`: Unique constraint on `(etf_id, date)`
- `btc_prices`: Unique constraint on `date`

---

## 🔄 GitHub Actions

The scraper runs automatically every day at 6:00 UTC via GitHub Actions.

### Secrets Required

| Secret | Description |
|--------|-------------|
| `DATABASE_URL` | PostgreSQL connection string (Supabase pooler recommended) |

### Manual Trigger

1. Go to **Actions** tab in your repository
2. Select **BTC ETF Scraper** workflow
3. Click **Run workflow**
4. Select the phase to run and whether to save files

### Schedule

- **Time**: 06:00 UTC daily
- **Artifacts**: Data is saved as artifacts for 30 days

---

## 📁 Project Structure

```bash
.
├── Dockerfile              # Container definition
├── docker-compose.yml      # Orchestration
├── docker-entrypoint.sh    # Xvfb startup script
├── requirements.txt        # Python dependencies
├── main.py                 # Entry point
├── core/
│   ├── multi_etf_scraper.py
│   ├── data_builder.py
│   ├── db.py               # Database connection & operations
│   ├── db_adapter.py       # Scraper-to-DB adapter
│   ├── utils/
│   │   └── helpers.py      # Shared utilities
│   └── scrapers/
│       ├── scraper_grayscale.py
│       ├── scraper_ishares.py
│       ├── scraper_invesco.py
│       ├── scraper_franklin.py
│       ├── scraper_fidelity.py
│       ├── scraper_vaneck.py
│       ├── scraper_ark.py
│       ├── scraper_coinshares.py
│       ├── scraper_bosera.py
│       ├── scraper_harvest.py
│       ├── scraper_chinaamc.py
│       ├── scraper_bitwise.py
│       ├── scraper_wisdomtree.py
│       └── scraper_cmc.py
├── db/
│   └── schema.sql          # Database schema
├── etfs_data/              # Output directory (mounted volume)
│   ├── csv/
│   ├── json/
│   └── etfs_completo/
└── .github/
    └── workflows/
        └── scraper.yml     # GitHub Actions workflow
```

---

## 🏃 Local Development

### With Database

```bash
# Create .env file
echo "DATABASE_URL=postgresql://user:pass@host:port/dbname" > .env

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run (saves to DB only)
python main.py --all

# Run with file backup
python main.py --all --save-files
```

### Without Database

```bash
# Just run without DATABASE_URL - automatically saves to CSV/JSON
python main.py --all
```

---

## 📊 Output

### Database Tables
Data is automatically saved to PostgreSQL when `DATABASE_URL` is configured.

### CSV/XLSX Files (per ETF)
When `--save-files` is enabled:
- `gbtc_dailynav.csv` - Grayscale Bitcoin Trust
- `ibit_dailynav.csv` - iShares Bitcoin Trust
- `btco_dailynav.csv` - Invesco Galaxy Bitcoin
- ... (14 ETFs total)

### Aggregated Files
- `bitcoin_etf_completo.csv` - All ETFs combined
- `bitcoin_etf_completo_estructurado.json` - Structured JSON with metadata

---

## 🔧 Troubleshooting

### Database connection fails
```bash
# Test DNS resolution
nslookup db.your-project.supabase.co

# Use Session Pooler for IPv4-only networks
# Change from Direct Connection to Session Pooler in Supabase
```

### Chrome crashes in Docker
```bash
# Increase shared memory
docker run --shm-size=2g ...
```

### Sites blocking requests
```bash
# Try standard Selenium (slower but different fingerprint)
ETF_DRIVER_MODE=standard docker-compose run --rm scraper
```

### Check logs
```bash
docker-compose logs -f scraper
```

---

## 📝 License

MIT

## 🤝 Contributing

Pull requests welcome! Please ensure all scrapers continue to work after changes.
