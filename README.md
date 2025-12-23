# Bitcoin ETF Scraper & Aggregator

A robust, modular Python pipeline for scraping, processing, and aggregating Bitcoin ETF data (NAV, Market Price, Shares, Flows, and Holdings) from multiple global providers.

## Project Structure

```bash
.
├── etfs_data/
│   ├── csv/               # Individual ETF CSV/XLSX raw data
│   ├── json/              # Individual ETF JSON raw data
│   └── etfs_completo/     # Final aggregated & processed files
├── main.py                # Main entry point with modular flags
└── requirements.txt       # Project dependencies
```

## Features

- **Automated Scraping**: Fetches data from major ETF providers (BlackRock, Fidelity, Grayscale, ARK, 21Shares, etc.) and CoinMarketCap.
- **Data Normalization**: Translates raw data into a consistent English format, handling various date and numeric styles.
- **Modular Pipeline**: Execute the full pipeline or specific phases (Individual sites, CMC, or Data Builder).
- **Smart Aggregation**:
  - Calculates **Holdings (BTC)** using initial seeds and daily flows.
  - Estimates **Shares Outstanding** and **NAV** where data is missing using multi-weighted strategies.
  - Propagates data to **weekends and holidays** based on local market calendars (US/HK).
- **Multiple Output Formats**: Generates both flat CSVs and structured JSON for easy integration.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/0xJJphy/BTC-ETFs-Scrapper.git
   cd BTC-ETFs-Scrapper
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Chromedriver**: Ensure you have Chrome installed. The scraper uses `webdriver-manager` to handle driver installation automatically.

## Usage

The `main.py` entry point supports several modular execution flags:

```bash
# Run the entire pipeline (Scrapers -> CMC -> Building)
python main.py --all

# Run only the individual ETF site scrapers
python main.py --sites

# Run only the CoinMarketCap flows scraper
python main.py --cmc

# Run only the Data Builder & Aggregator (uses existing raw data)
python main.py --build

# Run in an interactive window (not headless)
python main.py --no-headless
```

## Output

The final processed data is saved in `etfs_data/etfs_completo/`:
- `bitcoin_etf_completo.csv`: Flat table containing all ETFs and calculated metrics.
- `bitcoin_etf_completo_estructurado.json`: Documentation-rich JSON for developers.

## Environment Variables

Customizable via environment or `.env` file:
- `ETF_REQUEST_DELAY`: Base delay between steps (default: 3.0s).
- `ETF_MAX_RETRIES`: Max retries for failed downloads (default: 5).
- `ETF_SAVE_FORMAT`: File format for raw data (csv or xlsx).

## License
MIT
