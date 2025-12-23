# Bitcoin ETF Scraper & Aggregator

A robust, modular Python pipeline for scraping, processing, and aggregating Bitcoin ETF data (NAV, Market Price, Shares, Flows, and Holdings) from multiple global providers.

## Project Structure

```bash
.
├── etfs_data/
│   ├── csv/               # CSV & XLSX outputs
│   └── json/              # JSON outputs
├── main.py                # Single entry point for the pipeline
└── requirements.txt       # Project dependencies
```

## Features

- **Automated Scraping**: Fetches data from major ETF providers (BlackRock, Fidelity, Grayscale, ARK, 21Shares, etc.) and CoinMarketCap.
- **Data Normalization**: Translates raw data into a consistent English format, handling various date and numeric styles.
- **Smart Aggregation**:
  - Calculates **Holdings (BTC)** using initial seeds and daily flows.
  - Estimates **Shares Outstanding** and **NAV** where data is missing using multi-weighted strategies.
  - Propagates data to **weekends and holidays** based on local market calendars (US/HK).
- **Stealth Selenium**: Configured to mimic real browser behavior with retries, jitter, and headless support.
- **Multiple Output Formats**: Generates both flat CSVs and a structured JSON for easy integration.

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

Run the entire pipeline sequentially:

```bash
python main.py
```

This will:
1. Run all individual ETF scrapers sequentially.
2. Scrape Bitcoin ETF flows from CoinMarketCap.
3. Build the final aggregated dataset.

### Standalone Scrapers
You can also run individual scrapers for testing:
```bash
python core/scrapers/scraper_fidelity.py
```

## Output

The final processed data is saved in:
- `etfs_data/bitcoin_etf_completo.csv`: Flat table containing all ETFs and calculated metrics.
- `etfs_data/bitcoin_etf_completo_estructurado.json`: Documentation-rich JSON for developers.

## Environment Variables

Customizable via environment or `.env` file:
- `ETF_REQUEST_DELAY`: Base delay between steps (default: 3.0s).
- `ETF_MAX_RETRIES`: Max retries for failed downloads (default: 5).
- `ETF_SAVE_FORMAT`: File format for raw data (csv or xlsx).

## License
MIT
