# TAO-DETECTOR

A cryptocurrency new listing detection system that monitors Binance and OKX exchanges for new coin listings and stores the first 7 days of 1-minute OHLCV data.

## Overview

TAO-DETECTOR continuously monitors Binance and OKX exchanges to detect new coin listings as early as possible. When a new coin is detected, the system automatically downloads and stores the first 7 days of 1-minute OHLCV (Open, High, Low, Close, Volume) data from the genesis candle. This data is valuable for analyzing early trading patterns and price movements of newly listed cryptocurrencies.

## Features

- **Real-time Detection**: Monitors Binance and OKX for new coin listings
- **Early Detection**: Identifies new listings as soon as they appear
- **Data Collection**: Automatically downloads first 7 days of 1-minute OHLCV data
- **Data Storage**: Organizes data in CSV format for easy analysis
- **Multi-Exchange Support**: Covers both Binance and OKX exchanges
- **Historical Data**: Maintains database of all previous new listings

## Project Structure

```
TAO-DETECTOR/
├── README.md
├── requirements.txt
├── Binance Futures - CSV/     # Stored 1-minute OHLCV data for Binance new listings
├── Binance Futures - Scripts/ # Binance detection and data download scripts
├── OKX - CSV/                 # Stored 1-minute OHLCV data for OKX new listings
└── OKX Futures - Scripts /    # OKX detection and data download scripts
```

## Quick Start

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Binance Detector**:
   ```bash
   python "Binance Futures - Scripts/Master Binance Futures Code.py"
   ```

3. **Run OKX Detector**:
   ```bash
   python "OKX Futures - Scripts /okx_new_coins_downloader.py"
   ```

## Key Scripts

### Binance Detection
- `Master Binance Futures Code.py`: Main Binance new listing detector
- `merge_first_day.py`: Merge first day data from multiple coins

### OKX Detection
- `okx_new_coins_downloader.py`: Main OKX new listing detector
- `okx_7days_downloader.py`: Download 7 days of data for detected coins
- `async_candle_downloader_okx.py`: Async data downloader for OKX

## Data Format

Each CSV file contains 1-minute OHLCV data with the following columns:
- `open_time`: Timestamp of the candle
- `open`, `high`, `low`, `close`: OHLC prices
- `volume`: Trading volume
- `close_time`: End timestamp
- `quote_volume`: Volume in quote currency
- `trades`: Number of trades
- `taker_base_vol`, `taker_quote_vol`: Taker volumes

## Configuration

The system can be configured for:
- Detection frequency
- Data storage location
- Exchange API credentials
- Notification settings

## Data Usage

The collected data can be used for:
- **Pattern Analysis**: Study early trading patterns of new coins
- **Price Movement Analysis**: Analyze initial price movements
- **Volume Analysis**: Study trading volume patterns
- **Market Research**: Understand market behavior for new listings
- **Trading Strategy Development**: Develop strategies based on historical new listing data

## Monitoring

The system provides:
- Real-time detection logs
- Data download progress tracking
- Error handling and retry mechanisms
- Storage management for large datasets

## Risk Disclaimer

This software is for educational and research purposes only. The data collected is historical and should not be used as financial advice. Cryptocurrency trading involves substantial risk of loss.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For questions or issues, please open a GitHub issue or contact the maintainers. 