"""
Example usage of Wind Data API Pipeline.

This script demonstrates how to use the Wind data pipeline to download
market data from Wind Terminal.

Requirements:
- Wind Terminal must be running
- WindPy package must be installed (comes with Wind Terminal)

Usage:
    python -m data_api.wind.example
"""

from datetime import datetime, date
from data_api.wind import WindDownloader, WindPipeline, WindConfig, WIND_SYMBOLS


def example_single_symbol():
    """Download data for a single symbol."""
    print("=" * 60)
    print("Example 1: Download single symbol")
    print("=" * 60)
    
    # Create configuration
    config = WindConfig(
        data_path='data/wind',
        cache_enabled=True,
        retry_count=3
    )
    
    # Create downloader and pipeline
    downloader = WindDownloader(config.to_dict())
    pipeline = WindPipeline(downloader, config.to_dict())
    
    # Download gold futures data
    symbol = 'AU.SHF'  # Gold futures on SHFE
    start_date = '2024-01-01'
    end_date = '2024-12-31'
    
    result = pipeline.run_single(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        save_to_disk=True,
        file_format='csv'
    )
    
    if result['success']:
        print(f"Successfully downloaded {symbol}")
        print(f"Data shape: {result['data'].shape}")
        print(result['data'].head())
    else:
        print(f"Failed to download {symbol}: {result['error']}")


def example_batch_symbols():
    """Download data for multiple symbols."""
    print("\n" + "=" * 60)
    print("Example 2: Download batch of symbols")
    print("=" * 60)
    
    config = WindConfig(data_path='data/wind')
    downloader = WindDownloader(config.to_dict())
    pipeline = WindPipeline(downloader, config.to_dict())
    
    # Download all index futures
    symbols = list(WIND_SYMBOLS['index_futures'].keys())
    print(f"Downloading: {symbols}")
    
    result = pipeline.run_batch(
        symbols=symbols,
        start_date='2024-01-01',
        end_date='2024-12-31',
        save_to_disk=True
    )
    
    print(f"Success: {len(result['success'])} symbols")
    print(f"Failed: {len(result['failed'])} symbols")
    if result['failed']:
        print(f"Failed symbols: {result['failed']}")


def example_futures_contracts():
    """Download all contracts for a futures underlying."""
    print("\n" + "=" * 60)
    print("Example 3: Download futures contracts")
    print("=" * 60)
    
    config = WindConfig(data_path='data/wind')
    downloader = WindDownloader(config.to_dict())
    pipeline = WindPipeline(downloader, config.to_dict())
    
    # Download all copper futures contracts
    result = pipeline.run_futures(
        underlying='CU',
        start_date='2024-01-01',
        end_date='2024-12-31',
        save_to_disk=True
    )
    
    print(f"Downloaded {len(result['success'])} contracts for {result['underlying']}")
    print(f"Contracts: {result['success'][:5]}..." if len(result['success']) > 5 else result['success'])


def example_update_existing():
    """Update existing data file."""
    print("\n" + "=" * 60)
    print("Example 4: Update existing data")
    print("=" * 60)
    
    config = WindConfig(data_path='data/wind')
    downloader = WindDownloader(config.to_dict())
    pipeline = WindPipeline(downloader, config.to_dict())
    
    # Update existing data file
    result = pipeline.update_existing(
        symbol='AU_SHF',  # Note: use filename format
        end_date=datetime.now().strftime('%Y-%m-%d')
    )
    
    if result['success']:
        print(f"Updated {result['symbol']}, added {result['records_added']} records")
    else:
        print(f"Update failed: {result['error']}")


def example_convenience_function():
    """Using the convenience function for quick downloads."""
    print("\n" + "=" * 60)
    print("Example 5: Quick download using convenience function")
    print("=" * 60)
    
    from data_api.wind.pipeline import download_wind_data
    
    result = download_wind_data(
        symbols=['SH000001', 'SH000300'],  # SSE Index, CSI 300
        start_date='2024-01-01',
        end_date='2024-12-31',
        data_path='data/wind',
        save_to_disk=True
    )
    
    print(f"Success: {result['success']}")
    print(f"Failed: {result['failed']}")


def show_available_symbols():
    """Display available symbols in the configuration."""
    print("\n" + "=" * 60)
    print("Available Symbol Categories")
    print("=" * 60)
    
    for category, symbols in WIND_SYMBOLS.items():
        print(f"\n{category.upper()}:")
        for code, name in list(symbols.items())[:5]:
            print(f"  {code}: {name}")
        if len(symbols) > 5:
            print(f"  ... and {len(symbols) - 5} more")


if __name__ == '__main__':
    print("Wind Data API Pipeline Examples")
    print("=" * 60)
    print("\nNOTE: Wind Terminal must be running for these examples to work.")
    print("If WindPy is not installed, the examples will fail.\n")
    
    # Show available symbols
    show_available_symbols()
    
    # Uncomment the examples you want to run:
    
    # example_single_symbol()
    # example_batch_symbols()
    # example_futures_contracts()
    # example_update_existing()
    # example_convenience_function()
    
    print("\n" + "=" * 60)
    print("To run examples, uncomment them in the script.")
    print("Make sure Wind Terminal is running and WindPy is installed.")
    print("=" * 60)