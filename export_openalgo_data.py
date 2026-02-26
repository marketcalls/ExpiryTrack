"""
Export historical data using OpenAlgo symbols
Supports exporting to CSV, Excel, and JSON formats
"""

import duckdb
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import argparse

from src.config import config

def export_by_openalgo_symbol(symbol, format='csv', output_dir='exports'):
    """
    Export historical data for a specific OpenAlgo symbol

    Args:
        symbol: OpenAlgo symbol (e.g., 'NIFTY28AUG2522600CE', 'BANKNIFTY28AUG25FUT')
        format: Export format ('csv', 'excel', 'json')
        output_dir: Directory to save exported files
    """
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(exist_ok=True)

    # Connect to database
    conn = duckdb.connect(str(config.DB_PATH), read_only=True)

    try:
        # Query to get contract details and historical data
        query = """
        SELECT
            c.openalgo_symbol,
            c.trading_symbol,
            c.strike_price,
            c.contract_type,
            c.expiry_date,
            h.timestamp,
            h.open,
            h.high,
            h.low,
            h.close,
            h.volume,
            h.oi
        FROM contracts c
        JOIN historical_data h ON c.expired_instrument_key = h.expired_instrument_key
        WHERE c.openalgo_symbol = ?
        ORDER BY h.timestamp
        """

        # Load data into DataFrame
        df = conn.execute(query, [symbol]).fetchdf()

        if df.empty:
            print(f"No data found for symbol: {symbol}")
            return None

        # Format timestamp and remove timezone if present
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)

        # Get contract info
        contract_info = {
            'openalgo_symbol': df['openalgo_symbol'].iloc[0],
            'trading_symbol': df['trading_symbol'].iloc[0],
            'strike_price': df['strike_price'].iloc[0] if pd.notna(df['strike_price'].iloc[0]) else None,
            'contract_type': df['contract_type'].iloc[0],
            'expiry_date': str(df['expiry_date'].iloc[0]),
            'data_points': len(df)
        }

        print(f"\nExporting data for: {symbol}")
        print(f"Trading Symbol: {contract_info['trading_symbol']}")
        print(f"Contract Type: {contract_info['contract_type']}")
        print(f"Expiry Date: {contract_info['expiry_date']}")
        if contract_info['strike_price']:
            print(f"Strike Price: {contract_info['strike_price']}")
        print(f"Total Data Points: {contract_info['data_points']}")

        # Prepare data for export (remove duplicate contract info columns)
        export_df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']].copy()

        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"{symbol}_{timestamp}"

        # Export based on format
        if format.lower() == 'csv':
            filename = f"{output_dir}/{base_filename}.csv"
            export_df.to_csv(filename, index=False)
            print(f"\nExported to: {filename}")

        elif format.lower() == 'excel':
            filename = f"{output_dir}/{base_filename}.xlsx"
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Write data
                export_df.to_excel(writer, sheet_name='Historical Data', index=False)

                # Write contract info to separate sheet
                info_df = pd.DataFrame([contract_info])
                info_df.to_excel(writer, sheet_name='Contract Info', index=False)

            print(f"\nExported to: {filename}")

        elif format.lower() == 'json':
            filename = f"{output_dir}/{base_filename}.json"
            export_data = {
                'contract_info': contract_info,
                'historical_data': export_df.to_dict('records')
            }

            # Convert timestamp to string for JSON serialization
            for record in export_data['historical_data']:
                record['timestamp'] = record['timestamp'].isoformat()

            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)

            print(f"\nExported to: {filename}")

        else:
            print(f"Unsupported format: {format}")
            return None

        return filename

    finally:
        conn.close()

def export_multiple_symbols(symbols, format='csv', output_dir='exports'):
    """Export data for multiple OpenAlgo symbols"""
    exported_files = []

    for symbol in symbols:
        print("\n" + "="*60)
        filename = export_by_openalgo_symbol(symbol, format, output_dir)
        if filename:
            exported_files.append(filename)

    print("\n" + "="*60)
    print(f"Export Summary: {len(exported_files)} files exported")
    return exported_files

def search_and_export(pattern, format='csv', output_dir='exports', auto_confirm=False):
    """Search for symbols matching pattern and export them"""
    conn = duckdb.connect(str(config.DB_PATH), read_only=True)

    try:
        # Search for matching symbols
        result = conn.execute("""
            SELECT DISTINCT openalgo_symbol
            FROM contracts
            WHERE openalgo_symbol LIKE ?
            ORDER BY openalgo_symbol
        """, [f"%{pattern}%"]).fetchall()

        symbols = [row[0] for row in result]
    finally:
        conn.close()

    if not symbols:
        print(f"No symbols found matching pattern: {pattern}")
        return []

    print(f"Found {len(symbols)} symbols matching '{pattern}':")
    for symbol in symbols[:10]:  # Show first 10
        print(f"  - {symbol}")
    if len(symbols) > 10:
        print(f"  ... and {len(symbols) - 10} more")

    # Ask for confirmation if many symbols (skip in auto mode)
    if len(symbols) > 5 and not auto_confirm:
        try:
            response = input(f"\nExport all {len(symbols)} symbols? (y/n): ")
            if response.lower() != 'y':
                print("Export cancelled")
                return []
        except EOFError:
            print("\nNote: Use --auto flag to skip confirmation in non-interactive mode")
            print("Export cancelled")
            return []

    return export_multiple_symbols(symbols, format, output_dir)

def main():
    """Main function with examples"""
    print("\n" + "="*70)
    print("OpenAlgo Symbol Data Export Tool")
    print("="*70)

    # Example 1: Export single future
    print("\n1. Exporting NIFTY Future (if available):")
    export_by_openalgo_symbol('NIFTY28AUG25FUT', 'csv')

    # Example 2: Export single option
    print("\n2. Exporting NIFTY Option:")
    export_by_openalgo_symbol('NIFTY28AUG2522600CE', 'excel')

    # Example 3: Export multiple symbols
    print("\n3. Exporting multiple symbols:")
    symbols = ['NIFTY28AUG2522600CE', 'NIFTY28AUG2522600PE', 'NIFTY28AUG2522650CE']
    export_multiple_symbols(symbols, 'csv')

    # Example 4: Search and export
    print("\n4. Search and export pattern:")
    print("\nSearching for all NIFTY 22600 strikes:")
    search_and_export('NIFTY28AUG2522600', 'csv')

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Export historical data using OpenAlgo symbols')
    parser.add_argument('symbol', nargs='?', help='OpenAlgo symbol to export')
    parser.add_argument('--format', '-f', default='csv', choices=['csv', 'excel', 'json'],
                        help='Export format (default: csv)')
    parser.add_argument('--output', '-o', default='exports', help='Output directory (default: exports)')
    parser.add_argument('--search', '-s', help='Search pattern for symbols')
    parser.add_argument('--demo', action='store_true', help='Run demo examples')
    parser.add_argument('--auto', action='store_true', help='Auto-confirm batch exports')

    args = parser.parse_args()

    if args.demo:
        main()
    elif args.search:
        search_and_export(args.search, args.format, args.output, args.auto)
    elif args.symbol:
        export_by_openalgo_symbol(args.symbol, args.format, args.output)
    else:
        # Interactive mode
        print("\n" + "="*70)
        print("OpenAlgo Symbol Data Export Tool")
        print("="*70)
        print("\nUsage examples:")
        print("  python export_openalgo_data.py NIFTY28AUG2522600CE")
        print("  python export_openalgo_data.py BANKNIFTY28AUG25FUT --format excel")
        print("  python export_openalgo_data.py --search NIFTY28AUG25")
        print("  python export_openalgo_data.py --demo")

        symbol = input("\nEnter OpenAlgo symbol to export (or 'demo' for examples): ")
        if symbol.lower() == 'demo':
            main()
        else:
            format_choice = input("Export format (csv/excel/json) [csv]: ") or 'csv'
            export_by_openalgo_symbol(symbol, format_choice)
