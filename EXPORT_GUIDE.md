# ExpiryTrack Data Export Guide

## Overview
ExpiryTrack provides a powerful export tool that uses OpenAlgo symbology to easily export historical data for expired F&O contracts.

## OpenAlgo Symbol Format

### Futures
Format: `[BaseSymbol][DDMMMYY]FUT`
Example: `NIFTY28AUG25FUT`, `BANKNIFTY30SEP25FUT`

### Options
Format: `[BaseSymbol][DDMMMYY][Strike][CE/PE]`
Example: `NIFTY28AUG2522600CE`, `BANKNIFTY28AUG2547500PE`

## Export Script Usage

### Basic Export

```bash
# Export single symbol to CSV
python export_openalgo_data.py NIFTY28AUG25FUT

# Export to Excel format
python export_openalgo_data.py NIFTY28AUG2522600CE --format excel

# Export to JSON format
python export_openalgo_data.py BANKNIFTY28AUG2547500PE --format json

# Specify output directory
python export_openalgo_data.py NIFTY28AUG25FUT --output my_exports
```

### Search and Export

```bash
# Search for all NIFTY August 2025 contracts
python export_openalgo_data.py --search NIFTY28AUG25

# Search for all BANKNIFTY 47500 strikes
python export_openalgo_data.py --search BANKNIFTY28AUG2547500

# Export all matching with auto-confirmation
python export_openalgo_data.py --search NIFTY28AUG25 --auto --format excel
```

### Export Formats

1. **CSV** (default)
   - Simple, compatible with all tools
   - One file with timestamp, OHLC, volume, open_interest

2. **Excel**
   - Two sheets: Historical Data and Contract Info
   - Formatted for easy analysis
   - Requires `openpyxl` package

3. **JSON**
   - Structured format with contract info and data
   - Good for programmatic processing
   - ISO formatted timestamps

## Examples

### Export Specific Contract
```bash
# Export NIFTY 22600 Call option
python export_openalgo_data.py NIFTY28AUG2522600CE

# Export BANKNIFTY Future
python export_openalgo_data.py BANKNIFTY28AUG25FUT --format excel
```

### Batch Export
```bash
# Export all NIFTY options for a specific expiry
python export_openalgo_data.py --search NIFTY28AUG25 --auto

# Export all 22600 strikes (both CE and PE)
python export_openalgo_data.py --search 22600 --auto
```

### Python Integration
```python
from export_openalgo_data import export_by_openalgo_symbol

# Export single symbol
filename = export_by_openalgo_symbol('NIFTY28AUG25FUT', 'csv', 'my_exports')

# Export multiple symbols
symbols = ['NIFTY28AUG2522600CE', 'NIFTY28AUG2522600PE']
for symbol in symbols:
    export_by_openalgo_symbol(symbol, 'excel')
```

## Query Database Directly

You can also query the database directly using OpenAlgo symbols:

```python
from src.database.manager import DatabaseManager

db = DatabaseManager()

# Get contract by OpenAlgo symbol
contract = db.get_contract_by_openalgo_symbol('NIFTY28AUG2522600CE')

# Get all BANKNIFTY contracts
contracts = db.get_contracts_by_base_symbol('BANKNIFTY')

# Get option chain
chain = db.get_option_chain('NIFTY', '2025-08-28')

# Search symbols
results = db.search_openalgo_symbols('AUG25')
```

## SQL Query Examples

```sql
-- Get specific contract data
SELECT h.*, c.openalgo_symbol
FROM historical_data h
JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
WHERE c.openalgo_symbol = 'NIFTY28AUG25FUT'
ORDER BY h.timestamp;

-- Get all BANKNIFTY August options
SELECT * FROM contracts
WHERE openalgo_symbol LIKE 'BANKNIFTY%AUG25%'
AND (openalgo_symbol LIKE '%CE' OR openalgo_symbol LIKE '%PE');

-- Get futures expiring in September
SELECT * FROM contracts
WHERE openalgo_symbol LIKE '%SEP25FUT';
```

## Output Files

Files are saved in the `exports` directory by default with the naming format:
`[OpenAlgoSymbol]_[YYYYMMDD]_[HHMMSS].[format]`

Example: `NIFTY28AUG25FUT_20250918_224128.csv`

## Tips

1. **Use search patterns** to find symbols when you're not sure of exact format
2. **Use --auto flag** for batch exports in scripts
3. **Excel format** includes contract metadata in a separate sheet
4. **JSON format** is best for further programmatic processing
5. **CSV format** is most compatible with analysis tools

## Troubleshooting

### No data found
- Ensure data collection has been run for the contract
- Check the symbol format is correct
- Use search to find available symbols

### Export errors
- Ensure `openpyxl` is installed for Excel export: `pip install openpyxl`
- Check disk space for large exports
- Verify write permissions in output directory

### Finding symbols
```bash
# List all available symbols
python -c "from src.database.manager import DatabaseManager; db = DatabaseManager(); symbols = db.search_openalgo_symbols(''); print('\n'.join(symbols[:20]))"
```

## Support

For more information, see the main [README](README.md) or check the [GitHub repository](https://github.com/marketcalls/ExpiryTrack).