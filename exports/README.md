# Exports Directory

This directory stores exported historical data files from ExpiryTrack.

## File Formats

ExpiryTrack supports exporting data in three formats:

### CSV Files (.csv)
- Simple comma-separated format
- Columns: timestamp, open, high, low, close, volume, open_interest
- Compatible with Excel, pandas, and most analysis tools

### Excel Files (.xlsx)
- Two sheets:
  - **Historical Data**: OHLCV data with timestamps
  - **Contract Info**: Metadata about the contract
- Requires `openpyxl` package

### JSON Files (.json)
- Structured format with:
  - `contract_info`: Contract metadata
  - `historical_data`: Array of OHLCV records
- Best for programmatic processing

## File Naming Convention

Files are automatically named using the pattern:
`[OpenAlgoSymbol]_[YYYYMMDD]_[HHMMSS].[format]`

Examples:
- `NIFTY28AUG25FUT_20250918_224910.csv`
- `BANKNIFTY28AUG2547500CE_20250918_230145.xlsx`
- `NIFTY28AUG2522600PE_20250918_231022.json`

## Export Commands

```bash
# Export single symbol
python export_openalgo_data.py NIFTY28AUG25FUT

# Export to Excel
python export_openalgo_data.py NIFTY28AUG2522600CE --format excel

# Export to JSON
python export_openalgo_data.py BANKNIFTY28AUG25FUT --format json

# Search and export multiple
python export_openalgo_data.py --search NIFTY28AUG25 --auto
```

## Storage Notes

- Files in this directory can be safely deleted after backup
- Exported files are not automatically cleaned up
- Consider organizing exports into subdirectories for large datasets

## Using Exported Data

### Python/Pandas
```python
import pandas as pd
df = pd.read_csv('exports/NIFTY28AUG25FUT_20250918_224910.csv')
```

### Excel
Open .xlsx files directly in Microsoft Excel or LibreOffice Calc

### JSON Processing
```python
import json
with open('exports/NIFTY28AUG25FUT_20250918_224910.json') as f:
    data = json.load(f)
```

For more details, see the [Export Guide](../EXPORT_GUIDE.md)