# Data Directory

This directory stores the ExpiryTrack SQLite database.

## Contents

- `expirytrack.db` - Main SQLite database containing:
  - Credentials (encrypted)
  - Instruments and expiries
  - Contracts with OpenAlgo symbols
  - Historical OHLCV data
  - Job status and metadata

## Important Notes

- Database is created automatically on first run
- Do not delete or modify the database directly
- Backup this directory regularly to preserve your data
- Database uses SQLite WAL mode for better performance

## Database Schema

The database contains the following main tables:
- `credentials` - Encrypted API credentials
- `instruments` - Trading instruments (NIFTY, BANKNIFTY, etc.)
- `expiries` - Available expiry dates
- `contracts` - F&O contracts with OpenAlgo symbols
- `historical_data` - OHLCV candle data
- `job_status` - Data collection job tracking

For database queries, use the OpenAlgo symbology system documented in the main README.