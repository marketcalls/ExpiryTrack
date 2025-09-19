"""
Data Exporter for ExpiryTrack with OpenAlgo Symbol Format
"""
import os
import csv
import json
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)

class DataExporter:
    """Export collected data in various formats with OpenAlgo symbol support"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize exporter

        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager or DatabaseManager()
        self.export_dir = Path("exports")
        self.export_dir.mkdir(exist_ok=True)

    def get_openalgo_formatted_symbol(self, contract: Dict) -> str:
        """Convert contract to OpenAlgo symbol format

        Args:
            contract: Contract dictionary

        Returns:
            OpenAlgo formatted symbol (e.g., NIFTY25JAN25C24000)
        """
        try:
            symbol = contract.get('trading_symbol', '')

            # Parse the symbol
            if 'NIFTY' in symbol:
                if 'BANKNIFTY' in symbol:
                    base = 'BANKNIFTY'
                elif 'FINNIFTY' in symbol:
                    base = 'FINNIFTY'
                else:
                    base = 'NIFTY'
            else:
                base = symbol.split(' ')[0]

            # Extract expiry date
            expiry = contract.get('expiry_date', '')
            if expiry:
                expiry_date = datetime.strptime(expiry, '%Y-%m-%d')
                # Format: DDMMMYY (e.g., 25JAN25)
                expiry_str = expiry_date.strftime('%d%b%y').upper()
            else:
                expiry_str = ''

            # Extract option type and strike
            if ' CE ' in symbol or symbol.endswith(' CE'):
                option_type = 'C'
                strike = symbol.split(' CE')[0].split(' ')[-1]
            elif ' PE ' in symbol or symbol.endswith(' PE'):
                option_type = 'P'
                strike = symbol.split(' PE')[0].split(' ')[-1]
            else:
                # Futures
                option_type = 'F'
                strike = ''

            # Construct OpenAlgo symbol
            if option_type == 'F':
                openalgo_symbol = f"{base}{expiry_str}FUT"
            else:
                openalgo_symbol = f"{base}{expiry_str}{option_type}{strike}"

            return openalgo_symbol

        except Exception as e:
            logger.error(f"Error formatting OpenAlgo symbol: {e}")
            return contract.get('trading_symbol', 'UNKNOWN')

    def export_to_csv(self,
                     instruments: List[str],
                     expiries: Dict[str, List[str]],
                     options: Dict,
                     task_id: str) -> str:
        """Export data to CSV format

        Args:
            instruments: List of instrument keys
            expiries: Dictionary of instrument to expiry dates
            options: Export options
            task_id: Task ID for tracking

        Returns:
            Path to exported file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        all_data = []

        for instrument in instruments:
            instrument_expiries = expiries.get(instrument, [])
            instrument_name = instrument.split('|')[1].replace(' ', '_')

            logger.debug(f"Processing instrument: {instrument}, expiries: {instrument_expiries}")

            for expiry_date in instrument_expiries:
                # Get contracts for this expiry
                contracts = self.db_manager.get_contracts_for_expiry(instrument, expiry_date)

                for contract in contracts:
                    # Get historical data for contract
                    expired_instrument_key = contract.get('expired_instrument_key', '')
                    historical_data = self.db_manager.get_historical_data(expired_instrument_key)

                    # Apply time range filter
                    if options.get('time_range') != 'all':
                        historical_data = self._filter_by_time_range(
                            historical_data,
                            expiry_date,
                            options.get('time_range')
                        )

                    # Process each candle
                    for candle in historical_data:
                        row = {}

                        # Add OpenAlgo symbol as first column if requested
                        if options.get('include_openalgo', True):
                            row['openalgo_symbol'] = self.get_openalgo_formatted_symbol(contract)

                        # Add contract metadata
                        if options.get('include_metadata', True):
                            row['instrument'] = instrument_name
                            row['expiry'] = expiry_date
                            row['strike'] = contract.get('strike_price', '')
                            row['option_type'] = contract.get('contract_type', '')
                            row['trading_symbol'] = contract.get('trading_symbol', '')

                        # Add timestamp as separate date and time columns
                        timestamp_ms = candle[0]
                        # Handle both string and numeric timestamps
                        if isinstance(timestamp_ms, str):
                            # Parse ISO format timestamp
                            dt = datetime.fromisoformat(timestamp_ms.replace('+05:30', '+0530').replace('Z', '+0000'))
                        else:
                            # Convert from milliseconds
                            dt = datetime.fromtimestamp(timestamp_ms / 1000)
                        row['date'] = dt.strftime('%Y-%m-%d')
                        row['time'] = dt.strftime('%H:%M:%S')
                        row['timestamp'] = candle[0]

                        # Add OHLCV data
                        row['open'] = candle[1]
                        row['high'] = candle[2]
                        row['low'] = candle[3]
                        row['close'] = candle[4]
                        row['volume'] = candle[5]
                        row['oi'] = candle[6] if len(candle) > 6 else 0

                        all_data.append(row)

        # Create filename with OpenAlgo symbol format
        base_filename = f"ExpiryTrack_{instrument_name}_{timestamp}"
        if options.get('include_openalgo', True):
            base_filename = f"OpenAlgo_{base_filename}"

        filename = f"{base_filename}.csv"
        filepath = self.export_dir / filename

        # Write to CSV
        if all_data:
            df = pd.DataFrame(all_data)

            # Ensure proper column order with OpenAlgo symbol, date, time first
            if 'openalgo_symbol' in df.columns:
                preferred_order = ['openalgo_symbol', 'date', 'time', 'instrument', 'expiry', 'strike', 'option_type', 'trading_symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
                cols = [col for col in preferred_order if col in df.columns]
                remaining_cols = [col for col in df.columns if col not in cols]
                df = df[cols + remaining_cols]

            df.to_csv(filepath, index=False)
            logger.info(f"Exported {len(all_data)} rows to {filepath}")
        else:
            # Create empty file with headers
            headers = ['openalgo_symbol'] if options.get('include_openalgo') else []
            headers.extend(['date', 'time', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
            pd.DataFrame(columns=headers).to_csv(filepath, index=False)
            logger.warning(f"No data found for export. Created empty file with headers: {filepath}")

        return str(filepath)

    def export_to_json(self,
                      instruments: List[str],
                      expiries: Dict[str, List[str]],
                      options: Dict,
                      task_id: str) -> str:
        """Export data to JSON format

        Args:
            instruments: List of instrument keys
            expiries: Dictionary of instrument to expiry dates
            options: Export options
            task_id: Task ID for tracking

        Returns:
            Path to exported file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        export_data = {
            'metadata': {
                'exported_at': datetime.now().isoformat(),
                'instruments': instruments,
                'format': 'OpenAlgo' if options.get('include_openalgo') else 'Standard',
                'version': '1.0'
            },
            'data': {}
        }

        for instrument in instruments:
            instrument_expiries = expiries.get(instrument, [])
            instrument_name = instrument.split('|')[1].replace(' ', '_')

            export_data['data'][instrument_name] = {}

            for expiry_date in instrument_expiries:
                contracts = self.db_manager.get_contracts_for_expiry(instrument, expiry_date)

                export_data['data'][instrument_name][expiry_date] = []

                for contract in contracts:
                    contract_data = {
                        'openalgo_symbol': self.get_openalgo_formatted_symbol(contract),
                        'trading_symbol': contract.get('trading_symbol', ''),
                        'strike': contract.get('strike_price', ''),
                        'option_type': contract.get('contract_type', ''),
                        'historical_data': []
                    }

                    # Get historical data
                    expired_instrument_key = contract.get('expired_instrument_key', '')
                    historical_data = self.db_manager.get_historical_data(expired_instrument_key)

                    # Apply time range filter
                    if options.get('time_range') != 'all':
                        historical_data = self._filter_by_time_range(
                            historical_data,
                            expiry_date,
                            options.get('time_range')
                        )

                    # Format candles
                    for candle in historical_data:
                        timestamp_ms = candle[0]
                        # Handle both string and numeric timestamps
                        if isinstance(timestamp_ms, str):
                            # Parse ISO format timestamp
                            dt = datetime.fromisoformat(timestamp_ms.replace('+05:30', '+0530').replace('Z', '+0000'))
                        else:
                            # Convert from milliseconds
                            dt = datetime.fromtimestamp(timestamp_ms / 1000)
                        contract_data['historical_data'].append({
                            'date': dt.strftime('%Y-%m-%d'),
                            'time': dt.strftime('%H:%M:%S'),
                            'timestamp': candle[0],
                            'open': candle[1],
                            'high': candle[2],
                            'low': candle[3],
                            'close': candle[4],
                            'volume': candle[5],
                            'oi': candle[6] if len(candle) > 6 else 0
                        })

                    export_data['data'][instrument_name][expiry_date].append(contract_data)

        # Create filename
        base_filename = f"ExpiryTrack_{timestamp}"
        if options.get('include_openalgo', True):
            base_filename = f"OpenAlgo_{base_filename}"

        filename = f"{base_filename}.json"
        filepath = self.export_dir / filename

        # Write JSON
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)

        logger.info(f"Exported data to {filepath}")
        return str(filepath)

    def export_to_zip(self,
                     instruments: List[str],
                     expiries: Dict[str, List[str]],
                     options: Dict,
                     task_id: str) -> str:
        """Export data to ZIP archive with separate files

        Args:
            instruments: List of instrument keys
            expiries: Dictionary of instrument to expiry dates
            options: Export options
            task_id: Task ID for tracking

        Returns:
            Path to exported file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create ZIP filename
        base_filename = f"ExpiryTrack_{timestamp}"
        if options.get('include_openalgo', True):
            base_filename = f"OpenAlgo_{base_filename}"

        zip_filename = f"{base_filename}.zip"
        zip_filepath = self.export_dir / zip_filename

        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for instrument in instruments:
                instrument_expiries = expiries.get(instrument, [])
                instrument_name = instrument.split('|')[1].replace(' ', '_')

                for expiry_date in instrument_expiries:
                    contracts = self.db_manager.get_contracts_for_expiry(instrument, expiry_date)

                    # Group by option type if requested
                    if options.get('separate_files', False):
                        ce_data = []
                        pe_data = []
                        fut_data = []

                        for contract in contracts:
                            contract_type = contract.get('contract_type', '')
                            data = self._prepare_contract_data(contract, expiry_date, options)

                            if 'CE' in contract_type:
                                ce_data.extend(data)
                            elif 'PE' in contract_type:
                                pe_data.extend(data)
                            else:
                                fut_data.extend(data)

                        # Write separate files
                        if ce_data:
                            ce_filename = f"{instrument_name}_{expiry_date}_CE.csv"
                            self._write_csv_to_zip(zipf, ce_filename, ce_data, options)

                        if pe_data:
                            pe_filename = f"{instrument_name}_{expiry_date}_PE.csv"
                            self._write_csv_to_zip(zipf, pe_filename, pe_data, options)

                        if fut_data:
                            fut_filename = f"{instrument_name}_{expiry_date}_FUT.csv"
                            self._write_csv_to_zip(zipf, fut_filename, fut_data, options)
                    else:
                        # Single file per expiry
                        all_data = []
                        for contract in contracts:
                            data = self._prepare_contract_data(contract, expiry_date, options)
                            all_data.extend(data)

                        if all_data:
                            filename = f"{instrument_name}_{expiry_date}.csv"
                            self._write_csv_to_zip(zipf, filename, all_data, options)

        logger.info(f"Created ZIP archive: {zip_filepath}")
        return str(zip_filepath)

    def _prepare_contract_data(self, contract: Dict, expiry_date: str, options: Dict) -> List[Dict]:
        """Prepare contract data for export"""
        data = []
        expired_instrument_key = contract.get('expired_instrument_key', '')
        historical_data = self.db_manager.get_historical_data(expired_instrument_key)

        # Apply time range filter
        if options.get('time_range') != 'all':
            historical_data = self._filter_by_time_range(
                historical_data,
                expiry_date,
                options.get('time_range')
            )

        for candle in historical_data:
            row = {}

            if options.get('include_openalgo', True):
                row['openalgo_symbol'] = self.get_openalgo_formatted_symbol(contract)

            if options.get('include_metadata', True):
                row['strike'] = contract.get('strike_price', '')
                row['option_type'] = contract.get('contract_type', '')
                row['trading_symbol'] = contract.get('trading_symbol', '')

            # Add timestamp as separate date and time columns
            timestamp_ms = candle[0]
            # Handle both string and numeric timestamps
            if isinstance(timestamp_ms, str):
                # Parse ISO format timestamp
                dt = datetime.fromisoformat(timestamp_ms.replace('+05:30', '+0530').replace('Z', '+0000'))
            else:
                # Convert from milliseconds
                dt = datetime.fromtimestamp(timestamp_ms / 1000)
            row['date'] = dt.strftime('%Y-%m-%d')
            row['time'] = dt.strftime('%H:%M:%S')
            row['timestamp'] = candle[0]
            row['open'] = candle[1]
            row['high'] = candle[2]
            row['low'] = candle[3]
            row['close'] = candle[4]
            row['volume'] = candle[5]
            row['oi'] = candle[6] if len(candle) > 6 else 0

            data.append(row)

        return data

    def _write_csv_to_zip(self, zipf: zipfile.ZipFile, filename: str, data: List[Dict], options: Dict):
        """Write CSV data to ZIP file"""
        if not data:
            return

        df = pd.DataFrame(data)

        # Ensure proper column order with OpenAlgo symbol, date, time first
        if 'openalgo_symbol' in df.columns:
            preferred_order = ['openalgo_symbol', 'date', 'time', 'strike', 'option_type', 'trading_symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
            cols = [col for col in preferred_order if col in df.columns]
            remaining_cols = [col for col in df.columns if col not in cols]
            df = df[cols + remaining_cols]

        csv_content = df.to_csv(index=False)
        zipf.writestr(filename, csv_content)

    def _filter_by_time_range(self, data: List, expiry_date: str, time_range: str) -> List:
        """Filter historical data by time range"""
        if not data or time_range == 'all':
            return data

        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')

        # Calculate cutoff date
        if time_range == '1d':
            cutoff_dt = expiry_dt - timedelta(days=1)
        elif time_range == '7d':
            cutoff_dt = expiry_dt - timedelta(days=7)
        elif time_range == '30d':
            cutoff_dt = expiry_dt - timedelta(days=30)
        elif time_range == '90d':
            cutoff_dt = expiry_dt - timedelta(days=90)
        else:
            return data

        cutoff_timestamp = int(cutoff_dt.timestamp() * 1000)

        # Filter data
        filtered_data = [
            candle for candle in data
            if candle[0] >= cutoff_timestamp
        ]

        return filtered_data

    def get_available_expiries(self, instruments: List[str]) -> Dict[str, List[str]]:
        """Get available expiries for given instruments

        Args:
            instruments: List of instrument keys

        Returns:
            Dictionary of instrument to expiry dates
        """
        result = {}

        for instrument in instruments:
            expiries = self.db_manager.get_expiries_for_instrument(instrument)
            result[instrument] = sorted(expiries, reverse=True)  # Most recent first

        return result