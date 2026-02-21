"""
Data Exporter for ExpiryTrack with OpenAlgo Symbol Format
"""
import os
import csv
import json
import zipfile
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

from ..database.manager import DatabaseManager
from ..config import config

logger = logging.getLogger(__name__)

class DataExporter:
    """Export collected data in various formats with OpenAlgo symbol support"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db_manager = db_manager or DatabaseManager()
        self.export_dir = Path("exports")
        self.export_dir.mkdir(exist_ok=True)

    # ── Single JOIN helper (#13) ──────────────────────────────
    def _fetch_all_data(self, instruments: List[str], expiries: Dict[str, List[str]]) -> pd.DataFrame:
        """Fetch all data in a single JOIN query instead of N+1 per-contract queries."""
        pairs = []
        for instrument in instruments:
            for exp in expiries.get(instrument, []):
                pairs.append((instrument, exp))

        if not pairs:
            return pd.DataFrame()

        conn = duckdb.connect(str(config.DB_PATH))
        try:
            where_parts = []
            params = []
            for inst, exp in pairs:
                where_parts.append("(c.instrument_key = ? AND c.expiry_date = ?)")
                params.append(inst)
                params.append(exp)

            where_clause = " OR ".join(where_parts)

            df = conn.execute(f"""
                SELECT
                    c.openalgo_symbol,
                    c.instrument_key,
                    c.expiry_date,
                    c.strike_price,
                    c.contract_type,
                    c.trading_symbol,
                    h.timestamp,
                    h.open,
                    h.high,
                    h.low,
                    h.close,
                    h.volume,
                    h.oi
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE {where_clause}
                ORDER BY c.instrument_key, c.expiry_date, c.strike_price, h.timestamp
            """, params).fetchdf()

            return df
        finally:
            conn.close()

    def _add_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add date and time columns derived from timestamp."""
        if df.empty:
            return df
        ts = pd.to_datetime(df['timestamp'])
        df = df.copy()
        df['date'] = ts.dt.strftime('%Y-%m-%d')
        df['time'] = ts.dt.strftime('%H:%M:%S')
        return df

    def _apply_time_range_df(self, df: pd.DataFrame, time_range: str) -> pd.DataFrame:
        """Apply time range filter on DataFrame."""
        if df.empty or time_range == 'all' or not time_range:
            return df

        days_map = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}
        days = days_map.get(time_range)
        if not days:
            return df

        ts = pd.to_datetime(df['timestamp'])
        # Per-row: filter based on each row's expiry_date
        expiry_dt = pd.to_datetime(df['expiry_date'])
        cutoff = expiry_dt - pd.Timedelta(days=days)
        mask = ts >= cutoff
        return df[mask]

    def _build_filename(self, instruments: List[str], suffix: str, options: Dict) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if instruments:
            instrument_names = [inst.split('|')[1].replace(' ', '_') for inst in instruments]
            instrument_label = '_'.join(instrument_names[:3])
            if len(instruments) > 3:
                instrument_label += f'_and_{len(instruments) - 3}_more'
        else:
            instrument_label = 'NoInstruments'
        base = f"ExpiryTrack_{instrument_label}_{timestamp}"
        if options.get('include_openalgo', True):
            base = f"OpenAlgo_{base}"
        return f"{base}.{suffix}"

    def _format_df_for_csv(self, df: pd.DataFrame, options: Dict) -> pd.DataFrame:
        """Format DataFrame columns for CSV/ZIP output."""
        if df.empty:
            return df

        out = pd.DataFrame()

        if options.get('include_openalgo', True) and 'openalgo_symbol' in df.columns:
            out['openalgo_symbol'] = df['openalgo_symbol']

        out['date'] = df['date']
        out['time'] = df['time']

        if options.get('include_metadata', True):
            out['instrument'] = df['instrument_key'].apply(lambda x: x.split('|')[1].replace(' ', '_') if '|' in str(x) else x)
            out['expiry'] = df['expiry_date'].astype(str)
            out['strike'] = df['strike_price']
            out['option_type'] = df['contract_type']
            out['trading_symbol'] = df['trading_symbol']

        out['timestamp'] = df['timestamp']
        out['open'] = df['open']
        out['high'] = df['high']
        out['low'] = df['low']
        out['close'] = df['close']
        out['volume'] = df['volume']
        out['oi'] = df['oi'].fillna(0).astype(int)

        return out

    def get_openalgo_formatted_symbol(self, contract: Dict) -> str:
        """Convert contract to OpenAlgo symbol format"""
        try:
            symbol = contract.get('trading_symbol', '')
            if 'NIFTY' in symbol:
                if 'BANKNIFTY' in symbol:
                    base = 'BANKNIFTY'
                elif 'FINNIFTY' in symbol:
                    base = 'FINNIFTY'
                else:
                    base = 'NIFTY'
            else:
                base = symbol.split(' ')[0]

            expiry = contract.get('expiry_date', '')
            if expiry:
                expiry_date = datetime.strptime(expiry, '%Y-%m-%d')
                expiry_str = expiry_date.strftime('%d%b%y').upper()
            else:
                expiry_str = ''

            if ' CE ' in symbol or symbol.endswith(' CE'):
                option_type = 'C'
                strike = symbol.split(' CE')[0].split(' ')[-1]
            elif ' PE ' in symbol or symbol.endswith(' PE'):
                option_type = 'P'
                strike = symbol.split(' PE')[0].split(' ')[-1]
            else:
                option_type = 'F'
                strike = ''

            if option_type == 'F':
                return f"{base}{expiry_str}FUT"
            else:
                return f"{base}{expiry_str}{option_type}{strike}"
        except Exception as e:
            logger.error(f"Error formatting OpenAlgo symbol: {e}")
            return contract.get('trading_symbol', 'UNKNOWN')

    # ── CSV Export (rewritten with single query #13) ──────────
    def export_to_csv(self, instruments: List[str], expiries: Dict[str, List[str]],
                      options: Dict, task_id: str) -> str:
        df = self._fetch_all_data(instruments, expiries)

        # Apply time range filter
        if options.get('time_range') != 'all':
            df = self._apply_time_range_df(df, options.get('time_range'))

        df = self._add_datetime_columns(df)
        df = self._format_df_for_csv(df, options)

        filename = self._build_filename(instruments, 'csv', options)
        filepath = self.export_dir / filename

        if df.empty:
            headers = ['openalgo_symbol'] if options.get('include_openalgo') else []
            headers.extend(['date', 'time', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
            pd.DataFrame(columns=headers).to_csv(filepath, index=False)
            logger.warning(f"No data found for export. Created empty file: {filepath}")
        else:
            df.to_csv(filepath, index=False)
            logger.info(f"Exported {len(df)} rows to {filepath}")

        return str(filepath)

    # ── JSON Export (rewritten with single query #13) ─────────
    def export_to_json(self, instruments: List[str], expiries: Dict[str, List[str]],
                       options: Dict, task_id: str) -> str:
        df = self._fetch_all_data(instruments, expiries)

        if options.get('time_range') != 'all':
            df = self._apply_time_range_df(df, options.get('time_range'))

        df = self._add_datetime_columns(df)

        export_data = {
            'metadata': {
                'exported_at': datetime.now().isoformat(),
                'instruments': instruments,
                'format': 'OpenAlgo' if options.get('include_openalgo') else 'Standard',
                'version': '1.0'
            },
            'data': {}
        }

        if not df.empty:
            for instrument in instruments:
                instrument_name = instrument.split('|')[1].replace(' ', '_')
                export_data['data'][instrument_name] = {}

                inst_df = df[df['instrument_key'] == instrument]
                for expiry_date in sorted(inst_df['expiry_date'].astype(str).unique()):
                    exp_df = inst_df[inst_df['expiry_date'].astype(str) == expiry_date]
                    contracts_list = []

                    for (ts, ct, sp), group in exp_df.groupby(['trading_symbol', 'contract_type', 'strike_price']):
                        contract_data = {
                            'openalgo_symbol': group['openalgo_symbol'].iloc[0] if 'openalgo_symbol' in group.columns else '',
                            'trading_symbol': ts,
                            'strike': sp,
                            'option_type': ct,
                            'historical_data': []
                        }
                        for _, row in group.iterrows():
                            contract_data['historical_data'].append({
                                'date': row['date'],
                                'time': row['time'],
                                'timestamp': str(row['timestamp']),
                                'open': float(row['open']),
                                'high': float(row['high']),
                                'low': float(row['low']),
                                'close': float(row['close']),
                                'volume': int(row['volume']),
                                'oi': int(row['oi']) if pd.notna(row['oi']) else 0
                            })
                        contracts_list.append(contract_data)

                    export_data['data'][instrument_name][expiry_date] = contracts_list

        filename = self._build_filename(instruments, 'json', options)
        filepath = self.export_dir / filename

        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)

        logger.info(f"Exported data to {filepath}")
        return str(filepath)

    # ── ZIP Export (rewritten with single query #13) ──────────
    def export_to_zip(self, instruments: List[str], expiries: Dict[str, List[str]],
                      options: Dict, task_id: str) -> str:
        df = self._fetch_all_data(instruments, expiries)

        if options.get('time_range') != 'all':
            df = self._apply_time_range_df(df, options.get('time_range'))

        df = self._add_datetime_columns(df)

        filename = self._build_filename(instruments, 'zip', options)
        zip_filepath = self.export_dir / filename

        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            if df.empty:
                zipf.writestr('empty.txt', 'No data found for the selected parameters.')
            else:
                for instrument in instruments:
                    instrument_name = instrument.split('|')[1].replace(' ', '_')
                    inst_df = df[df['instrument_key'] == instrument]

                    for expiry_date in sorted(inst_df['expiry_date'].astype(str).unique()):
                        exp_df = inst_df[inst_df['expiry_date'].astype(str) == expiry_date]

                        if options.get('separate_files', False):
                            for ctype, label in [('CE', 'CE'), ('PE', 'PE'), ('FUT', 'FUT')]:
                                if ctype == 'FUT':
                                    type_df = exp_df[~exp_df['contract_type'].isin(['CE', 'PE'])]
                                else:
                                    type_df = exp_df[exp_df['contract_type'] == ctype]
                                if not type_df.empty:
                                    formatted = self._format_df_for_csv(type_df, options)
                                    csv_name = f"{instrument_name}_{expiry_date}_{label}.csv"
                                    zipf.writestr(csv_name, formatted.to_csv(index=False))
                        else:
                            if not exp_df.empty:
                                formatted = self._format_df_for_csv(exp_df, options)
                                csv_name = f"{instrument_name}_{expiry_date}.csv"
                                zipf.writestr(csv_name, formatted.to_csv(index=False))

        logger.info(f"Created ZIP archive: {zip_filepath}")
        return str(zip_filepath)

    # ── Parquet Export (already optimized — unchanged) ────────
    def export_to_parquet(self, instruments: List[str], expiries: Dict[str, List[str]],
                          options: Dict, task_id: str) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if instruments:
            instrument_names = [inst.split('|')[1].replace(' ', '_') for inst in instruments]
            instrument_label = '_'.join(instrument_names[:3])
            if len(instruments) > 3:
                instrument_label += f'_and_{len(instruments) - 3}_more'
        else:
            instrument_label = 'NoInstruments'
        base_filename = f"ExpiryTrack_{instrument_label}_{timestamp}"
        if options.get('include_openalgo', True):
            base_filename = f"OpenAlgo_{base_filename}"

        filename = f"{base_filename}.parquet"
        filepath = self.export_dir / filename

        pairs = []
        for instrument in instruments:
            for exp in expiries.get(instrument, []):
                pairs.append((instrument, exp))

        if not pairs:
            empty_df = pd.DataFrame(columns=[
                'openalgo_symbol', 'instrument', 'expiry', 'strike', 'option_type',
                'trading_symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'
            ])
            empty_df.to_parquet(str(filepath), index=False)
            logger.warning(f"No data found for export. Created empty Parquet: {filepath}")
            return str(filepath)

        conn = duckdb.connect(str(config.DB_PATH))
        try:
            where_parts = []
            params = []
            for inst, exp in pairs:
                where_parts.append("(c.instrument_key = ? AND c.expiry_date = ?)")
                params.append(inst)
                params.append(exp)

            where_clause = " OR ".join(where_parts)

            query = f"""
                SELECT
                    c.openalgo_symbol,
                    c.instrument_key AS instrument,
                    c.expiry_date AS expiry,
                    c.strike_price AS strike,
                    c.contract_type AS option_type,
                    c.trading_symbol,
                    h.timestamp,
                    h.open,
                    h.high,
                    h.low,
                    h.close,
                    h.volume,
                    h.oi
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE {where_clause}
                ORDER BY c.instrument_key, c.expiry_date, c.strike_price, h.timestamp
            """

            safe_filepath = str(filepath).replace("'", "''")
            conn.execute(f"""
                COPY ({query}) TO '{safe_filepath}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """, params)

            row_count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{safe_filepath}')"
            ).fetchone()[0]
            logger.info(f"Exported {row_count:,} rows to Parquet: {filepath}")
        finally:
            conn.close()

        return str(filepath)

    def get_available_expiries(self, instruments: List[str]) -> Dict[str, List[str]]:
        result = {}
        for instrument in instruments:
            expiries = self.db_manager.get_expiries_for_instrument(instrument)
            result[instrument] = sorted(expiries, reverse=True)
        return result
