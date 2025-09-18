"""
OpenAlgo Symbology Generator
Converts Upstox instrument keys to OpenAlgo format for user-friendly symbols
"""

from datetime import datetime
from typing import Optional
import re

class OpenAlgoSymbolGenerator:
    """Generate OpenAlgo format symbols for futures and options"""

    # Mapping of Upstox symbols to OpenAlgo base symbols
    SYMBOL_MAPPING = {
        # NSE Index
        'NIFTY 50': 'NIFTY',
        'NIFTY BANK': 'BANKNIFTY',
        'NIFTY FINANCIAL SERVICES': 'FINNIFTY',
        'NIFTY NEXT 50': 'NIFTYNXT50',
        'NIFTY MIDCAP SELECT': 'MIDCPNIFTY',
        'INDIA VIX': 'INDIAVIX',

        # BSE Index
        'SENSEX': 'SENSEX',
        'BANKEX': 'BANKEX',
        'SENSEX50': 'SENSEX50',

        # Common replacements
        'Nifty 50': 'NIFTY',
        'Nifty Bank': 'BANKNIFTY',
        'Bank Nifty': 'BANKNIFTY',
    }

    @staticmethod
    def format_expiry_date(expiry_date: str) -> str:
        """
        Convert expiry date to OpenAlgo format

        Args:
            expiry_date: Date in YYYY-MM-DD format

        Returns:
            Date in DDMMMYY format (e.g., 28MAR24)
        """
        try:
            if isinstance(expiry_date, str):
                dt = datetime.strptime(expiry_date, '%Y-%m-%d')
            else:
                dt = expiry_date

            # Format: DDMMMYY (e.g., 28MAR24)
            return dt.strftime('%d%b%y').upper()
        except Exception as e:
            print(f"Error formatting date {expiry_date}: {e}")
            return expiry_date

    @staticmethod
    def extract_base_symbol(trading_symbol: str) -> str:
        """
        Extract base symbol from Upstox trading symbol

        Args:
            trading_symbol: Full trading symbol from Upstox

        Returns:
            Base symbol for OpenAlgo format
        """
        # Remove common suffixes and extract base
        # Pattern to remove date and strike info
        pattern = r'(\d{2}[A-Z]{3}\d{2,4}|\d{5,}CE|\d{5,}PE|FUT$)'
        base = re.sub(pattern, '', trading_symbol).strip()

        # Apply mapping if exists
        for upstox_symbol, openalgo_symbol in OpenAlgoSymbolGenerator.SYMBOL_MAPPING.items():
            if upstox_symbol.upper() in base.upper() or base.upper() in upstox_symbol.upper():
                return openalgo_symbol

        # If no mapping found, clean up the symbol
        base = base.replace(' ', '').replace('-', '').replace('_', '')
        return base.upper()

    @staticmethod
    def generate_future_symbol(
        trading_symbol: str,
        expiry_date: str,
        instrument_key: str = None
    ) -> str:
        """
        Generate OpenAlgo future symbol

        Format: [Base Symbol][Expiration Date]FUT
        Example: BANKNIFTY28MAR24FUT

        Args:
            trading_symbol: Trading symbol from Upstox
            expiry_date: Expiry date in YYYY-MM-DD format
            instrument_key: Optional instrument key for additional info

        Returns:
            OpenAlgo formatted future symbol
        """
        base_symbol = OpenAlgoSymbolGenerator.extract_base_symbol(trading_symbol)
        formatted_date = OpenAlgoSymbolGenerator.format_expiry_date(expiry_date)

        return f"{base_symbol}{formatted_date}FUT"

    @staticmethod
    def generate_option_symbol(
        trading_symbol: str,
        expiry_date: str,
        strike_price: float,
        option_type: str,
        instrument_key: str = None
    ) -> str:
        """
        Generate OpenAlgo option symbol

        Format: [Base Symbol][Expiration Date][Strike Price][Option Type]
        Example: NIFTY28MAR2420800CE

        Args:
            trading_symbol: Trading symbol from Upstox
            expiry_date: Expiry date in YYYY-MM-DD format
            strike_price: Strike price of the option
            option_type: CE for Call, PE for Put
            instrument_key: Optional instrument key for additional info

        Returns:
            OpenAlgo formatted option symbol
        """
        base_symbol = OpenAlgoSymbolGenerator.extract_base_symbol(trading_symbol)
        formatted_date = OpenAlgoSymbolGenerator.format_expiry_date(expiry_date)

        # Format strike price (remove decimal if whole number)
        if strike_price == int(strike_price):
            strike_str = str(int(strike_price))
        else:
            strike_str = str(strike_price)

        # Ensure option type is uppercase
        option_type = option_type.upper()
        if option_type not in ['CE', 'PE']:
            # Try to determine from trading symbol
            if 'CE' in trading_symbol.upper():
                option_type = 'CE'
            elif 'PE' in trading_symbol.upper():
                option_type = 'PE'
            else:
                option_type = 'XX'  # Unknown

        return f"{base_symbol}{formatted_date}{strike_str}{option_type}"

    @staticmethod
    def generate_symbol(contract: dict) -> str:
        """
        Generate OpenAlgo symbol from contract dictionary

        Args:
            contract: Contract dictionary with fields:
                - underlying_symbol or trading_symbol
                - expiry or expiry_date
                - strike_price (for options)
                - instrument_type or contract_type
                - instrument_key (optional)

        Returns:
            OpenAlgo formatted symbol
        """
        # Use underlying_symbol if available, otherwise extract from trading_symbol
        base_symbol = contract.get('underlying_symbol')
        if not base_symbol:
            trading_symbol = contract.get('trading_symbol', '')
            base_symbol = OpenAlgoSymbolGenerator.extract_base_symbol(trading_symbol)

        # Map to OpenAlgo base symbol
        if base_symbol.upper() in OpenAlgoSymbolGenerator.SYMBOL_MAPPING:
            base_symbol = OpenAlgoSymbolGenerator.SYMBOL_MAPPING[base_symbol.upper()]
        elif 'NIFTY' in base_symbol.upper() and '50' not in base_symbol:
            base_symbol = base_symbol.upper()
        elif 'NIFTY 50' in base_symbol.upper() or base_symbol.upper() == 'NIFTY':
            base_symbol = 'NIFTY'

        expiry = contract.get('expiry') or contract.get('expiry_date', '')
        instrument_type = contract.get('instrument_type') or contract.get('contract_type', '')

        # Determine if it's a future or option
        if instrument_type.upper() in ['FUT', 'FUTURE', 'FUTURES']:
            return OpenAlgoSymbolGenerator.generate_future_symbol(
                base_symbol,
                expiry,
                contract.get('instrument_key')
            )
        elif instrument_type.upper() in ['CE', 'PE', 'CALL', 'PUT', 'OPT', 'OPTION', 'OPTIONS']:
            strike_price = contract.get('strike_price', 0)

            # Use instrument_type directly for CE/PE
            if instrument_type.upper() in ['CE', 'PE']:
                option_type = instrument_type.upper()
            elif instrument_type.upper() in ['CALL']:
                option_type = 'CE'
            elif instrument_type.upper() in ['PUT']:
                option_type = 'PE'
            else:
                option_type = 'XX'

            return OpenAlgoSymbolGenerator.generate_option_symbol(
                base_symbol,
                expiry,
                strike_price,
                option_type,
                contract.get('instrument_key')
            )
        else:
            # Default to future if unknown
            return OpenAlgoSymbolGenerator.generate_future_symbol(
                base_symbol,
                expiry,
                contract.get('instrument_key')
            )

    @staticmethod
    def parse_openalgo_symbol(openalgo_symbol: str) -> dict:
        """
        Parse OpenAlgo symbol back to components

        Args:
            openalgo_symbol: Symbol in OpenAlgo format

        Returns:
            Dictionary with parsed components
        """
        result = {
            'base_symbol': '',
            'expiry_date': '',
            'strike_price': None,
            'instrument_type': '',
            'original_symbol': openalgo_symbol
        }

        try:
            # Check if it's a future
            if openalgo_symbol.endswith('FUT'):
                result['instrument_type'] = 'FUT'
                # Remove FUT suffix
                symbol_part = openalgo_symbol[:-3]

                # Try to find the date pattern DDMMMYY
                date_match = re.search(r'(\d{1,2}[A-Z]{3}\d{2})', symbol_part)
                if date_match:
                    date_part = date_match.group(1)
                    base_part = symbol_part[:date_match.start()]
                    result['base_symbol'] = base_part
                    result['expiry_date'] = date_part
                else:
                    # Fallback to last 7 chars
                    date_part = symbol_part[-7:]
                    base_part = symbol_part[:-7]
                    result['base_symbol'] = base_part
                    result['expiry_date'] = date_part

            # Check if it's an option (ends with CE or PE)
            elif openalgo_symbol.endswith('CE') or openalgo_symbol.endswith('PE'):
                result['instrument_type'] = openalgo_symbol[-2:]
                # Remove option type
                symbol_part = openalgo_symbol[:-2]

                # Extract strike price (digits at the end)
                match = re.search(r'(\d+\.?\d*)$', symbol_part)
                if match:
                    result['strike_price'] = float(match.group(1))
                    symbol_part = symbol_part[:match.start()]

                # Try to find the date pattern DDMMMYY (7 or 8 chars: 1MAR24 or 28MAR24)
                date_match = re.search(r'(\d{1,2}[A-Z]{3}\d{2})', symbol_part)
                if date_match:
                    date_part = date_match.group(1)
                    base_part = symbol_part[:date_match.start()]
                    result['base_symbol'] = base_part
                    result['expiry_date'] = date_part
                else:
                    # If no match, assume it's malformed
                    result['base_symbol'] = symbol_part
                    result['expiry_date'] = ''

        except Exception as e:
            print(f"Error parsing OpenAlgo symbol {openalgo_symbol}: {e}")

        return result


# Utility functions for easy access
def to_openalgo_symbol(contract: dict) -> str:
    """Convert contract to OpenAlgo symbol"""
    return OpenAlgoSymbolGenerator.generate_symbol(contract)

def future_symbol(symbol: str, expiry: str) -> str:
    """Generate future symbol"""
    return OpenAlgoSymbolGenerator.generate_future_symbol(symbol, expiry)

def option_symbol(symbol: str, expiry: str, strike: float, opt_type: str) -> str:
    """Generate option symbol"""
    return OpenAlgoSymbolGenerator.generate_option_symbol(symbol, expiry, strike, opt_type)

def parse_symbol(openalgo_symbol: str) -> dict:
    """Parse OpenAlgo symbol"""
    return OpenAlgoSymbolGenerator.parse_openalgo_symbol(openalgo_symbol)