"""
Demo script to showcase OpenAlgo symbology queries
This demonstrates how users can easily query historical data using user-friendly symbols
"""

from src.database.manager import DatabaseManager
from src.utils.openalgo_symbol import option_symbol, future_symbol
from datetime import datetime

def demo_queries():
    """Demonstrate various OpenAlgo symbol queries"""
    print("\n" + "="*70)
    print("OpenAlgo Symbology Query Demo")
    print("="*70)

    # Initialize database
    db = DatabaseManager()

    print("\n1. Query Examples (will work after data collection):")
    print("-" * 50)

    # Example 1: Get specific option contract
    symbol = "NIFTY28MAR2420800CE"
    print(f"\n   Getting contract: {symbol}")
    print(f"   Query: db.get_contract_by_openalgo_symbol('{symbol}')")
    contract = db.get_contract_by_openalgo_symbol(symbol)
    if contract:
        print(f"   Found: {contract['trading_symbol']} expires {contract['expiry_date']}")
    else:
        print(f"   Not found - collect data first")

    # Example 2: Get all BANKNIFTY contracts
    print("\n   Getting all BANKNIFTY contracts:")
    print("   Query: db.get_contracts_by_base_symbol('BANKNIFTY')")
    contracts = db.get_contracts_by_base_symbol('BANKNIFTY')
    print(f"   Found {len(contracts)} BANKNIFTY contracts")

    # Example 3: Get option chain
    print("\n   Getting NIFTY option chain for March 28, 2024:")
    print("   Query: db.get_option_chain('NIFTY', '2024-03-28')")
    chain = db.get_option_chain('NIFTY', '2024-03-28')
    print(f"   Found {len(chain.get('calls', []))} calls and {len(chain.get('puts', []))} puts")

    # Example 4: Get futures
    print("\n   Getting all BANKNIFTY futures:")
    print("   Query: db.get_futures_by_symbol('BANKNIFTY')")
    futures = db.get_futures_by_symbol('BANKNIFTY')
    print(f"   Found {len(futures)} BANKNIFTY futures")

    # Example 5: Search symbols
    print("\n   Searching for March 2024 contracts:")
    print("   Query: db.search_openalgo_symbols('MAR24')")
    results = db.search_openalgo_symbols('MAR24')
    print(f"   Found {len(results)} contracts expiring in March 2024")

    print("\n2. Symbol Generation Examples:")
    print("-" * 50)

    # Generate various symbols
    examples = [
        ("NIFTY Future Mar 2024", future_symbol("NIFTY", "2024-03-28")),
        ("BANKNIFTY 47500 Call Apr 2024", option_symbol("BANKNIFTY", "2024-04-25", 47500, "CE")),
        ("NIFTY 20800 Put Mar 2024", option_symbol("NIFTY", "2024-03-28", 20800, "PE")),
        ("SENSEX Future Apr 2024", future_symbol("SENSEX", "2024-04-30")),
    ]

    for desc, symbol in examples:
        print(f"\n   {desc}:")
        print(f"   Symbol: {symbol}")

    print("\n3. SQL Query Examples:")
    print("-" * 50)

    sql_examples = [
        ("Get NIFTY 20800 Call",
         "SELECT * FROM contracts WHERE openalgo_symbol = 'NIFTY28MAR2420800CE'"),

        ("Get all Bank Nifty April options",
         "SELECT * FROM contracts WHERE openalgo_symbol LIKE 'BANKNIFTY%APR24%' "
         "AND (openalgo_symbol LIKE '%CE' OR openalgo_symbol LIKE '%PE')"),

        ("Get option chain for specific strike",
         "SELECT * FROM contracts WHERE openalgo_symbol IN "
         "('BANKNIFTY25APR2447500CE', 'BANKNIFTY25APR2447500PE')"),

        ("Get historical data with OpenAlgo symbol",
         """SELECT h.timestamp, h.open, h.high, h.low, h.close, h.volume,
         c.openalgo_symbol FROM historical_data h
         JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
         WHERE c.openalgo_symbol = 'NIFTY28MAR2420800CE'
         ORDER BY h.timestamp"""),
    ]

    for desc, query in sql_examples:
        print(f"\n   {desc}:")
        print(f"   {query}")

    print("\n" + "="*70)
    print("Benefits of OpenAlgo Symbology:")
    print("="*70)
    print("\n* User-friendly format - no complex instrument keys")
    print("* Consistent naming across all instruments")
    print("* Easy to remember and type")
    print("* Direct SQL queries without complex joins")
    print("* Compatible with trading platforms using similar formats")
    print("* Indexed for fast performance")

    print("\n" + "="*70)
    print("Note: Run data collection first to populate the database with contracts")
    print("="*70)

if __name__ == "__main__":
    demo_queries()