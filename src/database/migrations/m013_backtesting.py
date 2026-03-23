"""Migration m013: Add strategies and backtest_results tables."""

from typing import Any


def up(conn: Any) -> None:
    conn.execute("CREATE SEQUENCE IF NOT EXISTS strategies_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id INTEGER PRIMARY KEY DEFAULT nextval('strategies_id_seq'),
            name VARCHAR NOT NULL,
            description TEXT,
            code TEXT NOT NULL,
            is_preset BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("CREATE SEQUENCE IF NOT EXISTS backtest_results_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY DEFAULT nextval('backtest_results_id_seq'),
            strategy_id INTEGER,
            task_id VARCHAR,
            instrument_key VARCHAR NOT NULL,
            data_source VARCHAR NOT NULL DEFAULT 'candle_data',
            interval VARCHAR NOT NULL DEFAULT '1day',
            from_date VARCHAR,
            to_date VARCHAR,
            initial_capital DECIMAL(15,2) DEFAULT 100000,
            metrics JSON,
            trades JSON,
            equity_curve JSON,
            status VARCHAR NOT NULL DEFAULT 'pending',
            error_message TEXT,
            bars_processed INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def down(conn: Any) -> None:
    conn.execute("DROP TABLE IF EXISTS backtest_results")
    conn.execute("DROP SEQUENCE IF EXISTS backtest_results_id_seq")
    conn.execute("DROP TABLE IF EXISTS strategies")
    conn.execute("DROP SEQUENCE IF EXISTS strategies_id_seq")
