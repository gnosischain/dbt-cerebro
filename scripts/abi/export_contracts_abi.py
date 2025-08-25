#!/usr/bin/env python3
"""
Export contracts_abi table from ClickHouse to CSV seed file.

This script must be run before `dbt seed` to preserve any newly fetched ABIs.
The seed operation will overwrite the database table with the CSV contents,
so we need to export the current state first.
"""

import os
import sys
import csv
import logging
from pathlib import Path
from dotenv import load_dotenv

try:
    import clickhouse_connect
except ImportError:
    print("Error: clickhouse_connect is required. Install with: pip install clickhouse-connect")
    sys.exit(1)

# Setup
project_root = Path(__file__).resolve().parents[2]
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

# Paths
seeds_dir = project_root / 'seeds'
seeds_dir.mkdir(parents=True, exist_ok=True)
contracts_abi_csv = seeds_dir / 'contracts_abi.csv'

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('export_contracts_abi')

# ClickHouse connection
host = os.environ.get('CLICKHOUSE_URL', 'localhost')
port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
user = os.environ.get('CLICKHOUSE_USER', 'default')
password = os.environ.get('CLICKHOUSE_PASSWORD', '')
database = os.environ.get('CLICKHOUSE_DATABASE', 'dbt')
secure = os.environ.get('CLICKHOUSE_SECURE', 'True').lower() == 'true'

def export_contracts_abi():
    """Export contracts_abi table to CSV."""
    
    logger.info(f"Connecting to ClickHouse: {host}:{port} DB: {database}")
    
    try:
        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=secure
        )
        
        # Test connection
        client.ping()
        logger.info("Connected to ClickHouse successfully")
        
        # Query all ABIs
        query = """
        SELECT 
            contract_address,
            implementation_address,
            abi_json,
            contract_name,
            source,
            toString(updated_at) as updated_at
        FROM contracts_abi
        ORDER BY contract_address, implementation_address
        """
        
        logger.info("Fetching ABIs from database...")
        result = client.query(query)
        rows = result.result_rows or []
        
        logger.info(f"Found {len(rows)} ABI records")
        
        # Write to CSV with proper quoting
        logger.info(f"Writing to {contracts_abi_csv}")
        with open(contracts_abi_csv, 'w', newline='', encoding='utf-8') as f:
            # Use QUOTE_ALL to ensure all fields are quoted
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow([
                'contract_address',
                'implementation_address', 
                'abi_json',
                'contract_name',
                'source',
                'updated_at'
            ])
            
            # Write data rows
            for row in rows:
                writer.writerow(row)
        
        # Remove trailing newline from the file
        with open(contracts_abi_csv, 'rb+') as f:
            # Go to the end of file
            f.seek(0, 2)
            file_size = f.tell()
            
            # Check if file ends with newline and remove it
            if file_size > 0:
                f.seek(file_size - 1)
                last_char = f.read(1)
                if last_char in (b'\n', b'\r'):
                    # Check for Windows-style \r\n
                    if file_size > 1:
                        f.seek(file_size - 2)
                        two_chars = f.read(2)
                        if two_chars == b'\r\n':
                            f.seek(file_size - 2)
                            f.truncate()
                        else:
                            f.seek(file_size - 1)
                            f.truncate()
                    else:
                        f.seek(file_size - 1)
                        f.truncate()
        
        logger.info(f"Successfully exported {len(rows)} ABIs to {contracts_abi_csv}")
        print(f"✓ Exported {len(rows)} ABIs to seeds/contracts_abi.csv")
        print("You can now safely run: dbt seed")
        
    except Exception as e:
        logger.error(f"Failed to export ABIs: {e}")
        print(f"✗ Export failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    export_contracts_abi()