#!/usr/bin/env python3
import os
import sys
import csv
import json
import logging
from pathlib import Path
from typing import List, Tuple, Optional

from dotenv import load_dotenv
from web3 import Web3

# Optional ClickHouse import (kept for convenience if you want to read from DB)
try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except Exception:
    CLICKHOUSE_AVAILABLE = False

# --------------------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------------------

# Find the project root directory (where .env is located)
project_root = Path(__file__).resolve().parents[2]
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

# Paths
seeds_dir = project_root / 'seeds'
seeds_dir.mkdir(parents=True, exist_ok=True)
contracts_abi_seed_path = seeds_dir / 'contracts_abi.csv'
event_csv_path = seeds_dir / 'event_signatures.csv'
function_csv_path = seeds_dir / 'function_signatures.csv'

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('signature_generator')

# Web3 (for Keccak hashing)
w3 = Web3()

# ClickHouse env (only used if we can and want to read from DB)
host = os.environ.get('CLICKHOUSE_URL', 'localhost')
port = os.environ.get('CLICKHOUSE_PORT', '8123')
user = os.environ.get('CLICKHOUSE_USER', 'default')
password = os.environ.get('CLICKHOUSE_PASSWORD', '')
database = os.environ.get('CLICKHOUSE_DATABASE', 'dbt')

# Allow an env toggle to force CSV read even if CH is available
force_csv = os.environ.get('SIGNATURE_GEN_SOURCE', '').lower() == 'csv'

# --------------------------------------------------------------------------------------
# Data loading
# --------------------------------------------------------------------------------------

def try_fetch_from_clickhouse() -> Optional[List[Tuple[str, str, str, str]]]:
    """
    Returns list of tuples:
      (contract_address, implementation_address, abi_json, contract_name)
    or None if cannot fetch.
    """
    if not CLICKHOUSE_AVAILABLE or force_csv:
        return None
    logger.info(f"Connecting to ClickHouse: {host}:{port} DB: {database} User: {user}")
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=int(port),
            username=user,
            password=password,
            database=database,
            secure=True  # set to False if self-hosted without TLS
        )
        client.ping()
        logger.info("ClickHouse connection successful.")

        query = """
        SELECT
            contract_address,
            implementation_address,
            abi_json,
            contract_name
        FROM contracts_abi
        WHERE abi_json IS NOT NULL AND abi_json != '[]' AND abi_json != '{}'
        """
        logger.info("Fetching ABIs from contracts_abi table...")
        result = client.query(query)
        rows = result.result_rows or []
        logger.info(f"Fetched {len(rows)} ABI rows from ClickHouse.")
        return rows
    except Exception as e:
        logger.warning(f"Falling back to CSV. Could not fetch from ClickHouse: {e}")
        return None


def read_contracts_abi_from_csv(path: Path) -> List[Tuple[str, str, str, str]]:
    """
    Read contracts_abi.csv with columns:
      contract_address, implementation_address, abi_json, contract_name

    Returns list of tuples matching DB shape.
    """
    if not path.exists():
        logger.error(f"contracts_abi seed not found at {path}")
        return []

    rows: List[Tuple[str, str, str, str]] = []
    logger.info(f"Reading ABIs from {path} ...")
    with path.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        required = ['contract_address', 'implementation_address', 'abi_json', 'contract_name']
        for req in required:
            if req not in reader.fieldnames:
                logger.error(f"Missing required column '{req}' in {path}")
                return []

        for r in reader:
            rows.append((
                r.get('contract_address', '') or '',
                r.get('implementation_address', '') or '',
                r.get('abi_json', '') or '',
                r.get('contract_name', '') or '',
            ))
    logger.info(f"Loaded {len(rows)} ABI rows from CSV.")
    return rows

# --------------------------------------------------------------------------------------
# Processing
# --------------------------------------------------------------------------------------

def generate_signatures(
    abi_rows: List[Tuple[str, str, str, str]]
) -> Tuple[List[dict], List[dict]]:
    """
    Given ABI rows, produce two lists of dicts:
      - event_signatures
      - function_signatures

    Event dict fields:
      contract_address, implementation_address, contract_name, event_name,
      signature, anonymous, params, indexed_params, non_indexed_params

    Function dict fields:
      contract_address, implementation_address, contract_name, function_name,
      signature, state_mutability, input_params, output_params
    """
    event_signatures: List[dict] = []
    function_signatures: List[dict] = []

    logger.info("Processing ABIs to generate signatures...")
    for (contract_address, implementation_address, abi_json, contract_name) in abi_rows:
        try:
            if not abi_json:
                continue
            abi = json.loads(abi_json)
            if not isinstance(abi, list):
                # handle single-object ABI files
                logger.debug(f"ABI for {contract_address} is not a list; skipping.")
                continue

            for item in abi:
                typ = item.get('type')
                if typ == 'event':
                    event_name = item.get('name')
                    inputs = item.get('inputs', []) or []
                    anonymous = bool(item.get('anonymous', False))

                    if not event_name:
                        continue

                    # e.g., Transfer(address,address,uint256)
                    types = [inp.get('type', 'unknown') for inp in inputs]
                    signature_str = f"{event_name}({','.join(types)})"
                    # 32-byte Keccak for event topics (strip "0x", lowercase)
                    signature_hash = w3.keccak(text=signature_str).hex()[2:]

                    params = []
                    indexed_params = []
                    non_indexed_params = []
                    for i, inp in enumerate(inputs):
                        p = {
                            'name': inp.get('name', f'param{i}'),
                            'type': inp.get('type', 'unknown'),
                            'position': i + 1,
                            'indexed': bool(inp.get('indexed', False)),
                        }
                        params.append(p)
                        (indexed_params if p['indexed'] else non_indexed_params).append(p)

                    event_signatures.append({
                        'contract_address': contract_address or '',
                        'implementation_address': implementation_address or '',
                        'contract_name': contract_name or '',
                        'event_name': event_name,
                        'signature': signature_hash,
                        'anonymous': 1 if anonymous else 0,
                        'params': json.dumps(params, ensure_ascii=False),
                        'indexed_params': json.dumps(indexed_params, ensure_ascii=False),
                        'non_indexed_params': json.dumps(non_indexed_params, ensure_ascii=False),
                    })

                elif typ == 'function':
                    function_name = item.get('name')
                    if not function_name:
                        continue

                    inputs = item.get('inputs', []) or []
                    outputs = item.get('outputs', []) or []
                    state_mutability = item.get('stateMutability', 'nonpayable')

                    in_types = [inp.get('type', 'unknown') for inp in inputs]
                    signature_str = f"{function_name}({','.join(in_types)})"
                    # 4-byte selector (first 8 hex chars after 0x)
                    selector = w3.keccak(text=signature_str).hex()[2:10]

                    input_params = []
                    for i, inp in enumerate(inputs):
                        input_params.append({
                            'name': inp.get('name', f'param{i}'),
                            'type': inp.get('type', 'unknown'),
                            'position': i + 1,
                        })

                    output_params = []
                    for i, outp in enumerate(outputs):
                        output_params.append({
                            'name': outp.get('name', f'return{i}'),
                            'type': outp.get('type', 'unknown'),
                            'position': i + 1,
                        })

                    function_signatures.append({
                        'contract_address': contract_address or '',
                        'implementation_address': implementation_address or '',
                        'contract_name': contract_name or '',
                        'function_name': function_name,
                        'signature': selector,
                        'state_mutability': state_mutability,
                        'input_params': json.dumps(input_params, ensure_ascii=False),
                        'output_params': json.dumps(output_params, ensure_ascii=False),
                    })

        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode ABI JSON for {contract_address}. Skipping row. Error: {e}")
        except Exception as e:
            logger.error(f"Error processing ABI for {contract_address}. Skipping row. Error: {e}", exc_info=True)

    logger.info(f"Generated {len(event_signatures)} event signatures and {len(function_signatures)} function signatures.")
    return event_signatures, function_signatures

# --------------------------------------------------------------------------------------
# CSV writing
# --------------------------------------------------------------------------------------

def write_csv(path: Path, rows: List[dict], headers: List[str]) -> None:
    """
    Writes rows to CSV with UTF-8 encoding and newline='' for clean CSVs.
    """
    if not rows:
        logger.info(f"No rows to write for {path.name}. Skipping file creation.")
        return

    # Ensure seeds directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing {len(rows)} rows to {path} ...")
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    logger.info(f"Wrote CSV: {path}")

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

def main():
    # 1) Try ClickHouse; if unavailable or fails, use seeds/contracts_abi.csv
    abi_rows = try_fetch_from_clickhouse()
    if abi_rows is None:
        abi_rows = read_contracts_abi_from_csv(contracts_abi_seed_path)

    if not abi_rows:
        logger.error("No ABI rows found from either ClickHouse or CSV. Nothing to do.")
        sys.exit(1)

    # 2) Generate signatures
    event_signatures, function_signatures = generate_signatures(abi_rows)

    # 3) Write seeds
    event_headers = [
        'contract_address',
        'implementation_address',
        'contract_name',
        'event_name',
        'signature',
        'anonymous',
        'params',
        'indexed_params',
        'non_indexed_params'
    ]
    function_headers = [
        'contract_address',
        'implementation_address',
        'contract_name',
        'function_name',
        'signature',
        'state_mutability',
        'input_params',
        'output_params'
    ]

    write_csv(event_csv_path, event_signatures, event_headers)
    write_csv(function_csv_path, function_signatures, function_headers)

    logger.info("Signature CSV generation complete.")
    print("Signature CSV generation complete.")

if __name__ == '__main__':
    main()