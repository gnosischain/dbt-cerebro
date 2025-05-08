#!/usr/bin/env python3
import os
from pathlib import Path
from dotenv import load_dotenv
import json
import logging
import sys
from web3 import Web3
import clickhouse_connect
from clickhouse_connect.driver.client import Client

# Find the project root directory (where .env is located)
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env'

# Load the .env file from the project root
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('signature_generator')

# --- ClickHouse Client Setup ---
host = os.environ.get('CLICKHOUSE_URL', 'localhost')
port = os.environ.get('CLICKHOUSE_PORT', '8123')
user = os.environ.get('CLICKHOUSE_USER', 'default')
password = os.environ.get('CLICKHOUSE_PASSWORD', '')
# Ensure database name matches your dbt target schema if needed
database = os.environ.get('CLICKHOUSE_DATABASE', 'dbt')

logger.info(f"Connecting to ClickHouse: {host}:{port} DB: {database} User: {user}")
try:
    client = clickhouse_connect.get_client(
        host=host,
        port=int(port),
        username=user,
        password=password,
        database=database,
        secure=True # Assume True for ClickHouse Cloud, adjust if needed
    )
    client.ping() # Verify connection
    logger.info("ClickHouse connection successful.")
except Exception as e:
    logger.error(f"Failed to connect to ClickHouse: {e}", exc_info=True)
    sys.exit(1)

# --- Web3 Initialization ---
w3 = Web3()

# --- Fetch ABIs from contract_abis Table ---
# Modified query to fetch all ABIs including proxy implementations
query = """
SELECT
    contract_address,
    implementation_address,
    abi_json,
    contract_name
FROM contract_abis
WHERE abi_json IS NOT NULL AND abi_json != '[]' AND abi_json != '{}'
"""

logger.info("Fetching ABIs from contract_abis table...")
try:
    result = client.query(query)
    abi_rows = result.result_rows # Store rows
    row_count = len(abi_rows) # Get count before loop
    logger.info(f"Fetched {row_count} rows with non-empty ABIs.")
except Exception as e:
    logger.error(f"Failed to fetch ABIs from contract_abis: {e}", exc_info=True)
    sys.exit(1)

# --- Process ABIs and Generate Signatures ---
event_signatures = []
function_signatures = []

if row_count > 0:
    logger.info("Processing ABIs...")
    for contract_address, implementation_address, abi_json, contract_name in abi_rows:
        try:
            abi = json.loads(abi_json)

            for item in abi:
                # Process events
                if item.get('type') == 'event':
                    event_name = item.get('name')
                    inputs = item.get('inputs', [])
                    anonymous = item.get('anonymous', False)

                    if event_name:
                        # Calculate event signature
                        types = [input_param['type'] for input_param in inputs]
                        signature_str = f"{event_name}({','.join(types)})"
                        signature_hash = w3.keccak(text=signature_str).hex()[2:]

                        # Prepare parameters JSON
                        params = []
                        indexed_params = []
                        non_indexed_params = []

                        for i, input_param in enumerate(inputs):
                            param_info = {
                                'name': input_param.get('name', f'param{i}'),
                                'type': input_param.get('type', 'unknown'),
                                'position': i + 1,
                                'indexed': input_param.get('indexed', False)
                            }
                            params.append(param_info)

                            if param_info['indexed']:
                                indexed_params.append(param_info)
                            else:
                                non_indexed_params.append(param_info)

                        # All event signatures for this contract address, regardless of implementation
                        event_signatures.append({
                            'contract_address': contract_address,
                            'implementation_address': implementation_address,
                            'contract_name': contract_name,
                            'event_name': event_name,
                            'signature': signature_hash,
                            'anonymous': anonymous,
                            'params': json.dumps(params),
                            'indexed_params': json.dumps(indexed_params),
                            'non_indexed_params': json.dumps(non_indexed_params)
                        })

                # Process functions
                elif item.get('type') == 'function':
                    function_name = item.get('name')
                    inputs = item.get('inputs', [])
                    outputs = item.get('outputs', [])
                    state_mutability = item.get('stateMutability', 'nonpayable')

                    if function_name:
                        # Calculate function signature
                        types = [input_param['type'] for input_param in inputs]
                        signature_str = f"{function_name}({','.join(types)})"
                        signature_hash = w3.keccak(text=signature_str).hex()[2:10]

                        # Prepare input parameters JSON
                        input_params = []
                        for i, input_param in enumerate(inputs):
                            input_params.append({
                                'name': input_param.get('name', f'param{i}'),
                                'type': input_param.get('type', 'unknown'),
                                'position': i + 1
                            })

                        # Prepare output parameters JSON
                        output_params = []
                        for i, output_param in enumerate(outputs):
                            output_params.append({
                                'name': output_param.get('name', f'return{i}'),
                                'type': output_param.get('type', 'unknown'),
                                'position': i + 1
                            })

                        # All function signatures for this contract address, regardless of implementation
                        function_signatures.append({
                            'contract_address': contract_address,
                            'implementation_address': implementation_address,
                            'contract_name': contract_name,
                            'function_name': function_name,
                            'signature': signature_hash,
                            'state_mutability': state_mutability,
                            'input_params': json.dumps(input_params),
                            'output_params': json.dumps(output_params)
                        })
        except json.JSONDecodeError as e:
             logger.warning(f"Could not decode ABI JSON for {contract_address}. Skipping row. Error: {e}")
        except Exception as e:
            # Log other processing errors more clearly
            logger.error(f"Error processing ABI item for {contract_address}. Skipping row. Error: {e}", exc_info=True)

# Log generated counts
logger.info(f"Generated {len(event_signatures)} event signatures.")
logger.info(f"Generated {len(function_signatures)} function signatures.")

# --- Define CREATE TABLE IF NOT EXISTS statements ---

# Define schema for event_signatures matching the insert columns
# Using MergeTree engine as a default, ORDER BY is important
create_event_table_sql = f"""
CREATE TABLE IF NOT EXISTS {database}.event_signatures (
    contract_address String,
    implementation_address String,
    contract_name String,
    event_name String,
    signature String,
    anonymous UInt8,
    params String,
    indexed_params String,
    non_indexed_params String
) ENGINE = MergeTree()
ORDER BY (contract_address, signature, event_name)
"""

# Define schema for function_signatures matching the insert columns
# Using MergeTree engine as a default, ORDER BY is important
create_function_table_sql = f"""
CREATE TABLE IF NOT EXISTS {database}.function_signatures (
    contract_address String,
    implementation_address String,
    contract_name String,
    function_name String,
    signature String,
    state_mutability String,
    input_params String,
    output_params String
) ENGINE = MergeTree()
ORDER BY (contract_address, signature, function_name)
"""

# --- Ensure tables exist, Truncate, and Insert ---
try:
    # Execute CREATE TABLE IF NOT EXISTS
    logger.info("Ensuring event_signatures table exists...")
    client.command(create_event_table_sql)
    logger.info("Ensuring function_signatures table exists...")
    client.command(create_function_table_sql)
    logger.info("Schema readiness check complete.")

    # Truncate tables before inserting new data
    logger.info("Truncating existing signature tables...")
    client.command(f"TRUNCATE TABLE {database}.event_signatures")
    client.command(f"TRUNCATE TABLE {database}.function_signatures")
    logger.info("Tables truncated.")

    # Insert event signatures
    if event_signatures: # Check if list is not empty
        logger.info(f"Preparing {len(event_signatures)} event signatures for insert...")
        event_data = []
        # Convert boolean 'anonymous' to UInt8 (0 or 1) for ClickHouse
        for es in event_signatures:
            event_data.append([
                es['contract_address'],
                es['implementation_address'],
                es['contract_name'],
                es['event_name'],
                es['signature'],
                1 if es['anonymous'] else 0, # Conversion here
                es['params'],
                es['indexed_params'],
                es['non_indexed_params']
            ])

        logger.info("Inserting event signatures...")
        client.insert(f"{database}.event_signatures", event_data,
                      column_names=[
                          'contract_address', 'implementation_address', 'contract_name',
                          'event_name', 'signature', 'anonymous', 'params', 
                          'indexed_params', 'non_indexed_params'
                      ])
        logger.info(f"Successfully inserted {len(event_data)} event signatures.")
    else:
        logger.info("No event signatures generated to insert.")

    # Insert function signatures
    if function_signatures: # Check if list is not empty
        logger.info(f"Preparing {len(function_signatures)} function signatures for insert...")
        function_data = []
        for fs in function_signatures:
            function_data.append([
                fs['contract_address'],
                fs['implementation_address'],
                fs['contract_name'],
                fs['function_name'],
                fs['signature'],
                fs['state_mutability'],
                fs['input_params'],
                fs['output_params']
            ])

        logger.info("Inserting function signatures...")
        client.insert(f"{database}.function_signatures", function_data,
                      column_names=[
                          'contract_address', 'implementation_address', 'contract_name',
                          'function_name', 'signature', 'state_mutability', 
                          'input_params', 'output_params'
                      ])
        logger.info(f"Successfully inserted {len(function_data)} function signatures.")
    else:
        logger.info("No function signatures generated to insert.")

    print("Signature generation process completed.") # Final success message

except Exception as e:
    logger.error(f"Error during table creation or saving signatures: {e}", exc_info=True)
    sys.exit(1)