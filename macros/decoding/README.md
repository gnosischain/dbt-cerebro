# Contract Decoding System

This document explains the contract decoding system. This system allows for efficient decoding of EVM contract calls and events, providing human-readable data from blockchain transactions and logs.

## Overview

The system consists of three main components:

1. **ABI Retrieval System** - For fetching and storing contract ABIs from Blockscout
2. **Signature Generation** - For processing ABIs into usable function and event signatures
3. **Decoding Macros** - For transforming raw blockchain data into readable structures

## Key Components

### Database Tables

- `contract_abis` - Stores raw contract ABIs
- `function_signatures` - Stores processed function signatures for transaction decoding 
- `event_signatures` - Stores processed event signatures for log decoding

### Macros

- `fetch_abi_from_blockscout.sql` - Retrieves ABI from Blockscout API
- `decode_calls.sql` - Decodes transaction inputs using function signatures
- `decode_logs.sql` - Decodes event logs using event signatures

### Scripts

- `signature_generator.py` - Processes ABIs to generate signature tables

## Setup Process

### 1. Set Up Contract ABIs Table

First, create the storage table for the ABIs:

```bash
dbt run --select execution.abi.contract_abis
```

### 2. Fetch ABIs for Contracts

For each contract you need to decode, fetch its ABI:

```bash
dbt run-operation fetch_and_insert_abi --args '{"address": "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d"}'
```

Repeat this for all contracts you want to decode. The system will store these ABIs in the `contract_abis` table.

### 3. Generate Signature Tables

Run the signature generator script to process the ABIs:

```bash
python scripts/signature_generator.py
```

This script:
- Reads the ABIs from the `contract_abis` table
- Calculates function and event signatures using Web3's `keccak256` function
- Creates/updates the `function_signatures` and `event_signatures` tables

> **Important Note**: The keccak256 hash function required for topic0 hash calculation is not currently available natively in ClickHouse Cloud. This is why we use a Python script with Web3.py to perform this calculation externally. In the future, when ClickHouse adds native support for keccak256, this process could be integrated directly into the dbt pipeline.

### 4. Create Models for Contracts

Now you can create models that use the decoding macros:

#### For Transaction Calls:

```sql
-- models/execution/contracts/tokens/my_contract_calls.sql
{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        engine = 'ReplacingMergeTree()',
        order_by = '(block_timestamp, transaction_hash)',
        unique_key = '(block_timestamp, transaction_hash)',
        partition_by = 'toStartOfMonth(block_timestamp)',
        settings = { 'allow_nullable_key': 1 },
        pre_hook = ["SET allow_experimental_json_type = 1"]
    )
}}

{{ 
    decode_calls(
        tx_table = source('execution','transactions'),
        contract_address = '0xYOUR_CONTRACT_ADDRESS',
        output_json_type = true,
        incremental_column = 'block_timestamp'
    )
}}
```

#### For Event Logs:

```sql
-- models/execution/contracts/tokens/my_contract_events.sql
{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        engine = 'ReplacingMergeTree()',
        order_by = '(block_timestamp, log_index)',
        unique_key = '(block_timestamp, log_index)',
        partition_by = 'toStartOfMonth(block_timestamp)',
        settings = { 'allow_nullable_key': 1 },
        pre_hook = ["SET allow_experimental_json_type = 1"]
    )
}}

{{ 
    decode_logs(
        source_table = source('execution','logs'),
        contract_address = '0xYOUR_CONTRACT_ADDRESS',
        output_json_type = true,
        incremental_column = 'block_timestamp'
    )
}}
```

### 5. Run Your Models

Execute your models to decode the data:

```bash
dbt run --select my_contract_calls my_contract_events
```

## Adding New Contracts

To add a new contract for decoding:

1. **Fetch the ABI**:
   ```bash
   dbt run-operation fetch_and_insert_abi --args '{"address": "0xNEW_CONTRACT_ADDRESS"}'
   ```

2. **Regenerate Signature Tables**:
   ```bash
   python scripts/signature_generator.py
   ```

3. **Create Model Files** for the new contract using the template above
   
4. **Run the New Models**:
   ```bash
   dbt run --select your_new_models
   ```

## How It Works

### ABI Retrieval

The `fetch_abi_from_blockscout.sql` macro:
- Makes an HTTP request to Blockscout API
- Extracts the ABI from the response
- Stores it in the `contract_abis` table

### Signature Generation

The `signature_generator.py` script:
1. Reads ABIs from the `contract_abis` table
2. For each function/event:
   - Constructs the canonical signature format (name and parameter types)
   - Calculates keccak256 hash using Web3.py
   - For functions: Takes first 4 bytes of the hash (function selector)
   - For events: Uses the full 32-byte hash (topic0)
3. Populates the signature tables with name, types, and hash information

### Decoding Process

#### Transaction Decoding:
1. `decode_calls.sql` matches transaction input data selector (first 4 bytes) with function signatures
2. Extracts function arguments based on ABI parameter types
3. Returns decoded parameters as a structured JSON object

#### Event Decoding:
1. `decode_logs.sql` matches log topic0 with event signatures
2. Separates indexed parameters (in topics) from non-indexed (in data field)
3. Decodes all parameters according to their types
4. Returns a structured JSON with all decoded event parameters

## Requirements

- ClickHouse 24.1 or later
- Python dependencies:
  - web3
  - clickhouse-connect
  - Other dependencies in requirements.txt

## Troubleshooting

### Common Issues:

1. **Missing ABI**:
   - Check if the ABI was fetched correctly:
   ```sql
   SELECT * FROM contract_abis WHERE contract_address = '0xYOUR_CONTRACT_ADDRESS'
   ```
   - Try fetching the ABI again or verify the contract address

2. **Signature Generation Failed**:
   - Check if signatures were created:
   ```sql
   SELECT * FROM function_signatures WHERE contract_address = '0xYOUR_CONTRACT_ADDRESS'
   SELECT * FROM event_signatures WHERE contract_address = '0xYOUR_CONTRACT_ADDRESS'
   ```
   - Check Python script logs for errors

3. **Decoding Issues**:
   - Ensure the contract address in your model matches exactly
   - Check raw data format in source tables
   - Verify the signature tables have entries for your function or event

## Best Practices

1. **Contract Organization**:
   - Group related contracts in appropriate subdirectories
   - Use consistent naming patterns

2. **Performance Optimization**:
   - Use incremental models for large contracts
   - Consider partitioning by month for better query performance
   - Add appropriate indexes for your query patterns

3. **Maintenance**:
   - Periodically update ABIs for actively developed contracts
   - Monitor for new contract versions or upgrades

## Future Improvements

When ClickHouse adds native support for keccak256, the signature generation could be integrated directly into the dbt pipeline, eliminating the need for the external Python script.