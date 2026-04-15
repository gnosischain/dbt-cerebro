# Contract Decoding System

This document explains the contract decoding system. This system allows for efficient decoding of EVM contract calls and events, providing human-readable data from blockchain transactions and logs.

## Overview

The system consists of three main components:

1. **ABI Retrieval System** - For fetching and storing contract ABIs from Blockscout
2. **Signature Generation** - For processing ABIs into usable function and event signatures
3. **Decoding Macros** - For transforming raw blockchain data into readable structures

## Key Components

### Database Tables

- `contracts_abi` - Stores raw contract ABIs
- `function_signatures` - Stores processed function signatures for transaction decoding 
- `event_signatures` - Stores processed event signatures for log decoding

### Macros

- `fetch_abi_from_blockscout.sql` - Retrieves ABI from Blockscout API
- `decode_calls.sql` - Decodes transaction inputs using function signatures
- `decode_logs.sql` - Decodes event logs using event signatures

### Scripts

- `scripts/signatures/signature_generator.py` - Processes ABIs to generate signature tables

## Setup Process

### 1. Keep `contracts_abi.csv` up to date

The preferred flow is CSV-first: keep `seeds/contracts_abi.csv` as the source of truth, then seed it into ClickHouse.

```bash
python scripts/signatures/fetch_abi_to_csv.py 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d --regen
```

If `seeds/contracts_abi.csv` is already updated manually or from another source, you can skip the fetch step and go straight to signature generation.

### 2. Generate Signature Tables

If `seeds/contracts_abi.csv` is the freshest ABI source, force the generator to read from that CSV and then seed all three CSV-backed tables:

```bash
SIGNATURE_GEN_SOURCE=csv python scripts/signatures/signature_generator.py
dbt seed --select contracts_abi event_signatures function_signatures
```

This script:
- By default, tries the ClickHouse `contracts_abi` table first and falls back to `seeds/contracts_abi.csv`
- Reads directly from `seeds/contracts_abi.csv` when `SIGNATURE_GEN_SOURCE=csv` is set
- Calculates function and event signatures using Web3's `keccak256` function
- Regenerates `seeds/function_signatures.csv` and `seeds/event_signatures.csv`, ready for `dbt seed`

When you use forced CSV mode, do not seed `contracts_abi` before running the generator. Generate first, then seed `contracts_abi`, `event_signatures`, and `function_signatures` together afterward. If you used `fetch_abi_to_csv.py --regen`, these steps already ran for you.

> **Important Note**: The keccak256 hash function required for topic0 hash calculation is not currently available natively in ClickHouse Cloud. This is why we use a Python script with Web3.py to perform this calculation externally. In the future, when ClickHouse adds native support for keccak256, this process could be integrated directly into the dbt pipeline.

### 3. Create Models for Contracts

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

### 4. Run Your Models

Execute your models to decode the data:

```bash
dbt run --select my_contract_calls my_contract_events
```

## Adding New Contracts

To add a new contract for decoding:

1. **Preferred one-shot: fetch the ABI straight into `seeds/contracts_abi.csv`, regenerate signatures, and re-seed**:
   ```bash
   python scripts/signatures/fetch_abi_to_csv.py 0xNEW_CONTRACT_ADDRESS --regen
   ```

2. **If `seeds/contracts_abi.csv` is already updated, use the manual CSV-first flow**:
   ```bash
   SIGNATURE_GEN_SOURCE=csv python scripts/signatures/signature_generator.py
   dbt seed --select contracts_abi event_signatures function_signatures
   ```

3. **Create Model Files** for the new contract using the template above
   
4. **Run the New Models**:
   ```bash
   dbt run --select your_new_models
   ```

Legacy note: `dbt run-operation fetch_and_insert_abi` is still supported, but it writes to ClickHouse first. If you use that path, export the result back to `seeds/contracts_abi.csv` before regenerating signatures so the next `dbt seed` does not wipe the new ABI row.

## How It Works

### ABI Retrieval

The preferred ABI retrieval flow keeps `seeds/contracts_abi.csv` as the source of truth:
- `scripts/signatures/fetch_abi_to_csv.py` fetches the ABI from Blockscout and writes it directly to the CSV
- `dbt seed --select contracts_abi` pushes that CSV into ClickHouse

The legacy `fetch_abi_from_blockscout.sql` / `fetch_and_insert_abi` flow:
- Makes an HTTP request to Blockscout API
- Extracts the ABI from the response
- Stores it in the `contracts_abi` table in ClickHouse first
- Requires exporting that table back to `seeds/contracts_abi.csv` if you want the CSV seed to stay authoritative

### Signature Generation

The `scripts/signatures/signature_generator.py` script:
1. By default, tries to read ABIs from the ClickHouse `contracts_abi` table and falls back to `seeds/contracts_abi.csv`
2. Reads directly from `seeds/contracts_abi.csv` when `SIGNATURE_GEN_SOURCE=csv` is set
3. For each function/event:
   - Constructs the canonical signature format (name and parameter types)
   - Calculates keccak256 hash using Web3.py
   - For functions: Takes first 4 bytes of the hash (function selector)
   - For events: Uses the full 32-byte hash (topic0)
4. Writes `seeds/function_signatures.csv` and `seeds/event_signatures.csv`, which are then loaded with `dbt seed`

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
   SELECT * FROM contracts_abi WHERE contract_address = '0xYOUR_CONTRACT_ADDRESS'
   ```
   - If you are using the CSV-first flow, also verify that `seeds/contracts_abi.csv` contains the row you expect
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
