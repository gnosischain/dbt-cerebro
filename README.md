# Cerebro dbt - Gnosis Chain Analytics

![Cerebro dbt](img/header-cerebro-dbt.png)

This repository contains [dbt](https://www.getdbt.com/) models for comprehensive analysis of Gnosis Chain blockchain data. The project transforms raw on-chain data into actionable insights across multiple domains including P2P networking, consensus mechanisms, execution layer activity, and environmental sustainability metrics.

## 🏗️ Project Overview

Cerebro dbt enables transformation and analysis of Gnosis Chain data across four main domains:

- **📡 P2P Network**: Peer-to-peer interactions, client distributions, and network topology analysis
- **⚖️ Consensus Layer**: Validator activity, block proposals, attestations, and consensus-layer events
- **⚡ Execution Layer**: Transaction analysis, smart contract interactions, DeFi protocols, and user activity
- **🌍 ESG & Sustainability**: Environmental metrics including power consumption and carbon emissions
- **🔗 Contract Decoding**: Automated ABI management and smart contract event/transaction decoding

All data sources are unified in **ClickHouse Cloud** for high-performance analytics.

## 📁 Project Structure

```
├── models/
│   ├── consensus/           # Validator metrics and consensus analysis
│   ├── execution/           # Execution layer analysis
│   │   ├── abi/            # Contract ABI management
│   │   ├── blocks/         # Block production and client metrics
│   │   ├── contracts/      # Decoded contract interactions
│   │   ├── rwa/           # Real-world asset tracking
│   │   ├── state/         # Blockchain state analysis
│   │   ├── transactions/  # Transaction metrics
│   │   ├── transfers/     # Token transfer analysis
│   │   └── yields/        # DeFi yield calculations
│   ├── p2p/                # P2P network analysis
│   ├── probelab/           # Probelab crawler data
│   └── ESG/                # Environmental sustainability metrics
├── macros/                 # Reusable dbt macros
│   ├── db/                # Database utilities
│   ├── decoding/          # Contract decoding system
│   └── execution/         # Execution layer helpers
├── scripts/               # Python automation scripts
└── seeds/                 # Static reference data
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- ClickHouse Cloud account
- Python 3.8+

### 1. Environment Setup

Create a `.env` file in the project root:

```bash
# ClickHouse Cloud Configuration
CLICKHOUSE_URL=your-clickhouse-cloud-host.com
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_SECURE=True

# Docker Configuration
USER_ID=1000
GROUP_ID=1000
```

### 2. Launch the Environment

```bash
# Start the dbt documentation server
docker-compose up -d

# Access dbt docs at http://localhost:8080
```

### 3. Run dbt Models

```bash
# Enter the container
docker exec -it dbt /bin/bash

# Run all models
dbt run

# Run specific model groups
dbt run --select execution.contracts
dbt run --select p2p
dbt run --select consensus
```

## 🔧 Core Features

### Smart Contract Decoding System

Our automated contract decoding system transforms raw blockchain data into human-readable insights:

#### 1. **ABI Management**
- Automated ABI fetching from Blockscout
- Support for proxy contracts and implementations
- Version tracking and updates

#### 2. **Signature Generation**
- Automatic function and event signature calculation
- Keccak256 hashing using Web3.py
- Optimized signature tables for fast lookups

#### 3. **Transaction & Event Decoding**
- Real-time decoding of contract calls
- Event log parsing with parameter extraction
- Support for complex data types (arrays, structs, dynamic types)

### Environmental Sustainability (ESG)

Track the environmental impact of the Gnosis Chain network:

- **Power Consumption**: Node-level power usage estimation
- **Carbon Emissions**: CO2 impact calculation by geography
- **Client Efficiency**: Performance metrics across different clients

### P2P Network Analysis

Comprehensive peer-to-peer network insights:

- **Geographic Distribution**: Global node distribution mapping
- **Client Diversity**: Implementation variety and versions
- **Network Topology**: Connection patterns and health metrics

## 📊 Key Model Categories

### Execution Layer Models

| Category | Purpose | Example Models |
|----------|---------|----------------|
| **Blocks** | Block production analysis | `execution_blocks_clients_daily` |
| **Transactions** | Transaction metrics | `execution_txs_info_daily` |
| **Contracts** | Decoded contract interactions | `contracts_wxdai_events` |
| **State** | Blockchain state growth | `execution_state_size_daily` |
| **Yields** | DeFi yield calculations | `yields_sdai_apy_daily` |

### P2P Network Models

| Model | Description |
|-------|-------------|
| `p2p_peers_geo_daily` | Daily geographic distribution of peers |
| `p2p_peers_clients_daily` | Client software distribution |
| `p2p_peers_cl_fork_daily` | Consensus layer fork adoption |

### ESG Models

| Model | Description |
|-------|-------------|
| `esg_carbon_emissions` | Daily CO2 emissions estimates |
| `esg_country_power_consumption` | Power usage by country |

## 🔨 Adding New Smart Contracts

### Step 1: Fetch Contract ABI

```bash
# Add a new contract for decoding
dbt run-operation fetch_and_insert_abi --args '{"address": "0xYourContractAddress"}'
```

### Step 2: Generate Signatures

```bash
# Process ABIs into function/event signatures
python scripts/signatures/signature_generator.py
```

### Step 3: Create dbt Models

Create event decoding model:

```sql
-- models/execution/contracts/your_protocol/your_contract_events.sql
{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, log_index)',
        unique_key='(block_timestamp, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        pre_hook=["SET allow_experimental_json_type = 1"]
    )
}}

{{ 
    decode_logs(
        source_table=source('execution','logs'),
        contract_address='0xYourContractAddress',
        output_json_type=true,
        incremental_column='block_timestamp'
    )
}}
```

Create transaction decoding model:

```sql
-- models/execution/contracts/your_protocol/your_contract_calls.sql
{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        unique_key='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        pre_hook=["SET allow_experimental_json_type = 1"]
    )
}}

{{ 
    decode_calls(
        tx_table=source('execution','transactions'),
        contract_address='0xYourContractAddress',
        output_json_type=true,
        incremental_column='block_timestamp'
    )
}}
```

### Step 4: Run Your Models

```bash
dbt run --select your_contract_events your_contract_calls
```

## 🛠️ Available Macros

### Contract Decoding Macros

| Macro | Purpose |
|-------|---------|
| `decode_logs()` | Decode contract event logs |
| `decode_calls()` | Decode transaction function calls |
| `fetch_abi_from_blockscout()` | Retrieve ABI from Blockscout API |
| `fetch_and_insert_abi()` | Store ABI in contracts_abi table |

### Database Utilities

| Macro | Purpose |
|-------|---------|
| `apply_monthly_incremental_filter()` | Efficient incremental processing |
| `decode_hex_split()` | Parse hex data into readable text |
| `drop_dbt_trash()` | Clean up temporary tables |

## 📈 Data Sources

### Raw Data Tables

All raw data is sourced from Gnosis Chain and stored in ClickHouse:

- **execution**: Blocks, transactions, logs, traces, contracts
- **consensus**: Validator data, attestations, proposals
- **nebula**: P2P network crawl data
- **crawlers_data**: External data (IP geolocation, country codes, etc.)

### External Data Integration

- **Probelab**: P2P network health metrics
- **Blockscout**: Contract verification and ABIs  
- **Ember**: Electricity carbon intensity data
- **IP Geolocation**: Geographic mapping of nodes

## 🔄 Incremental Processing

The project uses efficient incremental strategies:

- **Monthly Partitioning**: Data partitioned by month for optimal performance
- **Delete+Insert**: Ensures data consistency for updated records
- **Time-based Filtering**: Only processes new data since last run

## 📊 Analytics Use Cases

### DeFi Analytics
- Track sDAI yield rates and APY calculations
- Monitor RWA token price feeds from BackedFi oracles
- Analyze Aave V3 lending protocol activity

### Network Health
- Monitor client diversity across execution and consensus layers
- Track geographic decentralization
- Identify network upgrade adoption rates

### Environmental Impact
- Calculate network-wide power consumption
- Estimate carbon emissions by region
- Track sustainability improvements over time

## 🐳 Docker Configuration

The project includes a complete Docker setup:

```yaml
# docker-compose.yml provides:
# - dbt documentation server (port 8080)
# - Isolated Python environment
# - Volume mounting for development
# - Environment variable management
```

## 🔧 Development Workflow

1. **Model Development**
   ```bash
   dbt run --select +my_model  # Run model and dependencies
   dbt test --select my_model  # Run data quality tests
   ```

2. **Documentation**
   ```bash
   dbt docs generate           # Generate documentation
   dbt docs serve --port 8000  # Serve docs locally
   ```

3. **Contract Integration**
   ```bash
   # Add new contract
   dbt run-operation fetch_and_insert_abi --args '{"address": "0x..."}'
   python scripts/signatures/signature_generator.py
   dbt run --select contracts.new_protocol
   ```

## 🚨 Troubleshooting

### Common Issues

1. **Missing ABI Data**
   ```sql
   -- Check if ABI was fetched correctly
   SELECT * FROM contracts_abi WHERE contract_address = '0x...';
   ```

2. **Signature Generation Failed**
   ```sql
   -- Verify signatures were created
   SELECT * FROM function_signatures WHERE contract_address = '0x...';
   SELECT * FROM event_signatures WHERE contract_address = '0x...';
   ```

3. **Decoding Issues**
   - Ensure contract address matches exactly (case-sensitive)
   - Verify raw data format in source tables
   - Check signature tables have entries

### Performance Optimization

- Use incremental models for large datasets
- Partition by month for better query performance
- Monitor ClickHouse query performance
- Add appropriate indexes for query patterns


## 📜 License

This project is licensed under the [MIT License](LICENSE).

## 🔗 Related Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Gnosis Chain Documentation](https://docs.gnosischain.com/)
- [Blockscout API](https://gnosis.blockscout.com/api-docs)