{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(contract_address, block_timestamp, transaction_hash)',
        unique_key              = '(contract_address, block_timestamp, transaction_hash)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1"
                                ],
        tags                    = ['production','contracts','uniswapv3','events','stablecoin_pools']
    )
}}

{# 
    Stablecoin/Stablecoin Pool addresses:
    - 0xd2233a4017aa610619df19fad6438770986ff2f1 (sDAI/EURe)
    - 0xe9e1793954f32d880ec0b2186e96d88e2b870e40 (USDC/WXDAI)
    - 0xa180bedd56438c596c9aced94d03a3001c5bb83c (USDT/USDC)
    - 0x04fd4354533879c25d59710d45d9637f7cd501b3 (sDAI/WXDAI)
#}

{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = [
            '0xd2233a4017aa610619df19fad6438770986ff2f1',
            '0xe9e1793954f32d880ec0b2186e96d88e2b870e40',  
            '0xa180bedd56438c596c9aced94d03a3001c5bb83c',  
            '0x04fd4354533879c25d59710d45d9637f7cd501b3'  
        ],
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2024-05-21'  
    )
}}

