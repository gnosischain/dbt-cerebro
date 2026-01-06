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
        tags                    = ['production','contracts','swapr','events','stablecoin_pools']
    )
}}

{# 
    Pool addresses:
    - 0x2bf164859173e70ff4f574128d22c09505712313 (WETH/USDC)
    - 0x2de7439f52d059e6cadbbeb4527683a94331cf65 (sDAI/EURe)
    - 0x4a3fec341be7134b8ef9e9edb6bf63ae2ba17f43 (WETH/WxDAI)
    - 0x63aec253e5dbdcfc634f5e9dd18daaeb6632c7c7 (GNO/WxDAI)
    - 0x6a1507579b50abfc7ccc8f9e2b428095b5063538 (USDC/WxDAI)
    - 0x80086b6a53249277961c8672f0c22b3f54ac85fb (GNO/sDAI)
    - 0x827fd9bb1bcdc9910b2261bf413453eda29aeaf6 (bCSPX/sDAI)
    - 0xa58b0b919fe089edb0266f11c518768031268f55 (SAFE/WxDAI)
    - 0xc67e7153daa362bbad23c862b1ba56e0e7f596e6 (USDC.e/WxDAI)
    - 0xfa374c336c9d7c912f423940dc7ec4f2f52bca0d (EURe/WxDAI)
    - 0xfd24c5c19df9f124f385b3c0f38f8c6c72f5a137 (sDAI/WxDAI)
#}

{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = [
            '0x2bf164859173e70ff4f574128d22c09505712313',  
            '0x2de7439f52d059e6cadbbeb4527683a94331cf65',  
            '0x4a3fec341be7134b8ef9e9edb6bf63ae2ba17f43',  
            '0x63aec253e5dbdcfc634f5e9dd18daaeb6632c7c7',  
            '0x6a1507579b50abfc7ccc8f9e2b428095b5063538',  
            '0x80086b6a53249277961c8672f0c22b3f54ac85fb',  
            '0x827fd9bb1bcdc9910b2261bf413453eda29aeaf6',  
            '0xa58b0b919fe089edb0266f11c518768031268f55',  
            '0xc67e7153daa362bbad23c862b1ba56e0e7f596e6',  
            '0xfa374c336c9d7c912f423940dc7ec4f2f52bca0d',  
            '0xfd24c5c19df9f124f385b3c0f38f8c6c72f5a137'   
        ],
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-10-06'  
    )
}}
