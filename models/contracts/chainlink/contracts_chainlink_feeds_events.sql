{{
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'append',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, log_index)',
        unique_key              = '(block_timestamp, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                },
        tags                    = ['production','contracts','chainlink','events','microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
-- All Chainlink price-feed aggregators we use on Gnosis, decoded in a SINGLE pass.
-- One combined model (vs one per feed) so execution.logs is scanned once per month
-- instead of once per feed (~9x fewer scans; cheaper backfill + daily refresh).
-- decoded_params['current'] is the answer; contract_address identifies which feed
-- (mapped to feed -> token symbol in int_execution_prices_oracle_daily). Address arrays
-- cover both phases per feed (phaseAggregators) for full history.
--
--   GNO/USD    : 0x016a45F646bbd35B61fE7A496a75D9Ea69bD243E, 0xcA16Ed36A7d1Ae2DC68873D62bce4f9BdCc2d378
--   ETH/USD    : 0x44513922bf52cEc40a0557797b040805deD50140, 0x059e7Bd8157e0d302dF3626E162B6C835340b311
--   WBTC/USD   : 0x5ED6A59735297Bc5D6CB4942913Ae7098E0cD703
--   EUR/USD    : 0x759be90a34E426042ed7d17916B78a5cD2567dd1
--   CHF/USD    : 0xbe18b8F41760878ba6D3b1E9475c4CcAD3D9aA8f, 0x6E2482E011EC31a1960a938791B6B4Ff5BAa3217
--   wstETH-ETH : 0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55  (18-decimal exchange rate)
--   USDC/USD   : 0xc15288Bc7E921dc462d9c4CE151318D5AA428a53, 0x30bA871Ee7a08dBd255CdD8e7e035DAd72014E27
--   USDT/USD   : 0xc4D924b6baB6FEc909E482b93847D997463f0c79
--   DAI/USD    : 0x12A6B73A568f8DC3D24DA1654079343f18f69236, 0xb65566283CAcE6b281308308da0f0783a613c416
{{
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = [
            '0x016a45F646bbd35B61fE7A496a75D9Ea69bD243E', '0xcA16Ed36A7d1Ae2DC68873D62bce4f9BdCc2d378',
            '0x44513922bf52cEc40a0557797b040805deD50140', '0x059e7Bd8157e0d302dF3626E162B6C835340b311',
            '0x5ED6A59735297Bc5D6CB4942913Ae7098E0cD703',
            '0x759be90a34E426042ed7d17916B78a5cD2567dd1',
            '0xbe18b8F41760878ba6D3b1E9475c4CcAD3D9aA8f', '0x6E2482E011EC31a1960a938791B6B4Ff5BAa3217',
            '0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55',
            '0xc15288Bc7E921dc462d9c4CE151318D5AA428a53', '0x30bA871Ee7a08dBd255CdD8e7e035DAd72014E27',
            '0xc4D924b6baB6FEc909E482b93847D997463f0c79',
            '0x12A6B73A568f8DC3D24DA1654079343f18f69236', '0xb65566283CAcE6b281308308da0f0783a613c416'
        ],
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2021-01-01'
    )
}}
