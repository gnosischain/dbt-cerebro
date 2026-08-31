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
-- cover all phases per feed (phaseAggregators) for full history; Chainlink rotated
-- every Gnosis aggregator on 2026-08-06/07 (third address per feed below).
--
--   GNO/USD    : 0x016a45F646bbd35B61fE7A496a75D9Ea69bD243E, 0xcA16Ed36A7d1Ae2DC68873D62bce4f9BdCc2d378, 0xe2cd6230A5168932A7908a97e0a84a5e76aC6854
--   ETH/USD    : 0x44513922bf52cEc40a0557797b040805deD50140, 0x059e7Bd8157e0d302dF3626E162B6C835340b311, 0x89BAf01D6E78b1F76922096f711430dB2DEFBbA2
--   WBTC/USD   : 0x5ED6A59735297Bc5D6CB4942913Ae7098E0cD703, 0x03FC3B121dCD823170f2bADd1A4CcE3DB589f9E2
--   EUR/USD    : 0x759be90a34E426042ed7d17916B78a5cD2567dd1, 0x0593D34ecB0B490551B8104F93735084b3E1fa49
--   CHF/USD    : 0xbe18b8F41760878ba6D3b1E9475c4CcAD3D9aA8f, 0x6E2482E011EC31a1960a938791B6B4Ff5BAa3217, 0xc78f9C32Ad86eb42885ccf1201947D825047C76D
--   wstETH-ETH : 0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55, 0x3aeB3548ab12117eC16c69da02240128A76dA4D7  (18-decimal exchange rate)
--   USDC/USD   : 0xc15288Bc7E921dc462d9c4CE151318D5AA428a53, 0x30bA871Ee7a08dBd255CdD8e7e035DAd72014E27, 0xd24B7aB13AE7cD8871806F0a1b1Ae66C0A225Ac2
--   USDT/USD   : 0xc4D924b6baB6FEc909E482b93847D997463f0c79, 0x096D1296515ae8154a8E7709DA66a8dc25C49b47
--   DAI/USD    : 0x12A6B73A568f8DC3D24DA1654079343f18f69236, 0xb65566283CAcE6b281308308da0f0783a613c416, 0xE034125D3c8CcCAfF97F498f166fF98cD839ee4c
{{
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = [
            '0x016a45F646bbd35B61fE7A496a75D9Ea69bD243E', '0xcA16Ed36A7d1Ae2DC68873D62bce4f9BdCc2d378', '0xe2cd6230A5168932A7908a97e0a84a5e76aC6854',
            '0x44513922bf52cEc40a0557797b040805deD50140', '0x059e7Bd8157e0d302dF3626E162B6C835340b311', '0x89BAf01D6E78b1F76922096f711430dB2DEFBbA2',
            '0x5ED6A59735297Bc5D6CB4942913Ae7098E0cD703', '0x03FC3B121dCD823170f2bADd1A4CcE3DB589f9E2',
            '0x759be90a34E426042ed7d17916B78a5cD2567dd1', '0x0593D34ecB0B490551B8104F93735084b3E1fa49',
            '0xbe18b8F41760878ba6D3b1E9475c4CcAD3D9aA8f', '0x6E2482E011EC31a1960a938791B6B4Ff5BAa3217', '0xc78f9C32Ad86eb42885ccf1201947D825047C76D',
            '0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55', '0x3aeB3548ab12117eC16c69da02240128A76dA4D7',
            '0xc15288Bc7E921dc462d9c4CE151318D5AA428a53', '0x30bA871Ee7a08dBd255CdD8e7e035DAd72014E27', '0xd24B7aB13AE7cD8871806F0a1b1Ae66C0A225Ac2',
            '0xc4D924b6baB6FEc909E482b93847D997463f0c79', '0x096D1296515ae8154a8E7709DA66a8dc25C49b47',
            '0x12A6B73A568f8DC3D24DA1654079343f18f69236', '0xb65566283CAcE6b281308308da0f0783a613c416', '0xE034125D3c8CcCAfF97F498f166fF98cD839ee4c'
        ],
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2021-01-01'
    )
}}
