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
        tags                    = ['production','celo','contracts','chainlink','events','microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
-- Chainlink price-feed aggregators on Celo, decoded in a single pass (same
-- pattern as the Gnosis contracts_chainlink_feeds_events model). The address
-- list mirrors seeds/celo_chainlink_feeds.csv, which also maps each
-- aggregator to its proxy + symbol pair for int_celo_token_prices_daily:
--
--   CELO/USD : 0x5FB8bFAEBe2dd6dB2AF308D293ea25e607D3A922 (proxy 0x0568fD19...)
--   USDT/USD : 0xEd9183DDb0718C80488BdEbdDA15b14D3166053C (proxy 0x5e37AF40...)
--   USDC/USD : 0xa82b382154BF1e2819B8156326016537cdff9be6 (proxy 0xc7A353Ba...)
--   USDm/USD : 0x2E5eddE44187c3099529eF63Ced87994F078FbdB (cUSD feed; USDm is the
--              rebranded cUSD at 0x765d...282a — proxy 0xe38A27BE...)
--   EUR/USD  : 0x9F25Ac5A8Cea4661EdB13e4Dcc4E6D2f67F5a7Fe (proxy 0x3D207061...)
--   GBP/USD  : 0x618645D07D9ba975d976De787976F84f6c4114a7 (proxy 0xe76FE54d...)
--
-- Aggregator activity verified two ways: AnswerUpdated volumes + magnitudes in
-- celo_execution.logs (backfill window) and on Dune celo.logs through the GP
-- launch window (June-July 2026). No XAU/USD Chainlink feed exists on Celo;
-- XAUt0 pricing comes via the Dune price fallback in the price hub.
-- start_blocktime = Celo L2 migration (celo_execution history starts there).
{{
    decode_logs(
        source_table      = source('celo_execution','logs'),
        contract_address  = [
            '0x5fb8bfaebe2dd6db2af308d293ea25e607d3a922',
            '0xed9183ddb0718c80488bdebdda15b14d3166053c',
            '0xa82b382154bf1e2819b8156326016537cdff9be6',
            '0x2e5edde44187c3099529ef63ced87994f078fbdb',
            '0x9f25ac5a8cea4661edb13e4dcc4e6d2f67f5a7fe',
            '0x618645d07d9ba975d976de787976f84f6c4114a7'
        ],
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2025-03-26',
        chain             = 'celo'
    )
}}
