{{
    config(
        materialized            = 'incremental',
        incremental_strategy    = ('append' if var('start_month', none) else 'delete+insert'),
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, log_index)',
        unique_key              = '(block_timestamp, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1"
                                ],
        tags                    = ['production','spark','contracts','events','atoken']
    )
}}


{{
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = [
            '0x5671b0B8aC13DC7813D36B99C21c53F6cd376a14',
            '0x629D562E92fED431122e865Cc650Bc6bdE6B96b0',
            '0x9Ee4271E17E3a427678344fd2eE64663Cb78B4be',
            '0xC9Fe2D32E96Bb364c7d29f3663ed3b27E30767bB',
            '0xE877b96caf9f180916bF2B5Ce7Ea8069e0123182',
            '0x5850D127a04ed0B4F1FCDFb051b3409FB9Fe6B90',
            '0xA34DB0ee8F84C4B90ed268dF5aBbe7Dcd3c277ec',
            '0x08B0cAebE352c3613302774Cd9B82D08afd7bDC4',
            '0x6dc304337BF3EB397241d1889cAE7da638e6e782'
        ],
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-10-06'
    )
}}
