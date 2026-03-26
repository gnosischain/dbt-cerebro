{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
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
        tags                    = ['production','aave','v3','contracts','events','atoken']
    )
}}


{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = [
            '0xA1Fa064A85266E2Ca82DEe5C5CcEC84DF445760e',
            '0xd0Dd6cEF72143E22cCED4867eb0d5F2328715533',
            '0x7a5c3860a77a8DC1b225BD46d0fb2ac1C6D191BC',
            '0xc6B7AcA6DE8a6044E0e32d0c841a89244A10D284',
            '0xEdBC7449a9b594CA4E053D9737EC5Dc4CbCcBfb2',
            '0xC0333cb85B59a788d8C7CAe5e1Fd6E229A3E5a65'
        ],
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-10-04'
    )
}}
