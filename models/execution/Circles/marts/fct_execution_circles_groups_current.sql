{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

WITH registrations AS (
    SELECT
        group_address,
        argMax(block_number, tuple(block_number, transaction_index, log_index)) AS block_number,
        argMax(block_timestamp, tuple(block_number, transaction_index, log_index)) AS block_timestamp,
        argMax(transaction_hash, tuple(block_number, transaction_index, log_index)) AS transaction_hash,
        argMax(transaction_index, tuple(block_number, transaction_index, log_index)) AS transaction_index,
        argMax(log_index, tuple(block_number, transaction_index, log_index)) AS log_index,
        argMax(mint_policy, tuple(block_number, transaction_index, log_index)) AS mint_policy,
        argMax(treasury_address, tuple(block_number, transaction_index, log_index)) AS treasury_address,
        argMax(group_name, tuple(block_number, transaction_index, log_index)) AS group_name,
        argMax(group_symbol, tuple(block_number, transaction_index, log_index)) AS group_symbol
    FROM {{ ref('int_execution_circles_group_registrations') }}
    GROUP BY 1
),
latest_settings AS (
    SELECT
        group_address,
        argMaxIf(owner, tuple(block_number, transaction_index, log_index), owner IS NOT NULL) AS owner,
        argMaxIf(mint_handler, tuple(block_number, transaction_index, log_index), mint_handler IS NOT NULL) AS mint_handler,
        argMaxIf(redemption_handler, tuple(block_number, transaction_index, log_index), redemption_handler IS NOT NULL) AS redemption_handler,
        argMaxIf(liquidity_provider, tuple(block_number, transaction_index, log_index), liquidity_provider IS NOT NULL) AS liquidity_provider,
        argMaxIf(treasury_address, tuple(block_number, transaction_index, log_index), treasury_address IS NOT NULL) AS treasury_address_override,
        argMaxIf(service, tuple(block_number, transaction_index, log_index), service IS NOT NULL) AS service,
        argMaxIf(fee_collection, tuple(block_number, transaction_index, log_index), fee_collection IS NOT NULL) AS fee_collection
    FROM {{ ref('int_execution_circles_group_settings_updates') }}
    GROUP BY 1
),
latest_metadata AS (
    SELECT
        lower(decoded_params['avatar']) AS avatar,
        argMax(decoded_params['metadataDigest'], tuple(block_number, transaction_index, log_index)) AS cid_v0_digest
    FROM {{ ref('contracts_circles_v2_NameRegistry_events') }}
    WHERE event_name = 'UpdateMetadataDigest'
    GROUP BY 1
),
wrapper_summary AS (
    SELECT
        avatar AS group_address,
        argMaxIf(wrapper_address, tuple(block_number, transaction_index, log_index), circles_type = 0) AS erc20_wrapper_demurraged,
        argMaxIf(wrapper_address, tuple(block_number, transaction_index, log_index), circles_type = 1) AS erc20_wrapper_static
    FROM {{ ref('int_execution_circles_wrappers') }}
    GROUP BY 1
),
member_counts AS (
    SELECT
        group_address,
        count() AS member_count
    FROM {{ ref('int_execution_circles_group_membership_timeline') }}
    WHERE valid_from <= toDateTime({{ circles_chain_now_ts() }})
      AND (valid_to IS NULL OR valid_to > toDateTime({{ circles_chain_now_ts() }}))
    GROUP BY 1
)

SELECT
    r.block_number,
    r.block_timestamp,
    r.transaction_hash,
    r.transaction_index,
    r.log_index,
    r.group_address,
    s.owner,
    r.mint_policy,
    s.mint_handler,
    s.redemption_handler,
    coalesce(s.treasury_address_override, r.treasury_address) AS treasury_address,
    s.service,
    s.fee_collection,
    coalesce(mc.member_count, 0) AS member_count,
    r.group_name,
    r.group_symbol,
    m.cid_v0_digest,
    w.erc20_wrapper_demurraged,
    w.erc20_wrapper_static
FROM registrations r
LEFT JOIN latest_settings s
    ON r.group_address = s.group_address
LEFT JOIN latest_metadata m
    ON r.group_address = m.avatar
LEFT JOIN wrapper_summary w
    ON r.group_address = w.group_address
LEFT JOIN member_counts mc
    ON r.group_address = mc.group_address
