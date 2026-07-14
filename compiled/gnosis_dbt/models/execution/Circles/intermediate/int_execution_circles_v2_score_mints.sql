

-- Per-mint fact for the OffchainScoreBasedMintPolicy PersonalMinted event.
-- `collateral` is the minter's personal-token id = uint256(avatar); the avatar
-- address is its low 160 bits. `score` is the avatar's off-chain trust score at
-- mint time (raw int; MAX_SCORE() is the ceiling). `day` is the Circles inflation day.
SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    lower(decoded_params['group'])                                                AS group_address,
    lower(concat('0x', lpad(lower(hex(bitAnd(
        toUInt256OrZero(decoded_params['collateral']),
        toUInt256('1461501637330902918203684832716283019655932542975')))), 40, '0'))) AS avatar,
    toFloat64(toUInt256OrZero(decoded_params['amount'])) / 1e18                    AS amount,
    toUInt64OrZero(decoded_params['score'])                                       AS score,
    toFloat64(toUInt256OrZero(decoded_params['mintedAmountOnToday'])) / 1e18       AS minted_amount_on_today,
    toUInt64OrZero(decoded_params['day'])                                         AS circles_day,
    toDate(block_timestamp)                                                        AS mint_date
FROM `dbt`.`contracts_circles_v2_score_policy_events`
WHERE event_name = 'PersonalMinted'