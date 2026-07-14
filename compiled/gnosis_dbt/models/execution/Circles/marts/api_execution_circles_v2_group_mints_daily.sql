

-- Daily group-token mints vs collateral redemptions per Circles v2 group.
-- Units are deliberately kept distinct and labelled:
--   * Mints (group CRC)          = group-token issuance (mint_events, group)
--   * Redemptions (collateral CRC) = member CRC burned/returned on redeem
-- The two series are different tokens and must not be read as netting.
SELECT date, group_address, kind, sum(amount) AS amount
FROM (
    SELECT
        toDate(block_timestamp) AS date,
        lower(token_address) AS group_address,
        'Mints (group CRC)' AS kind,
        amount_raw / 1e18 AS amount
    FROM `dbt`.`int_execution_circles_v2_mint_events`
    WHERE mint_kind = 'group'

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        lower(group_address) AS group_address,
        'Redemptions (collateral CRC)' AS kind,
        abs(delta_raw) / 1e18 AS amount
    FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs`
    WHERE event_name IN ('GroupRedeemCollateralBurn', 'GroupRedeemCollateralReturn')
)
WHERE date < today()
GROUP BY date, group_address, kind