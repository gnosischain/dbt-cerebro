

SELECT
    avatar,
    supply,
    wrapped,
    unwrapped,
    wrapped_pct,
    supply_demurraged,
    wrapped_demurraged
FROM `dbt`.`fct_execution_circles_v2_avatar_personal_token_supply_latest`