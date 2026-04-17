


SELECT
    
    sipHash64(concat(unhex('00'), lower(address)))
 AS user_pseudonym,
    first_seen_at,
    last_seen_at,
    heuristic_kinds,
    heuristic_hits,
    n_distinct_heuristics
FROM `dbt`.`int_execution_gnosis_app_users_current`