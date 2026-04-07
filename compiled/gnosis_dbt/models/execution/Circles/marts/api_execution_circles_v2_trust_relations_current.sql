

SELECT
    truster,
    trustee,
    valid_from,
    valid_to
FROM `dbt`.`fct_execution_circles_v2_trust_relations_current`
ORDER BY valid_from DESC