

WITH owners AS (
    SELECT
        safe_address,
        count() AS current_owner_count,
        any(current_threshold) AS current_threshold
    FROM `dbt`.`int_execution_safes_current_owners`
    GROUP BY safe_address
)

SELECT
    lower(co.owner) AS owner_address,
    lower(co.safe_address) AS safe_address,
    co.became_owner_at AS became_owner_at,
    o.current_threshold AS current_threshold,
    o.current_owner_count AS current_owner_count,
    s.creation_version AS creation_version,
    s.block_date AS deployment_date
FROM `dbt`.`int_execution_safes_current_owners` AS co
LEFT JOIN owners AS o ON o.safe_address = co.safe_address
LEFT JOIN `dbt`.`int_execution_safes` AS s ON s.safe_address = co.safe_address
WHERE co.owner IS NOT NULL
  AND co.safe_address IS NOT NULL