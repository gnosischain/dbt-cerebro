

SELECT 'avatar_count_v2' AS measure, toUInt64(count()) AS value
FROM `dbt`.`int_execution_circles_v2_avatars`

UNION ALL

SELECT 'human_count_v2', toUInt64(countIf(avatar_type = 'Human'))
FROM `dbt`.`int_execution_circles_v2_avatars`

UNION ALL

SELECT 'group_count_v2', toUInt64(countIf(avatar_type = 'Group'))
FROM `dbt`.`int_execution_circles_v2_avatars`

UNION ALL

SELECT 'org_count_v2', toUInt64(countIf(avatar_type = 'Organization'))
FROM `dbt`.`int_execution_circles_v2_avatars`

UNION ALL

SELECT 'active_trust_count_v2', toUInt64(count())
FROM `dbt`.`fct_execution_circles_v2_trust_relations_current`

UNION ALL

SELECT 'token_count_v2', toUInt64(countDistinct(token_address))
FROM `dbt`.`int_execution_circles_v2_balances_daily`
WHERE date = yesterday()

UNION ALL

SELECT 'wrapper_count_v2', toUInt64(count())
FROM `dbt`.`int_execution_circles_v2_wrappers`