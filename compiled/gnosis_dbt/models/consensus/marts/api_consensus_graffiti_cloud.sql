

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_consensus_graffiti_daily`) AS as_of_date
FROM (
SELECT
    label
    ,graffiti
    ,value
FROM `dbt`.`fct_consensus_graffiti_cloud`
ORDER BY label DESC, value DESC
) AS sub