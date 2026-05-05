

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(total_observed, date) AS nodes_observed
FROM (
    SELECT date, sum(observed_nodes) AS total_observed
    FROM `dbt`.`int_esg_node_classification`
    WHERE date < today()
    GROUP BY date
)
GROUP BY quarter
ORDER BY quarter