

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(total_estimated, date) AS nodes_estimated,
    argMax(total_lower_95, date) AS nodes_lower_95,
    argMax(total_upper_95, date) AS nodes_upper_95
FROM (
    SELECT
        date,
        sum(estimated_total_nodes) AS total_estimated,
        sum(nodes_lower_95) AS total_lower_95,
        sum(nodes_upper_95) AS total_upper_95
    FROM `dbt`.`int_esg_node_classification`
    WHERE date < today()
    GROUP BY date
)
GROUP BY quarter
ORDER BY quarter