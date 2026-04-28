

SELECT
    date
    ,attestations_total
    ,avg_inclusion_delay
    ,p50_inclusion_delay
    ,pct_inclusion_distance_1
    ,pct_inclusion_distance_le_2
    ,pct_inclusion_distance_gt_1
FROM `dbt`.`fct_consensus_attestations_performance_daily`