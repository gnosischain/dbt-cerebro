

SELECT *, today() AS as_of_date FROM `dbt`.`fct_execution_gnosis_app_gt_swaps_summary` ORDER BY app_scope, n_swaps DESC