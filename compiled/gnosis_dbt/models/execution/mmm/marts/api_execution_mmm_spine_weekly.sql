

-- API view passthrough over fct_execution_mmm_spine_weekly. Tier1
-- endpoint, requires X-API-Key. The MMM analyst persona reads this
-- directly to pull a complete (kpi, media, control) weekly spine
-- without hand-rolling the union.

SELECT * FROM `dbt`.`fct_execution_mmm_spine_weekly`
ORDER BY week