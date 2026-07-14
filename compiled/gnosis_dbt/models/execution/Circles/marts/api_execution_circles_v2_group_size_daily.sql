

-- Dashboard view over the per-group daily member count. Latest incomplete
-- day excluded per api_ convention.
SELECT date, group_address, n_members
FROM `dbt`.`int_execution_circles_v2_group_size_daily`
WHERE date < today()