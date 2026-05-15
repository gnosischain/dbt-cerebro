

SELECT
    w.wrapper_address,
    w.avatar,
    w.circles_type,
    coalesce(
        nullIf(splitByString('_0x', dl.project_raw)[1], ''),
        concat('CRC-', substring(w.wrapper_address, 3, 5))
    ) AS symbol
FROM `dbt`.`int_execution_circles_v2_wrappers` w
LEFT JOIN `dbt`.`stg_crawlers_data__dune_labels` dl
    ON dl.address = w.wrapper_address