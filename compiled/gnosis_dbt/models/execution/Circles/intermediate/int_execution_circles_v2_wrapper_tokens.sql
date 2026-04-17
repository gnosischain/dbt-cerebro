

SELECT
    w.wrapper_address,
    w.avatar,
    w.circles_type,
    coalesce(
        nullIf(splitByString('_0x', dl.label)[1], ''),
        concat('CRC-', substring(w.wrapper_address, 3, 5))
    ) AS symbol
FROM `dbt`.`int_execution_circles_v2_wrappers` w
LEFT JOIN `crawlers_data`.`dune_labels` dl
    ON dl.address = w.wrapper_address