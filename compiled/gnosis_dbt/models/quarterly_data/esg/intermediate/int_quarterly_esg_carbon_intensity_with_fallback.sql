

WITH real_data AS (
    SELECT
        month_date,
        country_code,
        carbon_intensity_mean,
        carbon_intensity_std,
        false AS is_estimated
    FROM `dbt`.`int_esg_carbon_intensity_ensemble`
),

last_known_per_country AS (
    SELECT
        country_code,
        max(month_date) AS last_real_month,
        argMax(carbon_intensity_mean, month_date) AS last_ci_mean,
        argMax(carbon_intensity_std, month_date) AS last_ci_std
    FROM `dbt`.`int_esg_carbon_intensity_ensemble`
    GROUP BY country_code
),

estimated_months AS (
    SELECT
        toStartOfMonth(addMonths(lk.last_real_month, n)) AS month_date,
        lk.country_code,
        lk.last_ci_mean AS carbon_intensity_mean,
        lk.last_ci_std AS carbon_intensity_std,
        true AS is_estimated
    FROM last_known_per_country lk
    ARRAY JOIN range(1, least(toUInt32(12), toUInt32(greatest(0, dateDiff('month', last_real_month, toStartOfMonth(today())))))) AS n
)

SELECT * FROM real_data
UNION ALL
SELECT * FROM estimated_months