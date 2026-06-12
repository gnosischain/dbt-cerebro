

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`) AS as_of_date
FROM (
SELECT
    annual_co2_tonnes_projected
FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
WHERE toStartOfMonth(date) < toStartOfMonth(today())
ORDER BY date DESC 
LIMIT 1
) AS sub