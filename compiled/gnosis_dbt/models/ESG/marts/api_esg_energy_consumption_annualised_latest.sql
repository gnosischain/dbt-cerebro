

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`) AS as_of_date
FROM (
SELECT
    annual_energy_Mwh_projected
FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
ORDER BY date DESC 
LIMIT 1
) AS sub