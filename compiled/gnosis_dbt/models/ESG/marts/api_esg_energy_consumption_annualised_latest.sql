


SELECT
    annual_energy_Mwh_projected
FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
ORDER BY date DESC 
LIMIT 1