




SELECT 
    date
    ,label
    ,category
    ,value
FROM (
    SELECT date, 'Home Staker' AS label, 'CO2e (kg)' AS category, home_staker_co2_kg_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Professional Validator' AS label, 'CO2e (kg)' AS category, professional_co2_kg_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Cloud Provider' AS label, 'CO2e (kg)' AS category, cloud_co2_kg_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Unknown' AS label, 'CO2e (kg)' AS category, unknown_co2_kg_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())

    UNION ALL 

    SELECT date, 'Home Staker' AS label, 'Energy (kWh)' AS category, home_staker_energy_kwh_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Professional Validator' AS label, 'Energy (kWh)' AS category, professional_energy_kwh_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Cloud Provider' AS label, 'Energy (kWh)' AS category, cloud_energy_kwh_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Unknown' AS label, 'Energy (kWh)' AS category, unknown_energy_kwh_daily AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())

    UNION ALL 

    SELECT date, 'Home Staker' AS label, 'Nodes' AS category, CAST(home_staker_nodes AS Float64) AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Professional Validator' AS label, 'Nodes' AS category, CAST(professional_nodes AS Float64) AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Cloud Provider' AS label, 'Nodes' AS category, CAST(cloud_nodes AS Float64) AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT date, 'Unknown' AS label, 'Nodes' AS category, CAST(unknown_nodes AS Float64) AS value FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
)
ORDER BY date, label, category