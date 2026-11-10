



select
    1
from `dbt`.`int_esg_dynamic_power_consumption`

where not(carbon_intensity_gco2_kwh > 0)

