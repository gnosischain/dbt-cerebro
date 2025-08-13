
SELECT
    parameter_name,
    parameter_value,
FROM 
    {{ source('consensus', 'specs') }} FINAL
