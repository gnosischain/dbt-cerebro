
{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ flexible_source('valtrack', 'ip_metadata', 'dev') }}