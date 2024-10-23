{{ 
    config(
        materialized='table'
    ) 
}}

WITH

validators AS (
    SELECT
        COUNT(*)
    FROM
        {{ get_postgres('chaind', 't_attestations') }}
)

SELECT * FROM validators