{{ config(materialized='view', tags=['production','execution','transactions', 'tier0', 'api: distinct_projects_sectors_total']) }}

SELECT
  toFloat64(countDistinct(project)) AS value1,  
  toFloat64(countDistinct(sector))  AS value2    
FROM {{ ref('fct_crawlers_data_distinct_projects_sectors') }}