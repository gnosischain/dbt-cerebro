

SELECT sub.*, today() AS as_of_date
FROM (
SELECT
  toFloat64(countDistinct(project)) AS value1,  
  toFloat64(countDistinct(sector))  AS value2    
FROM `dbt`.`fct_crawlers_data_distinct_projects_sectors`
) AS sub