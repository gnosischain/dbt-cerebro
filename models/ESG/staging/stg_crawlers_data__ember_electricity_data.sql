SELECT
    "Area",
    "ISO 3 code",
    "Date",
    "Area type",
    "Continent",
    "Ember region",
    "EU" ,
    "OECD",
    "G20",
    "G7",
    "ASEAN",
    "Category",
    "Subcategory",
    "Variable",
    "Unit",
    "Value",
    "YoY absolute change",
    "YoY % change" 
FROM
    {{ source('crawlers_data','ember_electricity_data') }}