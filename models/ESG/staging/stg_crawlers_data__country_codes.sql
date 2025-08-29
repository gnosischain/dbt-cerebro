SELECT
    "name",
    "alpha-2",
    "alpha-3",
    "country-code",
    "iso_3166-2",
    "region",
    "sub-region",
    "intermediate-region",
    "region-code",
    "sub-region-code",
    "intermediate-region-code"
FROM
    {{ source('crawlers_data','country_codes') }}