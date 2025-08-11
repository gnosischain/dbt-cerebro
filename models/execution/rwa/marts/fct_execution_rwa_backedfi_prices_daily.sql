
{% set btickers = [
  'bC3M','bCOIN','bCSPX','bHIGH',
  'bIB01','bIBTA','bMSTR','bNVDA','TSLAx'
] %}

{% set fill_start = "2020-01-01" %}   -- safely before any token was listed
{% set unions = [] %}

{% for b in btickers %}
{% set sql %}
SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      '{{ b }}'       AS bticker,
      date,
      /* forward-fill price */
      last_value(price) IGNORE NULLS
        OVER (
          ORDER BY date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS price
    FROM (
        /* sparse daily data for this one ticker */
        SELECT
          toDate(date)            AS date,
          max(price)              AS price          -- one value per day
        FROM {{ ref('int_execution_rwa_backedfi_prices') }}
        WHERE bticker = '{{ b }}'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('{{ fill_start }}')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL AND date < today()       -- drop rows before first real point
{% endset %}
{% do unions.append(sql) %}
{% endfor %}

{{ unions | join('\nUNION ALL\n') }}
ORDER BY bticker, date
