



   -- safely before any token was listed
































SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bC3M'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bC3M'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bCOIN'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bCOIN'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bCSPX'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bCSPX'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bHIGH'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bHIGH'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bIB01'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bIB01'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bIBTA'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bIBTA'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bMSTR'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bMSTR'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'bNVDA'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'bNVDA'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

UNION ALL

SELECT
  bticker,
  date,
  price
FROM (
    /* gather sparse rows, fill, forward-fill */
    SELECT
      'TSLAx'       AS bticker,
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
        FROM `dbt`.`rwa_backedfi_prices`
        WHERE bticker = 'TSLAx'
        GROUP BY date
        ORDER BY date
          WITH FILL
            FROM toDate('2020-01-01')
            TO today()
            STEP 1
    )
)
WHERE price IS NOT NULL           -- drop rows before first real point

ORDER BY bticker, date