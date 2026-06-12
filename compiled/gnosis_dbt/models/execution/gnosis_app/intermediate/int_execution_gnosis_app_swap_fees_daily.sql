

-- Daily aggregated CoW protocol fee revenue from Gnosis App swaps.
-- `fee_usd` is derived from `fee_amount` (denominated in the sold token)
-- pro-rated against the trade's USD value:
--    fee_usd = fee_amount / amount_sold * amount_usd
--
-- Rows are restricted to filled trades (was_filled = 1) so cancelled
-- pre-signatures don't show as zero-fee events.




SELECT
    toDate(first_fill_at)                                                   AS date,
    count()                                                                 AS n_filled_swaps,
    uniqExact(taker)                                                        AS n_distinct_takers,
    sum(toFloat64OrNull(toString(amount_usd)))                              AS volume_usd,
    sum(toFloat64OrNull(toString(fee_amount)))                              AS fee_native_total,
    sum(
        if(amount_sold > 0,
           toFloat64OrNull(toString(fee_amount))
             / toFloat64OrNull(toString(amount_sold))
             * toFloat64OrNull(toString(amount_usd)),
           toFloat64(0))
    )                                                                       AS fee_usd_total,
    round(
        sum(
            if(amount_sold > 0,
               toFloat64OrNull(toString(fee_amount))
                 / toFloat64OrNull(toString(amount_sold))
                 * toFloat64OrNull(toString(amount_usd)),
               toFloat64(0))
        )
        / nullIf(sum(toFloat64OrNull(toString(amount_usd))), 0) * 100,
        4
    )                                                                       AS fee_pct_of_volume
FROM `dbt`.`int_execution_gnosis_app_swaps`
WHERE was_filled = 1
  AND first_fill_at IS NOT NULL
  AND first_fill_at < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(first_fill_at)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_swap_fees_daily` AS x1
        WHERE 1=1 
      )
    
  

  
GROUP BY date