WITH payment_topic AS (
    SELECT lower(signature) AS signature
    FROM `dbt`.`event_signatures`
    WHERE lower(contract_address) = '0x186725d8fe10a573dc73144f7a317fcae5314f19'
      AND event_name = 'PaymentReceived'
    LIMIT 1
),
gateway_logs AS (
    SELECT count(*) AS gateway_runtime_log_count
    FROM `execution`.`logs`
    WHERE lower(concat('0x', address)) IN (
        SELECT address
        FROM `dbt`.`contracts_circles_registry`
        WHERE contract_type = 'PaymentGatewayRuntime'
    )
      AND lower(replaceAll(topic0, '0x', '')) = (
          SELECT replaceAll(signature, '0x', '')
          FROM payment_topic
      )
)

SELECT *
FROM gateway_logs
WHERE gateway_runtime_log_count > 0