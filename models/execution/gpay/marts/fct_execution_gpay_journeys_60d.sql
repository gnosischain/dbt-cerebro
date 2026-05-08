-- Pre-joined GP-side journey spine, 60-day lookback. Sensitivity-sweep
-- variant of the canonical 30-day mart. Memory-risky: at 3-role
-- identity fan-out × 60-day lookback × 2-year history this can OOM
-- the 10 GiB cluster cap even at monthly batches; the schema config
-- pins batch_months=1 so refresh.py can retry the rare batch that
-- pushes the limit.

{{ build_journey_lookback(60, 'gpay') }}
