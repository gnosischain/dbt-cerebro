-- Pre-joined GP-side journey spine, 30-day lookback. The 'gpay' sector
-- variant matches identity_role on both conversion + touchpoint sides
-- so role-grain semantics are preserved (treasury vs owner vs delegate).
--
-- Microbatched in 1-month chunks via scripts/full_refresh/refresh.py
-- because the role fan-out (3 roles × all GP activity × 30-day lookback)
-- can transiently OOM at 10 GiB on a single 2-year run. Per-month batches
-- generally fit under the cap; refresh.py auto-retries the rare batch
-- that does push the limit.

{{ build_journey_lookback(30, 'gpay') }}
