-- Pre-joined GA-side journey spine, 30-day lookback. Leakage guard
-- (event_ts < conversion_ts) and conversion-kind exclusion are built
-- in at materialization time via the build_journey_lookback macro.
--
-- The 7d / 14d / 60d sensitivity-sweep variants are deferred (PR 5);
-- when they land they'll just call build_journey_lookback(7) /
-- build_journey_lookback(14) / build_journey_lookback(60) the same way.

{{ build_journey_lookback(30, 'gnosis_app') }}
