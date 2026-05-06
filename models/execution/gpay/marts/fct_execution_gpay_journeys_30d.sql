-- Pre-joined GP-side journey spine, 30-day lookback. The 'gpay' sector
-- variant matches identity_role on both conversion + touchpoint sides
-- so role-grain semantics are preserved (treasury vs owner vs delegate).

{{ build_journey_lookback(30, 'gpay') }}
