{# Memory contract for `tag:refill_append` models.

   When a model is rewritten in append mode for a whole month (Phase 1 of
   the prices-gap recovery script), the GROUP BY / window-function state
   for one month of source data can exceed the cluster's per-query memory
   ceiling. ClickHouse Cloud's hard cap is ~10.8 GiB; the AggregatingTransform
   then trips OvercommitTracker → Code: 241.

   These hooks cap the query at 8 GiB and force GROUP BY / sort to spill
   to disk at 2 GiB. JOINs use grace_hash so they spill too. Post-hooks
   reset to defaults so daily incremental runs are unaffected.

   Usage in a models config:

       {{ config(
           ...
           pre_hook=refill_safe_pre_hook(),
           post_hook=refill_safe_post_hook(),
       ) }}

   Any model tagged `refill_append` whose source aggregation could span a
   whole month should use these hooks. Adding the tag without these hooks
   reproduces the pattern that OOMs supply_holders_daily and
   tokens_balance_cohorts_daily during refill.
#}

{% macro refill_safe_pre_hook() %}
  {{ return([
    "SET max_memory_usage = 8000000000",
    "SET max_bytes_before_external_group_by = 2000000000",
    "SET max_bytes_before_external_sort = 2000000000",
    "SET join_algorithm = 'grace_hash'"
  ]) }}
{% endmacro %}

{% macro refill_safe_post_hook() %}
  {{ return([
    "SET max_memory_usage = 0",
    "SET max_bytes_before_external_group_by = 0",
    "SET max_bytes_before_external_sort = 0",
    "SET join_algorithm = 'default'"
  ]) }}
{% endmacro %}
