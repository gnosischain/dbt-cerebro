# Refresh Advisor

Recommend the correct refresh/backfill lever for a model and situation, with the exact
command. Input: a model name, optionally with a description of what happened —
e.g. "a raw backfill landed for March" or "needs full history rebuild".

## Procedure

1. **Get the change packet** (this is mandatory, not optional):
   `python scripts/agent_context/context.py --select <model> --task backfill`
   It prints materialization, strategy (and whether it's a var-dependent expression),
   staged full_refresh config, cumulative flag, hazards, and the reprocess runbook.

2. **Check for pending run state**: list `target/refresh_state/` and check for a legacy
   `scripts/full_refresh/.refresh_state.json`. If a pending run overlaps the model,
   the answer is "finish or clear that run first" — refresh.py will refuse anyway.

3. **Classify downstream** before recommending anything that rewrites history:
   `grep -rl '{{ this }}' models/` intersected with `dbt ls -s <model>+` — cumulative
   downstreams mean history-first, chronological ordering
   (docs/lessons/backfill-order-cumulative.md).

4. **Pick the lever** from the decision table in AGENTS.md:
   - Daily catch-up → `dbt_incremental_runner.py`
   - Raw source backfilled into a passed month (decode chains) → `gap_window_refresh.py`
   - Full-history rebuild with `meta.full_refresh` stages → `full_refresh/refresh.py`
   - Small one-off → plain `dbt run -s`
   - Table-materialized with month-var branches → plain `dbt run` ONLY
     (docs/lessons/table-mat-batch-vars-truncation.md)
   - Sliced revenue-style reprocess → `reprocess_overwrite` one `slice` at a time
     (docs/lessons/append-over-populated-duplicates.md)

5. **Output**: the chosen lever, the exact command(s) in order, which lessons apply
   (by id), and what to verify afterwards (the packet's validation selectors). If the
   situation doesn't match any lever cleanly, say so and explain the conflict rather
   than guessing.
