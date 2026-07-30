# What & why

<!-- One paragraph: the change and the problem it solves. -->

## Model-change checklist

<!-- Delete this section if no models/ or semantic/ files changed. -->

- [ ] **Grain**: the grain of every touched model is stated (schema.yml / meta.agent) and unchanged — or the change is called out below.
- [ ] **Invariants & hazards**: I ran `python scripts/agent_context/context.py --select <model>` for each touched model and the change respects its listed hazards.
- [ ] **Downstream impact**: checked `python scripts/agent_context/check.py --base-ref main` — affected `api_` marts and dashboard/semantic consumers are updated or unaffected.
- [ ] **Backfill/reprocess behavior**: for incremental/staged models, I stated below how history gets (re)built and which lever applies (see AGENTS.md decision table).
- [ ] **Verification evidence**: pasted below — test output, row counts, or on-chain ground-truth checks (not just "dbt run succeeded").
- [ ] **High-risk contract**: changed high-risk models carry `meta.agent` (grain/invariants) or an entry in `agent_context/contract_ratchet.allow` with justification.
- [ ] **Bug fix?** The mistake class is captured: existing lesson updated or new `docs/lessons/<id>.md` added (with evidence), and a detection test where feasible (`/incident`).

## Backfill / reprocess notes

<!-- Which lever, which windows, in what order; or "n/a". -->

## Verification evidence

<!-- Command output, counts, comparisons. -->
