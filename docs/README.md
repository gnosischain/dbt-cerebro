# docs/ — what's durable and what's a snapshot

Not everything in this directory is current. Read this taxonomy before acting on
anything you find here.

## Durable references (kept current — safe to act on)

| Doc | What it is |
|---|---|
| [agents.md](agents.md) | Architecture of the agent knowledge system: file roles, artifact pipeline, contract resolution, gates/ratchets, CI tiers, typical flows (with diagrams). |
| [lessons/](lessons/INDEX.md) | Mistake classes with status lifecycle + evidence. Check before diagnosing or refreshing. |
| [workflows/](workflows/) | Vendor-neutral step-by-step workflows (new-model, generate-schema, refresh-advisor, incident). Claude slash commands are thin wrappers over these. |
| [semantic-authoring.md](semantic-authoring.md) | Canonical semantic-layer authoring runbook. |
| [economic_concepts.md](economic_concepts.md) | Cross-domain economic definitions (economically-active, in-app vs ecosystem scoping). |
| `../AGENTS.md` (+ scoped copies) | Required workflow, refresh levers, non-negotiable rules. |

## Point-in-time snapshots (historical — re-verify before acting)

| Doc | Dated |
|---|---|
| [model_review/](model_review/) | 2026-06 three-agent audit + revisit. **Findings may already be remediated** — see model_review/README.md. |
| [data-quality-learnings-and-remediation.md](data-quality-learnings-and-remediation.md) | 2026-07 negative-balances investigation. Durable content extracted to [lessons/](lessons/INDEX.md); §2–§5 proposals tracked there with per-lesson status. |
| [incidents/](incidents/) | Incident chronologies (e.g. the 2026 logs ingestion gap). Event-specific. |
| [api_migration_2026-07-13.md](api_migration_2026-07-13.md), [cron_preview_findings.md](cron_preview_findings.md), [cron_preview_remediation_results.md](cron_preview_remediation_results.md) | Dated migration/remediation trackers. |
| [gnosis_app_gt_build_spec.md](gnosis_app_gt_build_spec.md), [native_token_prices_build_plan.md](native_token_prices_build_plan.md), [future-validator-gpay-modeling.md](future-validator-gpay-modeling.md), [mixpanel_ga_*.md](.) | Build specs / forward-looking plans — may describe things that were never built or were built differently. |

Rule of thumb: a lesson record (`lessons/`) states what is *currently enforced* via its
status field; everything in the snapshot list states what someone *found or proposed on
a given date*.
