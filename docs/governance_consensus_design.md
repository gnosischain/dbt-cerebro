# Governance Consensus & Contestation — Design

Status: **Phase 1 foundation BUILT and validated in `playground_max` (2026-07-28).**
Supersedes the informal "governance sentiment analysis" idea.

## Implementation status

Built, all tests passing:

| artifact | kind | notes |
|---|---|---|
| `macros/governance/classify_choice_polarity.sql` | macro | shared direction vocabulary; verified 1:1 against the pre-existing outcome logic before extraction |
| `models/governance/intermediate/int_governance_vote_choices.sql` | table, 49,786 rows | resolves polymorphic `choice`; the foundation for every direction metric |
| `models/governance/marts/api_governance_proposal_direction.sql` | view | direction by headcount vs voting power + `head_minus_weight_against` |
| `models/governance/marts/api_governance_electorate_monthly.sql` | view | voters and capital per proposal over time |
| `models/governance/marts/api_governance_vote_trajectory.sql` | view | hourly opinion-formation curve + inflection deltas |
| `tests/governance_vote_choices_reconstruct_scores.sql` | singular test | reconciles reconstructed vp against Snapshot's own `scores[]` |
| `tests/governance_vote_choices_no_dropped_voters.sql` | singular test | fails loudly if a new ballot type silently drops voters |

Phase 2 (forum), built 2026-07-30:

| artifact | kind | notes |
|---|---|---|
| `click-runner` `create_forum_likes_table.sql` | DDL | per-like edges: one row per (post, liker) with the like's OWN timestamp |
| `click-runner` `forum_ingestor._ingest_likes()` | ingestor | crawls `/user_actions.json?filter=1`; flushes every 50 users so a long run is durable |
| `macros/governance/governance_phase_bucket.sql` | macro | one definition of the four lifecycle windows, used by two models |
| `stg_governance__forum_likes.sql` | view | staging over the new edge table |
| `int_governance_proposal_topic_links.sql` | table | the unified topic-proposal bridge, 265 pairs / 137 proposals |
| `int_governance_forum_post_phases.sql` | table | every post placed in a proposal phase, 4,122 rows / 2,745 posts |
| `int_governance_engagement_counters_daily.sql` | incremental | daily snapshots of the counters that *cannot* be backfilled |
| `api_governance_discussion_phases.sql` | view | posts, participants, likes and question share per (proposal, phase) |

Plus `schema.yml` entries in `staging/`, `intermediate/` and `marts/`, three more semantic
metrics, and a twelfth time-spine bridge. Build: **117 pass, 0 errors**. Semantic registry:
**0 errors, 0 warnings**.

### Two corrections this phase forced

**Likes are backfillable; counter-history is only for reads and views.** An earlier draft
treated all engagement counters as unrecoverable. `/user_actions.json?filter=1` returns one
row per like with its own timestamp and actor, so the *entire* like history is crawlable --
verified live, and the row counts matched `directory_items.likes_given` exactly for three
test users. Only `views` (topic) and `reads` (post) have no per-event endpoint, so
`int_governance_engagement_counters_daily` snapshots just those two. Snapshotting a like
counter would be strictly worse information at higher cost.

**Bridge coverage is 137 of 253 proposals (54%), not 89.** The 89 figure quoted earlier was
the proposal's own `discussion` URL field -- the weakest of three signals, and a subset of
the other two. GIP-number matching alone reaches 133; adding pasted post links reaches 137,
covering effectively all 134 GIP proposals. The `discussion` field is deliberately not used.

Also corrected: the full-backfill cost is **1,169 users**, not the 446 estimated from a
12-page sample of a 2,698-user directory. Expect 15-25 minutes, not 3.

### First phase-split findings (GIP proposals only)

| phase | proposals | posts | posts/proposal | question share |
|---|---|---|---|---|
| pre_discussion | 132 | 2,145 | 16.3 | 0.48 |
| pre_vote | 8 | 16 | 2.0 | 0.36 |
| voting | 102 | 501 | 4.9 | 0.28 |
| post_close | 101 | 1,374 | 13.6 | 0.41 |

Two things stand out. **`pre_vote` is almost empty** -- 16 posts across 8 proposals --
because GnosisDAO opens voting essentially when the proposal is created, so there is no
"published, now discuss before voting" window at all. And **discussion during the voting
window is thin: about 5 posts per proposal**, against 16 before and 14 after. Question
share is highest before the proposal exists (0.48) and lowest during voting (0.28), which
fits: people ask early and assert late.

Read the absolute counts with care -- `pre_discussion` and `post_close` are unbounded
windows while voting is typically 7 days, so only the per-proposal column is like-for-like.

First results: the divergence metric works on first build. GIP-119 (fund DeSciWorld) had
21.7% of voters against but **98.7% of voting power** against; GIP-65 (fund Yubiai) ran the
opposite way at 47.0% of heads versus 3.8% of power. Ten of the twelve largest gaps are
negative -- the crowd says yes, the capital says no -- and nearly all are grants or
sponsorships. The trajectory model located a real inflection on GIP-151: against-share by
voting power sat at 0.6% for 31 hours, then jumped to 4.2% at hour 40.

Not yet built: the forum phase split, resonance/provocation, the composite quadrant,
semantic-layer entries, and everything in Phases 2-4.

Still open: only whether the composite weighting needs stakeholder sign-off (section 11).

Author: analytics (Max) · Drafted 2026-07-28

Every factual claim below is tagged:

- **[V]** verified empirically on 2026-07-28, evidence inline
- **[V-prior]** verified in an earlier session (2026-07-27), not re-run today
- **[U]** **unverified** — a query or check is provided in section 8, run it before relying on this

---

## 1. Why this is not "sentiment analysis"

The original ask was to score forum comments and reactions to infer how the community
feels. That specific mechanism does not work on this corpus, and we have the measurement
to prove it rather than an opinion.

**[V-prior]** A pos/neg word-count score by quarter is **positive in every single quarter
from 2022Q1 to 2026Q3** — range 0.05 to 1.28, never once crossing zero. It is measuring
politeness, not stance. The corpus explains why: 23% of posts contain a politeness token,
19% are under 120 chars and purely procedural, and 11% contain `[quote=...]` blocks —
someone else's words, where sampled cases showed glowing quoted text wrapped in a
skeptical reply. Naive scoring inverts the author's actual position.

The reframe is not a retreat from the goal. It is a change of instrument:

> Text sentiment analysis exists because Amazon does not know whether you liked the
> product. **We do know.** 48,136 times over, signed and weighted.

A Snapshot vote is a declared, weighted, timestamped opinion. There is no inference step,
no model error, no sarcasm problem, no sampling error within the voting population. So we
measure **revealed preference** and use text as evidence rather than as a signal to score.

There is also existing precedent in this codebase for refusing to editorialize governance
outcomes: `cerebro-mcp`'s `QUORUM_STATUS_SQL` emits only `met` / `missed` / `unspecified`,
guarded by a test literally named
`test_quorum_sql_contract_never_passed_failed`. **[V]** This design follows that norm.

**Deliverable name: Consensus & Contestation.** Not sentiment.

---

## 2. Verified state of the world

### 2.1 What already exists

**[V]** The dbt governance layer is **intact and complete** on branch `feat/celo-native`
— 38 files: staging + intermediate + marts + `macros/governance/parse_gip_number.sql` +
`semantic/authoring/governance/semantic_models.yml`, with 15 `api_governance_*` marts.
(An earlier note claiming this had been trimmed was wrong.) Verify with
`git ls-tree -r --name-only <branch> | grep -i governance` — not by inspecting whichever
branch happens to be checked out.

**[V]** Staging already extracts nearly everything this design needs:

| Model | Fields relevant here |
|---|---|
| `stg_governance__forum_posts` | `post_number`, `reply_to_post_number`, `reply_count`, `reads`, `like_count`, `raw`, `cooked`, `created_at`, `updated_at` |
| `stg_governance__forum_topics` | `posts_count`, `reply_count`, `views`, `like_count`, `participant_count`, `tags`, `phase` (phase-1/2/3 parsed), `created_at`, `last_posted_at`, `bumped_at` |
| `stg_governance__snapshot_proposals` | `created_at`, `start_at`, `end_at`, `choices[]`, `scores[]`, `strategy_names[]`, `body`, `discussion`, `discussion_topic_id`, `quorum`, `scores_total` |
| `stg_governance__snapshot_votes` | `vp`, `choice_raw`, `vp_by_strategy[]`, `reason`, `created_at`, deduped to latest vote per (proposal, voter) |
| `stg_governance__forum_users` | `trust_level`, `likes_received`, `likes_given`, `post_count`, `days_visited` |

`int_governance_proposals` already derives outcome, category, `winning_choice`,
`quorum_met`, `unique_voters`, `total_vp`, `first_vote_at`, `last_vote_at`.
`int_governance_gip` is the GIP spine with `has_phase1/2/3`, `discussed_on_forum`,
`reached_vote`, and a canonical-outcome resolution that correctly refuses to pick a winner
when multi-ballot redo GIPs disagree.

### 2.2 What is not live

**[V]** Nothing is running:

- No governance cron anywhere in `infrastructure-gnosis-analytics`. Scheduled click-runner
  jobs are dune / mixpanel / cow / circles / celo_gpay / ember / probelab only. The four
  `governance-*-ingestor` docker-compose services on click-runner branch `feat/governance`
  have **no scheduler** — "daily" is an argument name. Whatever populated `governance_db`
  was a manual run.
- `dbt-cerebro` contains **zero** references to `governance_db`.
  `models/governance/governance_sources.yml:13` still defaults to `crawlers_data`.
- Nothing matching `*governance*` is materialized in **prod** `dbt`.
- Ingestion code is on the unmerged click-runner branch `feat/governance`.

**CORRECTION [V] — `playground_max` is fully populated and fully built.** An earlier draft of
this document claimed nothing was materialized in `playground_max`. That was a tool-usage
error on my part (`list_tables` with `name_pattern` and no wildcards silently returns
nothing; `like='%governance%'` returns everything). The real state:

| source table (in `playground_max`) | rows | | dbt object | rows |
|---|---|---|---|---|
| `forum_posts` | 6,847 | | `int_governance_proposals` | 253 |
| `forum_topics` | 886 | | `int_governance_gip` | 151 |
| `forum_users` | 2,667 | | `int_governance_turnout` | 134 |
| `forum_categories` | 15 | | `int_governance_vote_power_source` | 187,326 |
| `snapshot_votes` | 48,136 | | `int_governance_current_delegations` | 80 |
| `snapshot_proposals` | 253 | | `int_governance_forum_*` (3 tables) | 1,167 / 228 / 200 |
| `snapshot_follows` | 12,229 | | all 9 `stg_governance__*` | views |
| `snapshot_space` | 1 | | all 15 `api_governance_*` | views |
| `snapshot_delegations` | 127 (stale) | | | |

The two cross-domain dependencies `int_governance_turnout` needs are also present:
`api_crawlers_data_gno_supply_daily` (8,914 rows) and `api_consensus_staked_daily` (1,687).
`int_governance_turnout` at 134 rows matches the previously verified state exactly.

The 886 topics match an independent live walk of `/latest.json` exactly, and `snapshot_votes`
at 48,136 matches the earlier corpus profile — so this data is complete and current to
roughly one week ago.

**Two stale orphans to drop** (leftovers from an earlier model naming, superseded by
`int_governance_current_delegations` and `stg_governance__snapshot_delegations`):
`int_governance_snapshot_delegations_current` (210 rows) and
`stg_crawlers_data__snapshot_delegations`.

### 2.3 Access

**[V]** `user_max` gets `ACCESS_DENIED` on `governance_db` (verified twice: `list_tables`
reports "0 tables", and a direct `SELECT count()` fails with code 497). `rpc_log_indexer`
and `crawlers_data` read fine — the gap is specific to `governance_db`.

**This is not a blocker for development**, because we develop in `playground_max`. The
clean path is to run the click-runner governance ingestors with
`GOVERNANCE_DATABASE=playground_max` — the pattern their own `docker-compose.yml` header
documents — so source and models both live in `playground_max` and no grant is needed from
anyone. Cost is a second copy of roughly 8 MB.

**[V] In practice this is a non-issue: the `playground_max` copy is complete and readable**,
so all validation and diagnostics run there. A `governance_db` read grant would only be
needed to compare dev against prod, and prod has no governance models anyway. Deprioritized.

### 2.4 Delegations — resolved

**[V]** click-runner's deletion of its delegations ingestor (`feat/governance` HEAD
`201dd2e`) was **deliberate** — superseded by the `rpc-log-indexer` repo, which is live,
readable, and fresher. Measured against `rpc_log_indexer.v_delegate_events` (query
**without** `FINAL`; these are already reorg-safe canonical views):

| chain | SetDelegate | ClearDelegate | delegators | delegates | latest event |
|---|---|---|---|---|---|
| 1 (Ethereum mainnet) | 103 | 23 | 90 | 60 | 2026-05-06 |
| 100 (Gnosis Chain) | 161 | 31 | 134 | 66 | 2026-07-20 |

Chain 1 matches the numbers from the earlier Dune-based work exactly (103/23, 90
delegators, 60 delegates) — an independent cross-validation of both pipelines.

`stg_governance__snapshot_delegations` and the two delegate marts are therefore
**orphaned**, pointing at a source table nothing creates. They should be repointed at
`rpc_log_indexer.v_delegate_events`, not resurrected. Tracked separately.

**[V] The repoint is zero-risk for mainnet — the two pipelines agree byte-for-byte.**
Compared 2026-07-28:

| source | chain | set | clear | set delegators | first event | last event |
|---|---|---|---|---|---|---|
| `playground_max.snapshot_delegations` (current dbt source, Dune CSV) | n/a | 103 | 23 | 90 | 2021-06-18 15:38:52 | 2026-05-06 12:00:23 |
| `rpc_log_indexer.v_delegate_events` | 1 | 103 | 23 | 90 | 2021-06-18 15:38:52 | 2026-05-06 12:00:23 |
| `rpc_log_indexer.v_delegate_events` | 100 | 161 | 31 | 134 | 2022-03-29 15:28:05 | 2026-07-20 20:43:40 |

Identical to the second on chain 1. So the current dbt source is *exactly* the mainnet
subset, and the Dune-CSV and direct-`eth_getLogs` pipelines independently agree — a genuine
cross-validation, not a coincidence. Repointing loses nothing.

What it *adds* is chain 100: **192 further events and 134 delegators.** Note also that
**chain 100 holds all delegation activity since 2026-05-06** — read mainnet-only and
delegation looks dormant; read both and it is live through last week.

Implementation detail: the action vocabulary differs. The stale source uses `set` / `clear`;
`v_delegate_events` uses `SetDelegate` / `ClearDelegate`. `int_governance_current_delegations`
filters on the old values and must be remapped.

### 2.4.1 RESOLVED [V] — include both chains, do not filter to mainnet

An earlier draft asserted that gnosis.eth's Snapshot space "runs on network 1", so chain-100
delegations would not count. **That was wrong.** The space-level `network` field is only a
*default*; **every Snapshot strategy carries its own `network`**, and gnosis.eth's config
(fetched live from `hub.snapshot.org/graphql`, 2026-07-28) has five strategies:

| # | strategy | network | reads |
|---|---|---|---|
| 0 | `contract-call` | **100** | `getGnoVotingPower` @ `0xE6C45c06e4C73e2aD58Aedf9bf83bCe1534b524a` |
| 1 | `beacon-chain` | **100** | `gbc-snapshot.gnosischain.com` (staked GNO) |
| 2 | `contract-call` | 1 | `getGnoVotingPower` @ `0x1B2Eef4dd90cF1aF05967e0F246d4C0a19387B2A` |
| 3 | **`delegation`** | **100** | DelegateRegistry on Gnosis Chain |
| 4 | **`delegation`** | 1 | DelegateRegistry on Ethereum mainnet |

Both delegation registries are read. Per-proposal history confirms it is long-standing, not
a recent change: `delegation@1` on 252 proposals (2020-11-23 → 2026-06-19, 134 GIP);
**`delegation@100` on 231 proposals (2022-03-29 → 2026-06-19, 113 GIP)**.

Decisive corroboration: **the first chain-100 `SetDelegate` event is 2022-03-29 15:28:05 —
the same day `delegation@100` first appears on a proposal.** The strategy was added and
used immediately. These delegations have always counted.

**Recommendation: include both chains. Never filter to `chain_id = 1`.** Doing so
undercounts active delegators by 56% (80 vs 182) and hides all activity since 2026-05-06.

Treatment rules, measured [V]:

- **Counts must dedupe by address across chains.** Active delegators: 80 on chain 1, 130 on
  chain 100, **182 distinct**. A naive per-chain sum gives 210 — **28 addresses delegate on
  both chains and would be double-counted**. Use `uniqExact(delegator)`, never `sum(count)`.
- **Delegated power sums per chain.** Snapshot adds the per-strategy `vp`, so someone
  delegating on both legitimately confers power from both registries. Counts dedupe; power sums.
- **Keep `chain_id` as a dimension** on the graph mart so either view stays possible.
- **Respect era boundaries for historical views.** Before 2022-03-29 only `delegation@1`
  applied (22 proposals), so a point-in-time "who had delegated power" view must not apply
  chain-100 delegations to those proposals.

**This also corrects prior work:** the earlier Dune-based delegate graph was mainnet-only and
therefore missed 130 chain-100 delegators — it was not merely stale, it was incomplete by
design. `int_governance_current_delegations` at exactly 80 rows confirms it is the chain-1
subset.

### 2.4.2 Governance eras, now empirically established [V]

Derived from per-proposal strategy sets. This grounds the "percentile-rank within era"
rule in section 6.4, which was previously asserted without evidence:

| era | window | strategies | proposals |
|---|---|---|---|
| 1 | 2020-11-23 → 2022-03-17 | `erc20-balance-of@1`, `delegation@1` | 22 |
| 2 | 2022-03-29 → 2025-10-21 | `gno@1`, `gno@100`, `delegation@1`, `delegation@100` | 219 |
| 3 | 2025-11-16 → present | `contract-call@1`, `contract-call@100`, `beacon-chain@100`, both `delegation` | 12 |

Era 3 is when staked GNO (`beacon-chain`) enters voting power — consistent with
`int_governance_turnout`'s existing rule of adding staked supply only when the proposal's
own `strategy_names` contains `beacon-chain`. Note era 3 has only 12 proposals, so
within-era percentile ranking there is thin; consider merging eras 2 and 3 for ranking
purposes and keeping the distinction only for turnout denominators.

### 2.5 Ingestion completeness — a concern that turned out to be unfounded

**[V]** `forum.gnosis.io` runs Discourse `2026.1.5`. `/about.json` reports
`topics_count=922`, `posts_count=21894`, `users_count=2673`.

That `21,894` looks alarming next to the `6,843` rows in `forum_posts` **[V-prior]** — a
3.2x apparent gap. It is not an ingestion defect. An anonymous walk of `/latest.json`
(30 pages) finds **886 distinct topics whose `posts_count` sums to 6,884**. So the
ingestion is at **99.4% of what is anonymously visible**. The `about.json` figure counts
non-public content (PMs, restricted categories, deleted posts).

The 886-vs-922 topic gap (96%) is the known `/latest.json` limitation: archived and
unlisted topics never appear there.

**[V]** Topic size distribution, which materially constrains deliberation metrics:

| posts per topic | topics |
|---|---|
| 1 | 241 |
| 2–5 | 323 |
| 6–20 | 247 |
| 21–50 | 57 |
| 51+ | 18 |

**64% of topics have 5 posts or fewer; only 75 topics (8.5%) exceed 20 posts.** Per-topic
deliberation analysis will be meaningless for most topics and meaningful for a few dozen.
This is a strong independent argument for the triage-not-scoring approach in section 5.

Largest topics (useful as fixtures): `1529` GIP-13 Gnosis Protocol Token (157 posts),
`1904` GIP-16 xDAI/Gnosis merge (113), `4033` SNAFU airdrop (112), `10377` GIP-131 Gnosis
Pay Cashback (104), `5896` GIP-64 SAFE distribution (98), `2735` GIP-13 Phase 2 CowDAO
(96), `3476` GIP-29 safeDAO/SAFE (95), `11957` GIP-148 Treasury RFP (83).

---

## 3. What the data supports, and what it cannot

### 3.1 Signal inventory

| Signal | Grain | Phase-resolvable |
|---|---|---|
| Vote direction (`choice_raw`), `vp`, `vp_by_strategy` | (proposal, voter) | Yes — votes carry `created_at` |
| `choices[]` / `scores[]`, positionally aligned | proposal | Yes |
| `created_at` / `start_at` / `end_at` | proposal | Defines the phases |
| Post `created_at`, `post_number`, `reply_to_post_number`, `reads` | post | **Yes** |
| Vote `reason` | vote | Yes |
| Declared `phase-1/2/3` | topic | Already parsed — coverage **[U]**, see Q2 |
| Post `like_count` | post | **No** — see 3.2 |
| `views`, `participant_count` | topic | No — current-state scalar |
| `likes_given` / `likes_received` | user | No — lifetime totals |

### 3.2 Hard limits

1. **`like_count` is frozen at each topic's last bump, not current.** It is derived from
   `actions_summary[id==2].count` at fetch time, and daily mode only re-fetches topics
   whose `bumped_at` beat the watermark. Likes accruing on a dormant thread are never
   captured. **[V-prior]**
2. **No reaction types exist at all.** The discourse-reactions plugin is **not installed**
   (`/discourse-reactions/custom-reactions.json` → 404), and `post_action_types` is only
   `like(2)` plus moderation flags: `off_topic(3)`, `inappropriate(4)`, `notify_user(6)`,
   `notify_moderators(7)`, `spam(8)`, `illegal(10)`. There is nothing richer to ingest. **[V]**
3. **Ballot `choice` is polymorphic** across basic / approval / weighted / quadratic /
   ranked. A single "dissent share" is not the same quantity across types. Must branch on
   `type` or the metric is silently incomparable. Distribution **[U]**, see Q5.
4. **Deletions are never handled; silent edits are missed.** ReplacingMergeTree can only
   replace, so deleted and moved topics persist forever. **[V-prior]**
5. **Small n.** ~134 real GIPs, after excluding 119-of-253 non-GIP Snapshot proposals that
   are spam/phishing averaging ~0.1% turnout. **[V-prior]**
6. **Forum identity does not join to wallets in general.** See section 4 — it partially
   works, at low yield, with a bias that matters.

### 3.3 Phases

Two independent notions, both usable:

**Declared** — `forum_topics.tags` → phase-1 discussion / phase-2 temp check / phase-3
vote, already parsed in staging. Coverage unknown **[U]**.

**Derived windows** — the robust one, since every post carries a timestamp:

```
first forum post ──▶ proposal.created_at ──▶ start_at ──▶ end_at ──▶ after
   pre-discussion         pre-vote            voting        post-close
```

Every post buckets into exactly one window. **Post-close discussion is its own signal** —
argument arriving after the ballot closed is regret or unresolved controversy, not
deliberation. Nothing in the platform tracks this today. **[V]**

---

## 4. Identity linking — measured, and more limited than it looks

The single highest-value route was checked first and is **dead**:

**[V] There is no wallet profile field.** `/site.json` reports `user_fields` count = **0**
(the key exists, the list is empty), and `user_fields` is `null` on all 15 profiles
sampled via `/u/{username}.json`. Nothing to ingest. Route closed.

Two routes do work, both verified:

**[V] ENS resolution — 37 of 38 handles resolved** (only `fuckeverything.eth` failed),
resolved against Ethereum mainnet. Catches users whose *display name* is an ENS name even
when the username is not (`gramajo`, `cmagan`, `kelvin`, `Billion`, `TBSocialist`, `0x0mb`).

**[V] Address in username or display name — 19 users** of 400 sampled. A recurring
`"0xADDR Ethereum"` display-name pattern suggests an airdrop-claim campaign.

**[V] Combined: 62 distinct candidate addresses across roughly 53 distinct users out of
600 sampled — about 9% coverage**, with zero text mining. Bio mining adds nothing: 0 of 15
sampled bios contained an address.

### 4.1 Three findings that temper this

**Conflicts are real and must not be silently resolved.** Two users yield *different*
addresses depending on the route:

| handle | via ENS | via display name |
|---|---|---|
| `robertwilliams.eth` | `0x0b8b0a62…` | `0x95a32d9e…` |
| `0xmint777.eth` | `0x5a2abe25…` | `0xc3c78438…` |

So the link table must be **one-to-many**, record `method` and `confidence` per row, and
**surface conflicts rather than picking one**.

**On-chain participation is thin.** Of the 62 candidates, only **3** appear anywhere in
the delegation registry: `jackgale.eth` (delegator and delegate, both chains),
`5pence.eth` (delegate, mainnet), `boonjue.eth` (delegate, chain 100). The base rate is
low because the registry itself is small, but this is not a rich seam. The decisive test
is against *voters*, which is **[U]** — see Q4.

**The bias runs the opposite way from what you would guess.** The linkable set skews
toward people who had a reason to publish an address — airdrop claimants, delegate
marketing — **not** toward the people who actually shape proposals. The highest-post-count
users are all *unlinkable* by these routes: `mkoeppelmann`, `auryn_macmillan`, `Karpatkey`,
`StefanGeorge`, `john_szczepaniak`, `refri`, `staworth`.

### 4.2 What that means for scope

The identity link does **not** unlock community-opinion analysis, and it only partly
unlocks delegate accountability. Build it as a **tiered side table with reported
coverage**, and never let it feed a headline metric. Concretely:

- Store `(username, address, method, confidence, first_seen)`; one-to-many; conflicts visible.
- Only surface links the person **self-declared publicly** — a pasted address, an ENS
  handle. Never an inferred or correlation-derived one. That is simultaneously the accuracy
  rule and the privacy rule: linking pseudonymous accounts to wallets is deanonymization,
  and self-declaration is the line that keeps it defensible.
- Explicitly rejected: temporal correlation ("posted at T, voted at T+5min"). At this
  population size it is coincidence, and it would poison an otherwise defensible table.
- Every metric derived from it prints "of the linked subset (n=…)".

The one genuinely good use case that survives: **do delegates vote the way they talk?**
For the delegates who *are* linkable, compare stated forum position against cast vote.
Nobody in the ecosystem publishes that.

---

## 5. Text: what we will and will not do

### 5.1 The library question, answered

A library instead of a model is the obvious instinct. It does not survive contact with the
numbers.

| Option | Verdict |
|---|---|
| VADER / TextBlob / AFINN | **No.** Measure *tone*, not *stance toward a target*. "Excited about this, but the funding is far too high and we should reject it" scores positive. Tuned on tweets and reviews, not long hedged prose that quotes other people. Already empirically disproven on this corpus (section 1). |
| scikit-learn (TF-IDF + logistic regression) | **No — economically pointless.** It needs labeled training data, so you hand-label ~400 posts anyway. What does the model then buy? Generalization to future posts. The corpus grows ~80 posts/month, of which maybe 10 carry enough likes to matter. **You would build, validate and maintain a classifier to avoid labeling ten posts a month.** ML solves scale problems; there is no scale problem here. |
| Local transformer (HuggingFace zero-shot) | **No.** Avoids an endpoint but adds a ~2 GB torch dependency to the dbt cron image and is *worse* at nuanced stance than a frontier model. |
| Rule patterns on explicit governance speech acts | **Yes.** "I oppose", "I'd vote against", "NACK", "+1", "seconded", "strongly in favour". High precision, low recall, no labels, no model. Report coverage honestly. |

### 5.2 The mechanism that replaces scoring

**Use behavior to find the text, then show the text.**

Overlay forum post timestamps on the cumulative against-share curve across a voting
window. Where the against-share inflects within hours of a specific post, that post is a
candidate for having moved opinion. Surface it verbatim.

The output is not "sentiment: 0.34". It is *"these three posts preceded an 8% shift in
voting power."* Strictly more useful, needs no NLP, and is checkable by anyone who clicks
through. Label it "posts near an inflection", never "posts that caused" — this is a
coincidence detector, not a causal claim.

### 5.3 Where a model would actually earn its place

Only one place: **likes are the sole signal from people who never voted and never posted**,
and a like is only directional if the liked post's stance is known.

A post with zero likes contributes zero weight to any like-weighted aggregate. So stance
is needed only on the posts that carry the like mass — sized by Q7 **[U]**, expected to be
a few hundred, not 6,843. At that size the choice between an offline batch pass and a
person reading the list once becomes a matter of taste, not architecture. Either way the
output is a **static labeled asset committed as a dbt seed keyed by content hash**, with no
runtime dependency, no API key in production, and full diffability in review.

### 5.4 The free path that already exists

**[V]** `cerebro-mcp`'s `ui/src/mini-apps/governance/model/contextPrompt.ts`
(`buildAskPrompt`, `buildModelContextLines`) already hands proposal bodies, forum markdown
and vote reasons to the **host chat** via `sendMessage` — the "Ask Cerebro" handoff. For
reading a specific contested thread on demand, this works today with no endpoint, no key,
and no infrastructure.

**[V]** There is no NLP or LLM code anywhere in cerebro-mcp. No `anthropic`, `openai`,
`nltk`, `vader`, `spacy`, or embedding library in the dependency tree; the only text
dependency is `rank-bm25` for catalog keyword search.

### 5.5 Preprocessing that must happen regardless

Strip `[quote=...]...[/quote]` blocks from `raw` before any text handling. This resolves
most of the 11% quoted-opinion problem deterministically, with a regex. Also exclude
`hidden` and `user_deleted` posts from engagement metrics — moderated content must not
count as engagement.

---

## 6. Metric design

### 6.1 Tier 1 — Contestation (vote-derived, deterministic, needs nothing new)

- `dissent_share`, `abstain_share`, `margin` — branch on ballot `type`
- **`head_vs_weight_divergence`** = against-share by voter count − against-share by voting
  power. Positive means small holders were more opposed than whales. **The single most
  interesting number in the set**, pure SQL, and no text model can produce anything like it.
- Voting-power Gini / entropy per proposal; `whale_concentration` already exists
- `redo_flag` — GIPs with multiple ballots. The spine already computes `outcome_ambiguous`
  and `decisive_outcomes`.
- Opinion-formation curve; early-vs-late vote split; time-to-quorum. **Nothing like this
  exists today** — `proposal_vote_trend`'s hourly curve in the mini-app is the only
  intra-proposal timing anywhere, and there is no early/late, time-to-quorum, or
  discussion-vs-vote logic. **[V]**

### 6.2 Tier 2 — Deliberation friction (forum timestamps)

- posts and distinct participants, split across the four derived phase windows
- `question_density` — share of posts containing `?` **after** quote-stripping
- reply-depth vs broadcast, via `reply_to_post_number`
- `author_response_share` — does the proposer answer questions
- `discussion_concentration` — Gini of posts per participant: one loud voice vs real debate
- `discussion_lead_time` = `proposal.created_at` − first forum post; rushed proposals
- `silence_flag` — reached a vote with zero forum discussion.
  `int_governance_gip.discussed_on_forum` already computes it.

Confirmed with the user: the forum **is** where GnosisDAO debates, so the silence signal
is valid rather than a measurement artifact of debate happening on Discord.

### 6.3 Tier 3 — Attention and reaction (caveated by 3.2)

Post-level, and genuinely open ground — **[V]** there are *no* normalized engagement
metrics anywhere in the platform today (no likes-per-post, reads-per-view,
participants-per-reply):

- **resonance** = `like_count / reads`
- **provocation** = `reply_count / reads`

Giving a post-level classification: high likes + low replies = consensus statement; low
likes + high replies = lightning rod; high both = the pivotal argument; low both = noise.
The top-right quadrant is the input to the section 5.2 triage.

Both must be percentile-ranked **within year**, not used raw — forum traffic grew over a
decade, so raw ratios are not comparable across eras. Guard denominators with `nullIf`.

Also `views_per_participant` at topic grain — the lurker ratio, salience without
participation.

### 6.4 Tier 4 — Two axes, not one score

- **Salience** — did anyone care: turnout, voters, participants, views, posts
- **Contestation** — how divided: dissent share, margin, head-vs-weight divergence,
  question density

Quadrants: high salience + low contestation = **mandate**; high + high = **conflict**;
low + low = **rubber stamp**; low + high = **fringe dispute**.

Construction rules: percentile-rank **within governance era**, never z-scores (n≈134,
non-normal, and the strategy set genuinely changed across eras — `gno` / `delegation` /
`erc20-balance-of` / `beacon-chain` in different combinations). Any weighting is a value
judgment: make it a versioned dbt var with documented rationale, and always expose the
components beside the composite.

### 6.5 Presentation

**Never a single sentiment number.** A −1..+1 gauge invites exactly the misreading the data
cannot support and discards everything interesting.

**The atomic unit is a per-proposal opinion card:**

1. **Two stacked bars, not one** — result by voting power, and the same result by
   headcount, adjacent. The gap between them *is* the story.
2. **The opinion-formation curve** — cumulative For/Against share across the voting window,
   with forum post markers as ticks. Inflections clickable.
3. **Most-endorsed arguments** — top 3–5 posts by likes, verbatim, with like counts.
   Labeled as what they are, not aggregated into a score. Plus vote reasons verbatim.
4. **A phase strip** — how discussion distributed across the four windows.

**Two overview screens:** the salience × contestation scatter (one dot per GIP over time,
colored by outcome, every dot opens its card); and the divergence table (proposals ranked
by headcount-vs-power gap — bluntly, the "whales versus everyone else" screen).

**One rule that keeps it honest:** every figure carries the population it describes —
"voters (n=1,204)", "forum participants (n=17)", "likers (identity unknown)". The moment
those blend into "the community thinks", it becomes fiction.

---

## 7. Architecture — forced, not chosen

**[V] cerebro-mcp cannot persist anything.** Every ClickHouse client is pinned
`readonly: 1` at session level, and the query-budget mechanism can only make a query
*stricter*. The sole write path is `ScratchStore`, gated by `RPC_SCAN_ENABLED` (which is
`False` in this checkout), and even enabled its `_TABLE_RE` locks table names to
`rpc_(logs|calls|storage|code|traces|blocks|scan_jobs)_*` — so `scratch.governance_*` is
**literally unrepresentable**.

Therefore: **all scoring lives in dbt.** This is not a style preference.

**[V] Render in Metric Lab first, not a new Explorer section.** New `api_governance_*`
marts plus semantic-layer entries are immediately explorable in Metric Lab, whose picker is
sourced from the semantic registry — zero mini-app work. By contrast the Governance
Explorer costs:

- `MAX_RETAINED_SECTIONS = 5` is already saturated (5 sections + an entity pseudo-section),
  so a scope is already being evicted; a sixth tab makes it worse.
- A new chart requires editing **three hardcoded copies** of `SECTION_GROUPS`
  (`governance_explorer.py:115-147`, `ui/src/mini-apps/governance/model/datasetGroups.ts:7-39`,
  and `__tests__/datasetGroups.test.ts:9-30`) plus `devFixture.ts`. A new section is 16
  registry edits across both languages.
- Mini-app SQL is capped at 9,900 chars, single statement, must contain `ORDER BY`, must not
  contain `SETTINGS`, must carry `FINAL` after every `governance_db` table, and must bind all
  user values — all lexically test-enforced. A percentile-rank-within-era score is not
  something to express under those rules and re-execute per page-load behind a 30-minute cache.

Promote the two or three metrics that prove out into the existing `proposals` and `forum`
sections later, rather than opening a new tab.

**[V] Highest-value code to port:** `_classify_choice` and `_choice_warning_scan`
(`governance_explorer.py:475-542`) already resolve polymorphic Snapshot `choice` into
`single` / `ranked` / `unsupported` with index validation — but in mini-app Python. dbt has
only unparsed `choice_raw`. Every Tier 1 direction metric depends on this, and
re-deriving it independently is how the two surfaces end up disagreeing.

---

## 8. Diagnostic queries — run these before Phase 1

**ALL RUN 2026-07-28 against `playground_max`. Results and verdicts below.** [V]
Substitute `{{db}}` = `playground_max`. Re-run after any fresh ingest.

### Results summary — every design gate passed

| # | Question | Result | Verdict |
|---|---|---|---|
| Q1 | Is the opinion-formation curve viable? | 210 closed proposals with votes; median window **168h**, median **100 votes**, median **50 distinct active hours** (30.3% of hours have a vote); **36.2%** of votes in first 24h, only **4.8%** in last 24h; 142 proposals with 50+ votes, 70 with 200+ | **PASS — build it.** Front-loaded but far from degenerate. Restrict inflection detection to the 142 proposals with 50+ votes. |
| Q2 | Declared phase tag coverage | Overall sparse: `none` 693 (78.5%), `phase-2` 80, `phase-1` 75, `phase-3` 35. **But of 158 GIP-titled topics, 120 (76%) carry a tag**, and `phase-3` is 35-of-35 GIP | **Usable as a secondary dimension.** Derived timestamp windows stay primary (universal); declared phase is a good extra on GIP topics. |
| Q3 | Address extraction from post bodies | **1,285 posts (18.8%) contain an address**, 272 authors, 679 distinct addresses — but only **7 posts / 6 authors** use first-person framing | **ROUTE DEAD.** Addresses are everywhere and almost never the poster's own (contracts, counterparties, exploit addresses). Near-zero precision for identity. |
| Q4 | Do the 62 identity candidates vote? | 6,341 distinct voters total. **24 of 62 candidates voted**, 280 votes — but **0.165% of all voting power** | **Mechanism works, weight negligible.** Keep as a label, not an analysis tier. See 4.3. |
| Q5 | Ballot type distribution | **`basic` 245 (96.8%)**, `single-choice` 7, `ranked-choice` 1. GIP proposals: 135 | **Polymorphism worry is nearly moot.** One dissent-share definition covers 96.8%; handle or exclude the single ranked-choice proposal. |
| Q6 | Forum-to-proposal bridge coverage | 253 closed proposals: **94 carry a forum URL, 89 parseable**, 154 have empty `discussion`. Post-link bridge adds more (`int_governance_forum_topic_proposal_links` = 200) | **Report coverage.** Phase-resolved deliberation covers roughly 35–79% of proposals depending on method, not all. |
| Q7 | How concentrated are likes? | 6,847 posts / **14,177 likes**; 2,286 (33%) have zero; p50=1, p90=5, p99=13, max=70. **880 posts with 5+ likes hold 49.2% of all likes**; 160 posts have 10+ | **Labeling is tractable.** 160 posts is an afternoon; 880 covers half the like mass. No model needed. |
| Q8 | Vote reason coverage | Rising steadily: 2022 0.03% → 2023 0.69% → 2024 2.44% → 2025 6.99% → **2026 7.79%**; 342 total | **Use verbatim, never aggregated.** Confirms the trend. |

### Q8 also surfaced the most important number in the dataset

Vote volume by year: **2022: 31,157 → 2023: 9,567 → 2024: 3,604 → 2025: 2,117 → 2026 YTD: 398.**

That is a **~98.7% collapse in voting participation from the 2022 peak.** 2022 was the
airdrop-farming era (SNAFU, SAFE distribution) so the peak is inflated, but the trend since
is monotonic and steep. Whatever else this project ships, *this* is the governance-health
headline, and it comes out of a single `GROUP BY toYear(created_at)`. It should be on the
first screen, and it reframes the whole exercise: the question is less "how do people feel
about proposals" than "why has almost everyone stopped voting."

### The queries

Note: ClickHouse WITH-clause scoping breaks when a CTE is referenced more than once in a
complex query. Each CTE below is referenced exactly once.

### Q1 — Is the opinion-formation curve viable? (highest risk in Phase 1)

If most votes land in one cluster, the curve degenerates into a step function and the
inflection idea in 5.2 dies.

```sql
WITH prop AS (
    SELECT id, start_at, end_at
    FROM {{db}}.snapshot_proposals FINAL
    WHERE state = 'closed'
      AND end_at > start_at
      AND start_at > toDateTime('2021-01-01 00:00:00', 'UTC')
),
vote AS (
    SELECT proposal_id, created_at
    FROM {{db}}.snapshot_votes FINAL
),
pv AS (
    SELECT
        p.id                                                    AS proposal_id,
        dateDiff('hour', p.start_at, p.end_at)                  AS window_hours,
        count()                                                 AS votes,
        uniqExact(toStartOfHour(v.created_at))                   AS active_hours,
        countIf(v.created_at <  p.start_at + toIntervalHour(24)) AS votes_first_24h,
        countIf(v.created_at >= p.end_at   - toIntervalHour(24)) AS votes_last_24h
    FROM prop AS p
    INNER JOIN vote AS v ON v.proposal_id = p.id
    GROUP BY p.id, p.start_at, p.end_at
)
SELECT
    count()                                                  AS proposals,
    round(median(window_hours), 1)                           AS median_window_hours,
    round(median(votes), 1)                                  AS median_votes,
    round(median(active_hours), 1)                           AS median_active_hours,
    round(median(active_hours / nullIf(window_hours, 0)), 3)  AS median_share_hours_active,
    round(median(votes_first_24h / nullIf(votes, 0)), 3)      AS median_share_first_24h,
    round(median(votes_last_24h  / nullIf(votes, 0)), 3)      AS median_share_last_24h,
    countIf(votes >= 50)                                     AS proposals_50plus_votes
FROM pv
```

**Decision rule:** if `median_share_hours_active` is very low and
`median_share_first_24h` is above ~0.8, drop the curve from Phase 1 and keep only the
early-vs-late split.

### Q2 — Declared phase tag coverage

```sql
SELECT
    phase,
    topics,
    gip_topics,
    round(100.0 * topics / (SELECT count() FROM {{db}}.forum_topics FINAL), 1) AS pct_of_all
FROM (
    SELECT
        multiIf(
            position(tags, 'phase-3') > 0, 'phase-3',
            position(tags, 'phase-2') > 0, 'phase-2',
            position(tags, 'phase-1') > 0, 'phase-1',
            'none'
        )                                                                        AS phase,
        count()                                                                  AS topics,
        countIf(toInt32OrNull(extract(title, '(?i)\\bGIP[\\s-]?0*([0-9]+)')) > 0) AS gip_topics
    FROM {{db}}.forum_topics FINAL
    GROUP BY phase
)
ORDER BY topics DESC
```

**Decision rule:** if `none` dominates, rely purely on derived timestamp windows and treat
the declared phases as a nice-to-have dimension.

### Q3 — Address extraction from post bodies (identity route 3)

```sql
SELECT
    countIf(match(raw, '0x[a-fA-F0-9]{40}'))                          AS posts_with_address,
    uniqExactIf(user_id, match(raw, '0x[a-fA-F0-9]{40}'))             AS distinct_authors,
    countIf(
        match(raw, '0x[a-fA-F0-9]{40}')
        AND match(lower(raw), '(my (address|wallet|safe)|send to me|payout address|receiving address)')
    )                                                                 AS posts_first_person,
    uniqExactIf(
        user_id,
        match(raw, '0x[a-fA-F0-9]{40}')
        AND match(lower(raw), '(my (address|wallet|safe)|send to me|payout address|receiving address)')
    )                                                                 AS first_person_authors
FROM {{db}}.forum_posts FINAL
```

### Q4 — Do the 62 identity candidates actually vote? (decides section 4's value)

Candidate list is in
`scratchpad/identity_candidates.json` from the 2026-07-28 probe.

```sql
SELECT
    uniqExact(lower(voter))                                     AS distinct_voters_total,
    uniqExactIf(lower(voter), lower(voter) IN ({{candidates}}))  AS candidates_that_voted,
    countIf(lower(voter) IN ({{candidates}}))                    AS votes_by_candidates,
    round(sumIf(vp, lower(voter) IN ({{candidates}})) / nullIf(sum(vp), 0), 5) AS candidate_vp_share
FROM {{db}}.snapshot_votes FINAL
```

**Decision rule:** if `candidates_that_voted` is in single digits and `candidate_vp_share`
is negligible, drop identity linking from the plan entirely rather than carrying a tier
that cannot support a metric.

### Q5 — Ballot type distribution (how much of the corpus supports a simple dissent share)

```sql
SELECT
    type,
    count()                       AS proposals,
    countIf(state = 'closed')     AS closed,
    countIf(gip_number > 0)       AS gip_proposals
FROM (
    SELECT
        type,
        state,
        toInt32OrNull(extract(title, '(?i)\\bGIP[\\s-]?0*([0-9]+)')) AS gip_number
    FROM {{db}}.snapshot_proposals FINAL
)
GROUP BY type
ORDER BY proposals DESC
```

### Q6 — Forum-to-proposal bridge coverage (how many proposals get phase analysis at all)

```sql
SELECT
    count()                                                   AS closed_proposals,
    countIf(discussion LIKE '%forum.gnosis.io%')              AS with_forum_discussion_url,
    countIf(toUInt32OrNull(extract(splitByChar('?', discussion)[1], '/([0-9]+)/?$')) > 0)
                                                              AS parseable_topic_id
FROM {{db}}.snapshot_proposals FINAL
WHERE state = 'closed'
```

### Q7 — How concentrated are likes? (sizes the Phase 3 labeling job)

The most decision-relevant query here. If a few hundred posts hold most of the likes,
hand-labeling is trivially viable.

```sql
SELECT
    count()                                        AS posts,
    sum(like_count)                                AS total_likes,
    countIf(like_count = 0)                        AS zero_like_posts,
    quantilesExact(0.5, 0.9, 0.99)(like_count)     AS p50_p90_p99,
    max(like_count)                                AS max_likes,
    countIf(like_count >= 5)                       AS posts_5plus_likes,
    countIf(like_count >= 10)                      AS posts_10plus_likes
FROM {{db}}.forum_posts FINAL
```

Then the exact size of the 80%-of-likes set:

```sql
SELECT
    countIf(running <= 0.8 * grand) AS posts_holding_80pct_of_likes,
    grand                           AS total_likes
FROM (
    SELECT
        sum(like_count) OVER (
            ORDER BY like_count DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                     AS running,
        (SELECT sum(like_count) FROM {{db}}.forum_posts FINAL) AS grand
    FROM {{db}}.forum_posts FINAL
)
```

### Q8 — Vote reason coverage trend

```sql
SELECT
    toYear(created_at)                                                   AS yr,
    count()                                                              AS votes,
    countIf(JSONExtractString(raw_json, 'reason') != '')                  AS with_reason,
    round(100.0 * countIf(JSONExtractString(raw_json, 'reason') != '') / count(), 2) AS pct
FROM {{db}}.snapshot_votes FINAL
GROUP BY yr
ORDER BY yr
```

---

## 9. Phased plan

### Phase 0 — Unblock. No new modelling.

1. Run the click-runner governance ingestors with `GOVERNANCE_DATABASE=playground_max`.
2. Set `DBT_GOVERNANCE_SCHEMA=playground_max`; rebuild the existing 38 files
   (`--target ch_dbt`).
3. Repoint the orphaned delegations lineage at `rpc_log_indexer.v_delegate_events`,
   deciding the chain_id question from 2.4.
4. **Start the counter-history model now**, before it is needed: a dbt incremental keyed
   on `(post_id, snapshot_date)`, appending current `like_count` and `reads` after each
   ingest, idempotent so a second run in one day does not double-write. It records "last
   known value", so a dormant topic's row simply repeats — fine and honest.
   **This cannot be backfilled. Every day of delay is history permanently lost.**
5. Extract the free `raw_json` columns in staging: post-level `readers_count`, `score`,
   `incoming_link_count`, `percent_rank`, `hidden`, `user_deleted`, `trust_level`. No
   ingestion change needed. `hidden` and `user_deleted` are required for 5.5 hygiene.
6. Run Q1–Q8 and record the answers in this document.

### Phase 1 — The core. Zero text.

Port the choice classifier into dbt, then build Tier 1 and Tier 2 (sections 6.1, 6.2),
scoped to `is_gip = 1` per the earlier turnout decision. Publish as `api_governance_*`
marts plus semantic entries so they surface in Metric Lab with no mini-app work.

Gate: if Q1 says vote timestamps are too clustered, ship the early-vs-late split and drop
the curve.

### Phase 2 — Triage. Still zero NLP.

Tier 3 (section 6.3). Rank posts by likes, reads, replies, and proximity to vote
inflections. Ship the "most endorsed arguments" panel — top posts verbatim with like
counts, plus vote reasons. **This is the part that actually answers "what do people
think", and it does it by quoting them rather than scoring them.**

### Phase 3 — Stance. One-time, no model to maintain.

Rule patterns per 5.1 with reported coverage, plus hand-labels on the high-like set sized
by Q7. Committed as a dbt seed keyed by content hash. Then like-weighted opinion —
including from people who never voted — becomes available.

Identity linking (section 4) lands here too, gated on Q4.

### Phase 4 — Surfaces. Only if 1–3 earn it.

Governance Explorer section, dashboard section.

### Explicitly not doing

A sentiment score. An LLM endpoint. A transformers or NLP dependency. Lexicon scoring
under any name. Correlation-inferred identity links.

### The one ingestion change worth making

**[V] The likes graph is obtainable and cheap.** `/user_actions.json?username=X&filter=1`
is anonymously readable and validated exactly: for `mkoeppelmann`, `john_szczepaniak` and
`auryn_macmillan` the fetched row count equals `directory_items.likes_given` precisely
(121=121, 284=284, 208=208), with one row per liked post and a single distinct
`acting_username`. Each row carries `post_id`, `topic_id`, `post_number`, `created_at`,
`hidden`, `deleted`. (`filter=2` returns 404 — not needed; iterating `filter=1` over users
yields the whole graph.)

Sizing: across 600 directory users, `sum(likes_given) = 9,702` and
`sum(likes_received) = 12,246` (the gap implies likes from users outside the sample), with
446 users having at least one like given. At 30 rows per page that is **324 pages, roughly
2.7 minutes of crawling at the ingestor's existing 2 req/s**.

This buys **like timestamps and liker identity** — which converts likes from a frozen,
unattributable scalar into a real time series with actors, and is the only signal from
silent participants. Best value-per-effort item in the whole design. Schedule it in Phase 2.

Also cheap and worth adding while touching the ingestor: `/u/{username}.json` exposes
`created_at` (join date), `time_read`, `profile_view_count` and `badge_count`, all
currently dropped. `time_read` is a genuine engagement signal — one sampled user had
588,381 seconds (163 hours) of read time. **[V]**

---

## 10. Validation plan

- **Face validity.** GIP-150 / 151 / 152 (the contentious treasury-redemption cluster;
  GIP-150 alone had 50 posts from 17 participants) must land in "conflict". Redo GIPs
  (74 / 87 / 91 / 134) must show high contestation on the failed attempt. If they do not,
  the metric is wrong.
- **Negative control.** The ~119 non-GIP spam/phishing proposals must land in "rubber
  stamp" or be excluded. Reuse `is_gip = 1`, already validated as removing noise without
  moving GIP-1 / 71 / 151 spot turnout values (6.79% / 3.87% / 4.86%).
- **Hand-labeled holdout.** ~20 proposals rated contested/uncontested by someone who was
  there, checked by rank correlation. The only basis for claiming the index means anything.
- **Fixtures.** Use the eight largest topics listed in 2.5 as regression fixtures for the
  phase-bucketing and resonance logic.

---

## 11. Open questions

### Resolved since first draft — no longer open

- ~~Whether to pursue a `governance_db` read grant~~ — **deprioritized.** The
  `playground_max` copy is complete and readable; prod has no governance models to compare
  against anyway (section 2.3).
- ~~Does identity linking stay in the plan?~~ — **decided by Q4.** 24 of 62 candidates vote,
  but they hold 0.165% of voting power, and Q3 killed post-body extraction (7 first-person
  posts out of 1,285 containing an address). **Identity drops out as an analysis tier and
  survives only as a ~24-60 row label seed.** All three routes are now measured; no further
  research is warranted.
- ~~Is the opinion-formation curve viable?~~ — **yes, Q1 passed.**
- ~~Is the forum the real venue for debate, or is it Discord?~~ — **confirmed by Max: the
  forum is correct.** The `silence_flag` metric is therefore valid.
- ~~Is ballot polymorphism a problem?~~ — **no, Q5: 96.8% are plain `basic`.**
- ~~Do chain-100 delegations count for gnosis.eth?~~ — **YES, verified against the live
  Snapshot config (section 2.4.1).** `delegation@100` has been an active strategy on 231
  proposals since 2022-03-29. Include both chains; dedupe counts by address (28 overlap),
  sum power per chain. The repoint is now fully unblocked.

### Genuinely open — these need a human decision

**1. Does the participation collapse become the lead question?** (new, from Q8)
Votes per year: 31,157 (2022) → 9,567 → 3,604 → 2,117 → 398 (2026 YTD), a ~98.7% decline
from a peak inflated by airdrop farming but monotonic since. This is the loudest signal in
the dataset and it is *not* what the project set out to measure. If it becomes the lead,
the phases reorder: participation decomposition (who stopped voting — whales, retail,
delegates?) moves ahead of per-proposal contestation. **Decision owner: Max.** This is the
single most consequential open item.

**2. Branch hygiene, now that we are about to add code.** (new)
The entire governance layer lives on `feat/celo-native`, mixed with unrelated Celo work.
Adding contestation models piles more onto that branch. Previously Max accepted this "until
everything is done", but the calculus changes once new modelling starts. Options: carve a
`feat/governance-consensus` branch off the governance subtree, or keep accumulating and
split at deployment time. **Decision owner: Max**, coordinating with whoever deploys.

**3. Does the composite weighting in 6.4 need governance-stakeholder sign-off?**
It encodes a judgment about what "contested" means. Publishing a number that ranks
proposals by contestation is a mildly political act. Worth deciding whether that ships as
an analytics artifact or needs review first.

### Tracked separately, not a question

Repointing the orphaned delegations lineage at `rpc_log_indexer.v_delegate_events` is a
**confirmed action item**, not an open question — see Phase 0 step 3. Its only unresolved
input is open question 2 above (the chain filter).
