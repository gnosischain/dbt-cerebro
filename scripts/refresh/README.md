# Refresh Helpers

This folder contains helpers for running large dbt refreshes in ways that are easier on ClickHouse while still letting dbt's graph stay the source of truth.

## Files

- `dbt_run_batches.py`: splits a normal incremental `dbt run --select ...` into ordered batches measured in lineage chains

## Why This Exists

The daily production job used to run:

```bash
dbt run --select tag:production
```

That is simple, but it can put too much memory pressure on ClickHouse when too many heavy models run in one invocation.

We wanted a batching strategy that:

- does not hardcode model names, folders, or batch order
- uses dbt lineage instead of manual maintenance
- does not rerun the same selected incremental model multiple times
- is easy to tune with one knob

`dbt_run_batches.py` is the small planner that provides that behavior.

## High-Level Behavior

Given a selector such as `tag:production`, the helper:

1. runs `dbt parse` to refresh `target/manifest.json`
2. runs `dbt ls` to get the current selected model set
3. builds the selected subgraph from the manifest
4. topologically orders that graph
5. peels the graph into runnable chains
6. groups those chains into batches
7. prints one batch per line for the shell runner

The shell script then executes:

```bash
dbt run --select "<batch selector>"
```

for each generated batch.

If needed, the shell runner can also pause between those generated `dbt run` invocations by setting:

```bash
DBT_RUN_BATCH_SLEEP_SECONDS
```

## Algorithm Discussion

This section explains exactly how path finding works.

The planner does not try to find mathematically longest paths, cheapest paths, or domain-specific paths.

Instead, it uses a deterministic greedy algorithm that is easy to reason about and follows dbt lineage directly.

### Step 1: Build The Selected-Only Graph

The helper first asks dbt which models are in scope for the selector.

For example, if the selector is:

```bash
tag:production
```

then only models selected by `tag:production` are considered part of the batching graph.

This matters because the planner is not trying to schedule the entire project DAG.

It is only trying to schedule the selected subgraph.

That means:

- unselected parents are not emitted as part of a batch
- dependency checks only apply to selected parents
- the generated batches preserve the semantics of `dbt run --select <same selector>`

### Step 2: Topologically Sort The Selected Graph

After the selected graph is built, the helper performs a standard topological sort.

Conceptually:

- models with no selected parents are runnable first
- once a model is considered scheduled, its dependents may become runnable
- this continues until all selected models are ordered

This is the same core dependency rule dbt itself follows:

- parents before children

When more than one node is runnable at the same time, the helper breaks ties deterministically using:

1. `original_file_path`
2. model name

This keeps the order stable across runs for the same manifest.

### Step 3: Peel The Graph Into Chains

Once there is a stable topological order, the helper converts that order into chains.

It maintains two sets:

- `done`: models already assigned to earlier chains
- `remaining`: models not yet assigned

Then it repeats:

1. scan the topological order from the start
2. pick the first model that is still in `remaining` and whose selected parents are already in `done`
3. start a new chain from that model
4. keep extending that chain through one runnable child at a time
5. stop when there is no runnable child left
6. mark the whole chain as `done`
7. repeat until `remaining` is empty

That is the key rule:

- take the next runnable start
- follow one path
- come back later for the leftover branches

### Step 4: Choose One Child When A Branch Appears

Suppose the current model has multiple runnable children.

The helper does not try to take all of them in the same chain.

Instead, it chooses exactly one child and keeps walking forward.

The chosen child is:

- the runnable child with the smallest topological index

That means the path choice is deterministic and consistent with the earlier topological ordering.

So if a node branches like this:

```text
stg_a
  -> int_b -> api_x
  -> int_c -> api_y
```

the planner will do something like:

```text
chain 1: stg_a -> int_b -> api_x
chain 2: int_c -> api_y
```

assuming `int_b` comes before `int_c` in the topological order.

Notice that `chain 2` starts at `int_c`, not `stg_a`.

That is not a bug.

It means the shared parent `stg_a` was already handled in `chain 1`.

### Why The Algorithm Works This Way

The batching problem has a real tension in branching DAGs.

In general, you cannot have all three of these at once:

- every batch is a full root-to-leaf path
- shared upstream models are never repeated
- batching is derived automatically from the dbt graph with no hardcoded grouping

Once many downstream models share the same upstream nodes, one of those goals has to give.

This helper chooses:

- automatic graph-driven batching
- no repeated selected models
- one path now, remaining sibling paths later

That is why some chains are "complete" from the current runnable point onward, but not always from the original source root.

### Pseudocode

The behavior can be summarized like this:

```text
ordered = topological_sort(selected_graph)
done = {}
remaining = set(ordered)
chains = []

while remaining is not empty:
    start = first node in ordered that is in remaining and whose selected parents are in done
    chain = [start]

    current = start
    while current has a runnable child still in remaining:
        current = runnable child with smallest topo index
        chain.append(current)

    chains.append(chain)
    done += chain
    remaining -= chain
```

### Worked Example

Imagine the selected graph looks like this:

```text
stg_prices
  -> int_token_prices
      -> api_token_prices_daily
      -> api_token_prices_latest
  -> int_price_quality
      -> api_price_quality_daily
```

One possible result is:

```text
chain 1: stg_prices -> int_token_prices -> api_token_prices_daily
chain 2: api_token_prices_latest
chain 3: int_price_quality -> api_price_quality_daily
```

Why does this happen?

- `stg_prices` is the first runnable selected node
- `int_token_prices` is one runnable child path
- `api_token_prices_daily` is chosen before `api_token_prices_latest` by deterministic order
- after `chain 1` is done, the leftover sibling `api_token_prices_latest` is already runnable
- the other branch `int_price_quality -> api_price_quality_daily` is also handled later

This is exactly the intended behavior:

- choose one path if there are many
- run the remaining paths after

### Why Some Chains Are Very Short

A chain may have length 1 for valid reasons.

Examples:

- a selected model has no selected children
- a sibling branch was left behind after a shared parent already ran
- the model is a terminal selected node whose parents were already handled

So output like:

```text
api_consensus_blocks_daily
```

as a one-model chain can be perfectly correct.

It means:

- all of its selected parents were already satisfied earlier
- it is now the next not-yet-run path fragment in the graph

### Why Some Chains Start At `api_*`

This is the same idea in a more surprising form.

If a model's selected parents were already consumed in an earlier chain, then that model can become the start of a later chain even if it lives in a "downstream" layer.

So a later chain can begin at:

- `int_*`
- `fct_*`
- `api_*`

without violating lineage.

The rule is not:

- every chain must begin at a source-most model

The real rule is:

- every chain begins at the first not-yet-assigned selected node whose selected parents are already satisfied

### Determinism

The algorithm is intentionally deterministic.

For the same:

- selector
- manifest
- file layout

it will generate the same chains and the same batches.

That makes it easier to:

- inspect preview output
- reason about production behavior
- compare one run plan to another

### Why Not Use Longest Path Or Connected Components

Those options were considered and rejected for this use case.

Longest-path style batching tends to be harder to reason about operationally and can produce surprising jumps in ordering.

Connected-component batching keeps shared subgraphs together, but it can create very large batches that are not "one path at a time" in any intuitive sense.

The current greedy path-peeling approach is simpler:

- it respects lineage
- it is deterministic
- it avoids model repetition
- it matches the desired operator mental model

### Operational Interpretation

When reading preview output, think of each chain as:

- the next runnable path fragment through the selected dbt graph

not as:

- a guaranteed source-to-terminal lineage covering every shared parent every time

That distinction is important.

It explains why the output can still be correct even when:

- some chains are short
- some chains start in downstream layers
- some terminal models show up on their own later

## What A "Chain" Means Here

A chain is one deterministic runnable path through the selected dbt graph.

When the selected graph branches, the planner:

- picks the next runnable node in topological order
- walks one runnable child path
- stops when it reaches a leaf or a point with no further runnable child
- leaves the remaining sibling branches for later chains

This matches the operating rule:

- choose one path if there are many
- run the remaining paths after

## Important Consequence

Some later chains will start at `int_*`, `fct_*`, or even `api_*` models instead of `stg_*`.

That is expected.

It means the shared upstream part of that lineage was already executed by an earlier chain, so the remaining branch can continue from the first not-yet-run selected model.

This is the key tradeoff that lets us keep both of these properties:

- batches are measured in lineage terms
- each selected model runs at most once

In a branching DAG, you cannot have every batch be a full root-to-leaf chain and also avoid repeating shared upstream models. This helper chooses:

- one path first
- sibling paths later
- no repeated selected models

## Batch Size

`--batch-size` means:

- maximum number of chains per generated batch

It does not mean models per batch.

Examples:

- `--batch-size 1`: one chain per `dbt run`
- `--batch-size 5`: five chains per `dbt run`

The observability job uses the environment variable:

```bash
DBT_RUN_BATCH_SIZE
```

The shell runner also supports:

```bash
DBT_RUN_BATCH_SLEEP_SECONDS
```

This adds a sleep between generated `dbt run` batches, which can help give ClickHouse merges or memory pressure time to settle.

## Commands

Preview the generated batches:

```bash
python scripts/refresh/dbt_run_batches.py \
  --select tag:production \
  --batch-size 5 \
  --project-dir . \
  --profiles-dir ~/.dbt \
  --preview
```

Generate machine-readable TSV output:

```bash
python scripts/refresh/dbt_run_batches.py \
  --select tag:production \
  --batch-size 5 \
  --project-dir . \
  --profiles-dir ~/.dbt
```

Output format in non-preview mode:

```text
<batch_id>\t<model_count>\t<chain_count>\t<space-separated model selector>
```

Example:

```text
001    11    5    stg_x int_x api_x stg_y int_y ...
```

## Preview Format

Preview mode is meant for humans. It prints chain boundaries explicitly.

Example:

```text
001 (11 model(s), 5 chain(s)):
  1. stg_x -> int_x -> api_x
  2. stg_y -> int_y
  3. api_shared_child
```

This is much easier to inspect than a single flat selector line.

## Integration With `run_dbt_observability.sh`

The daily job uses this helper in:

- `scripts/run_dbt_observability.sh`

The flow is:

1. generate the run plan with `scripts/refresh/dbt_run_batches.py`
2. store it in a temp file
3. execute one `dbt run` per generated batch
4. record each batch as `dbt-run:<batch_id>`
5. treat `dbt-run` as a batched mandatory step in the final summary

Relevant environment variable:

```bash
DBT_RUN_BATCH_SIZE="${DBT_RUN_BATCH_SIZE:-5}"
```

Optional pause between batches:

```bash
DBT_RUN_BATCH_SLEEP_SECONDS="${DBT_RUN_BATCH_SLEEP_SECONDS:-0}"
```

If the value is greater than `0`, the runner sleeps between generated `dbt-run` batches, but not after the final batch.

## Guarantees

For the selected model set, the planner is designed to guarantee:

- deterministic batch generation
- dependency-respecting execution order
- no repeated selected models across generated chains
- no hardcoded lineage grouping

More concretely:

- a model is never scheduled before its selected parents are satisfied
- each selected model appears in exactly one generated chain
- batch order is stable for the same manifest and selector

## What It Does Not Try To Do

This helper does not:

- full-refresh models
- batch by time ranges or dates
- look at model cost, size, or runtime
- hardcode domain-specific ordering

Those concerns belong elsewhere:

- full refresh orchestration lives in `scripts/full_refresh/`
- chain batching here is only for the normal incremental production run

## Maintenance

This helper is intended to be low-maintenance.

In normal use, adding new models requires no code change.

Why:

- `dbt ls` discovers the current selector membership
- `manifest.json` provides the lineage
- batching is derived from the graph at runtime

The only normal operational tuning is changing:

```bash
DBT_RUN_BATCH_SIZE
```

Use a smaller number when ClickHouse is under memory pressure.

Use a larger number when the system can handle more work per invocation.

You can also increase:

```bash
DBT_RUN_BATCH_SLEEP_SECONDS
```

when you want a cooldown between invocations without changing the batch plan itself.

## Troubleshooting

### Preview Looks Strange

A chain can legitimately begin at `int_*`, `fct_*`, or `api_*`.

That usually means its shared upstream parent chain already ran earlier.

This is expected and does not mean lineage is being ignored.

### Too Many Tiny Batches

Increase:

```bash
DBT_RUN_BATCH_SIZE
```

### ClickHouse Needs A Cooldown Between Batches

Set:

```bash
DBT_RUN_BATCH_SLEEP_SECONDS
```

Example:

```bash
DBT_RUN_BATCH_SLEEP_SECONDS=30
```

This keeps the same generated batch plan but inserts a 30-second pause between consecutive `dbt run` batch executions.

### A Batch Fails But The Script Keeps Going

That is intentional.

`run_dbt_observability.sh` is designed to finish all steps and then decide final exit status from the mandatory-step summary.

### `dbt-run` Marked As Failed Even Though Some Batches Passed

Also intentional.

`dbt-run` is treated as a batched mandatory step. If any `dbt-run:<batch_id>` fails, the overall `dbt-run` mandatory check fails.

## Related Paths

- `scripts/refresh/dbt_run_batches.py`
- `scripts/run_dbt_observability.sh`
- `scripts/full_refresh/refresh.py`
- `scripts/full_refresh/README.md`
