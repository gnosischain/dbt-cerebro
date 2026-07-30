# Incident → Lesson

Turn a just-diagnosed incident or mistake class into a durable lesson record so no
agent repeats it. Input: short description of what happened.

This is the learning loop that keeps `docs/lessons/` alive — run it at the END of any
investigation that uncovered a new failure class (not for one-off typos).

## Procedure

1. **Check it's genuinely new**: read `docs/lessons/INDEX.md`. If an existing lesson
   covers the class, UPDATE that lesson (new evidence, status change, widened scope)
   instead of creating a near-duplicate.

2. **Gather evidence FIRST** (a lesson without evidence is a rumor):
   - commit SHAs (`git log -S`, `git log --oneline -- <path>`) with dates + subjects
   - file paths with line refs for the bug locus and the fix
   - query IDs / incident docs / test names that prove the symptom
   Claims that can't be evidenced get `status: observed` with an explicit note.

3. **Write `docs/lessons/<kebab-id>.md`** with this exact frontmatter shape (YAML —
   quote list items containing ": ", use `>-` for multi-line scalars):

   ```markdown
   ---
   id: <kebab-id, must equal filename>
   title: <one sentence>
   status: observed | remediated | enforced | proposed
   scope: <which model classes / paths it bites>
   symptom: <what you see when it fires>
   last_verified: <today, YYYY-MM-DD>
   evidence:
     - <commit / path:line / doc / query id>
   ---
   ## Symptom
   ## Root cause
   ## Forbidden action
   ## Detection
   ## Safe remediation
   ## Ground truth
   ## Enforcement
   ```

   Status semantics: `observed` = seen, no safeguard; `remediated` = instance fixed,
   recurrence possible; `enforced` = a gate/test/code fix prevents recurrence;
   `proposed` = idea only.

   **Status describes the DEPLOYED state, never the working tree.** Production runs
   the CI-built image from merged main — a model/script fix that isn't merged yet is
   at most `observed` with an explicit "fix in tree, pending deploy" evidence line.
   Only flip to `remediated` after the merge deploys, and to `enforced` after the
   safeguard has demonstrably fired in a production run.

4. **Index it**: add a one-liner to `docs/lessons/INDEX.md` under the right section.

5. **Wire it in** (as applicable):
   - profile-level: add the lesson id to `hazards` of the matching profile in
     `agent_context/profiles.yml`
   - model-level: add to `meta.agent.hazards` of specifically-affected models
   - detection: add a singular test under `tests/data_quality/` (tag
     `data_quality_daily` or `data_quality_weekly`) — a lesson with a test can reach
     `enforced`
   - then `python scripts/agent_context/build_agent_context.py` must pass (it
     validates every hazard reference).

6. **Verify**: `python scripts/agent_context/context.py --select <affected-model>`
   shows the new hazard.
