---
id: classifier-unsafe-default
title: A classifier's ELSE branch must not assert a category — unmatched input is 'Unknown'
status: remediated
scope: any model deriving a category from free-text with a pattern-match chain
  (multiIf/CASE over org/name/label strings), and every consumer that turns that
  category into a claim
symptom: a bucket is far larger than reality and nobody notices, because the ELSE
  branch silently assigns it to every input the rules failed to match
last_verified: 2026-08-05
evidence:
  - models/p2p/staging/stg_crawlers_data__ipinfo.sql — generic_provider multiIf; ELSE was 'Public ISP (Home/Office)', now 'Unknown'
  - 'measured before fix: 10,162 of 15,557 IPs labelled Public ISP (Home/Office) matched no residential pattern at all; 651 were AS51167 Contabo GmbH, 174 M247, 108+38 Datacamp'
  - 'HOPR blast radius: int_hopr_nodes reported 545 of 1,020 dufour nodes residential; after fix 157, with 751 datacenter-hosted — a ~3.5x overstatement of home hosting'
  - 'consumer that converts it to a claim: models/ESG/intermediate/int_esg_node_classification.sql:57 maps the bucket to node_category=''home_staker'' at 0.80 confidence'
  - 'post-fix distribution over crawlers_data.ipinfo (22,177 rows): Unknown 872 (3.9%), three transitions only, all out of Public ISP — 1,802 to Hosting/CDN (Other), 863 to Unknown, 110 to Oracle Cloud'
---

## Symptom
A category column looks well-populated and plausible, and one bucket quietly holds the
majority of rows. Nothing errors, no test fails, row counts are stable. The bucket is
inflated by everything the pattern rules did not recognise.

## Root cause
The terminal branch of the pattern chain named a real category instead of an unknown:

    lowerUTF8(org) ILIKE '%broadband%' OR ..., 'Public ISP (Home/Office)',
    lowerUTF8(org) = '', 'Unknown',
    'Public ISP (Home/Office)'          -- <-- absence of evidence becomes a claim

Two failure modes compound:
- **The default is a positive assertion.** "I could not classify this" is rendered
  indistinguishable from "I classified this as residential". The `'Unknown'` bucket
  existed but only fired on an empty org string — 9 rows out of 22,177 — so the honest
  answer was unreachable in practice.
- **The manufactured value was the flattering one.** Downstream this became
  `home_staker`, i.e. the classifier's own coverage gaps read as evidence of
  decentralisation. An unsafe default that produced an *unwelcome* number would have
  been investigated years earlier; this one confirmed what readers hoped.

Coverage gaps are inevitable in string matching and are not themselves the bug. The bug
is resolving them into a category.

## Forbidden action
Do not fix this downstream. Overriding the bucket in one consumer (the HOPR model, in
this case) leaves the defect live for every other consumer and makes two models disagree
about the same IP with nothing marking either authoritative. Fix the column where it is
defined.

Do not add a new top-level bucket name as part of the fix without checking consumers:
`int_esg_node_classification` enumerates bucket names explicitly and falls through to
`node_category='unknown'`, so introducing e.g. a `'Contabo'` bucket would make a
*correctly identified* datacenter classify worse than an unrecognised one. Route
additional providers into an existing bucket the consumers already understand.

## Safe remediation / convention
1. Widen both sides of the rule set first — the hosting list *and* the residential list.
   Flipping the fallback alone just trades over-claiming one category for over-claiming
   another (here it would have demoted Starlink and 1&1 Versatel, both genuine consumer
   ISPs, to Unknown).
2. Then make the terminal branch `'Unknown'`, and confirm consumers have a branch for it
   (ESG already scored `'Unknown'` at 0.30 confidence).
3. Leave the residue unknown. 4.0% unclassified is a true statement about the data;
   widen the lists over time, never narrow the fallback.

## Detection
Measure how much of a bucket got there by fallthrough rather than by a rule — re-run the
bucket's own positive predicates over its members and count the misses:

    SELECT countIf(<positive predicates for that bucket>) AS by_rule,
           countIf(NOT (<same predicates>))              AS by_fallthrough
    FROM <model> WHERE <category> = '<bucket>'

`by_fallthrough` should be 0. Any other value is the size of the lie. Then diff old vs
new classification as a migration matrix grouped by (old, new) — a correct widening
shows transitions *out of* the over-assigned bucket and none into it.

Watch for accented brand spellings while auditing: `lowerUTF8` lowercases the accent but
`ILIKE` matches literal bytes, so `'%telefon%'` matches neither `TELEFONICA BRASIL` nor
`Telefonos`, and a single brand splits across buckets by country. One stem does not fix it
either — the accent lands in a different position per language, so `'%telef%'` recovers
`Telefonica` but still misses `Telefonos` (breaks at `tel-e`). Use a stem on each side of
the accent and keep the patterns ASCII, since some `scripts/checks` readers open model SQL
with no explicit encoding and die on cp1252.

## Enforcement
No static gate — whether an ELSE branch is a legitimate category or an unsafe default is
intent, not statically checkable. Recorded as a convention in the
`stg_crawlers_data__ipinfo.generic_provider` column doc, which states the contract
(positive-evidence-only), why the bucket names are constrained, and to widen the lists
rather than narrow the fallback. Related: [circular-completeness-proof](circular-completeness-proof.md)
(a metric that validates itself against its own coverage).
