

/*
  GnosisVPN client nodes -- one row per (network, node_address).

  WHAT MAKES A NODE A CLIENT: it emitted a KeyBinding but NEVER an
  AddressAnnouncement. Binding a packet key to a chain key is how any node joins
  the network; announcing a multiaddress is how a node advertises that it is
  REACHABLE as a relay. A node that joins without advertising an address cannot
  be routed through, so it is an edge of the mixnet -- a client -- not a relay or
  an exit.

  This is HOPR's own definition, taken from the SQL behind their published
  GnosisVPN active-users dashboard, and adopted deliberately so our numbers are
  comparable to the ones they publish rather than a parallel invention.

  WHY THIS MODEL EXISTS RATHER THAN A COLUMN ON int_hopr_nodes: that model's spine
  is announcement / safe-registry / channel activity, so a node that only ever
  key-bound is ABSENT from it entirely -- which is precisely this population. It
  also drops first_key_binding_at, the date the client came into existence.

  The classification is STRUCTURAL, derived from chain events, so it needs no
  maintenance as HOPR adds clients. Contrast hopr_node_registry, which names exit
  nodes from a git config and silently goes stale.

  CAVEAT A CONSUMER MUST CARRY: one operator can run many nodes, so a node is an
  upper bound on a person. There is no on-chain identity that collapses them --
  clients do not register a Safe, so even the operator trick used elsewhere in
  this domain does not apply here.
*/

WITH key_bindings AS (
    SELECT
        r.network                                       AS network,
        r.is_testnet                                    AS is_testnet,
        lower(a.decoded_params['chain_key'])            AS node_address,
        min(a.block_timestamp)                          AS first_key_binding_at,
        max(a.block_timestamp)                          AS last_key_binding_at,
        count()                                         AS key_binding_count
    FROM `dbt`.`contracts_hopr_Announcements_events` AS a
    INNER JOIN `dbt`.`contracts_hopr_registry`       AS r
        -- execution.logs carries addresses as BARE hex, the registry stores them
        -- 0x-prefixed; joining raw matches nothing and yields an empty model that
        -- still builds green.
        ON lower(a.contract_address) = replaceAll(r.address, '0x', '')
    WHERE a.event_name = 'KeyBinding'
      AND NOT empty(coalesce(a.decoded_params['chain_key'], ''))
    GROUP BY network, is_testnet, node_address
),

-- Any announcement at all disqualifies a node, however old: announcing once means
-- it offered itself as reachable. DISTINCT because a node re-announces whenever
-- its address changes and this is a membership test, not a count.
announced AS (
    SELECT DISTINCT
        r.network                                       AS network,
        lower(a.decoded_params['node'])                 AS node_address
    FROM `dbt`.`contracts_hopr_Announcements_events` AS a
    INNER JOIN `dbt`.`contracts_hopr_registry`       AS r
        ON lower(a.contract_address) = replaceAll(r.address, '0x', '')
    WHERE a.event_name = 'AddressAnnouncement'
      AND NOT empty(coalesce(a.decoded_params['node'], ''))
),

-- Known infrastructure from the seed. These announce, so the anti-join below has
-- already removed them; excluding them explicitly is a guard, not a filter -- if a
-- cover-traffic or exit node ever appeared here it would mean it never announced,
-- and counting HOPR's own infrastructure as a customer is the error worth blocking.
infrastructure AS (
    SELECT lower(node_address) AS node_address FROM `dbt`.`hopr_node_registry`
)

SELECT
    kb.network                                          AS network,
    kb.is_testnet                                       AS is_testnet,
    kb.node_address                                     AS node_address,
    kb.first_key_binding_at                             AS first_key_binding_at,
    toDate(kb.first_key_binding_at)                     AS first_deploy_date,
    kb.last_key_binding_at                              AS last_key_binding_at,
    kb.key_binding_count                                AS key_binding_count
FROM key_bindings AS kb
LEFT ANTI JOIN announced AS an
    ON kb.network = an.network AND kb.node_address = an.node_address
WHERE kb.node_address NOT IN (SELECT node_address FROM infrastructure)