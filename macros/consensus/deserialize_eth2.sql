/*
FROM: https://notes.ethereum.org/@vbuterin/Sys3GLJbD

def compute_fork_digest(current_version: Version, genesis_validators_root: Root) -> ForkDigest:
    """
    Return the 4-byte fork digest for the ``current_version`` and ``genesis_validators_root``.
    This is a digest primarily used for domain separation on the p2p layer.
    4-bytes suffices for practical separation of forks/chains.
    """
    return ForkDigest(compute_fork_data_root(current_version, genesis_validators_root)[:4])


fork_diges: 
    is compute_fork_digest(current_fork_version, genesis_validators_root) where current_fork_version 
    is the fork version at the node's current epoch defined by the wall-clock time 
    (not necessarily the epoch to which the node is sync) genesis_validators_root is the static Root 
    found in state.genesis_validators_root;
next_fork_version: 
    is the fork version corresponding to the next planned hard fork at a future epoch. 
    If no future fork is planned, set next_fork_version = current_fork_version to signal this fact;
next_fork_epoch: 
    is the epoch at which the next fork is planned and the current_fork_version will be updated. 
    If no future fork is planned, set next_fork_epoch = FAR_FUTURE_EPOCH to signal this fact;
*/

{% macro deserialize_eth2(eth2) %}
    substr({{ eth2 }}, 1, 4) as fork_digest,
    substr({{ eth2 }}, 5, 4) as next_fork_version,
    {{ bytea_to_bigint('substr(' ~ eth2 ~ ', 9, 8)') }} as next_fork_epoch
{% endmacro %}