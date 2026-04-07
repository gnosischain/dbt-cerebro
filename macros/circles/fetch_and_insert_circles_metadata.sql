{# ================================================================
   fetch_and_insert_circles_metadata.sql

   Nightly delta fetcher for Circles avatar IPFS metadata.

   Pulls a batch of unresolved (avatar, metadata_digest) targets from
   `int_execution_circles_v2_avatar_metadata_targets` and, for each
   row, uses ClickHouse's `url()` table function to fetch the JSON
   payload from the configured IPFS gateway, then inserts the body
   into `circles_avatar_metadata_raw`.

   Notes:
   - Mirrors the pattern in `macros/decoding/fetch_and_insert_abi.sql`.
   - Records successful fetches only. A failed `url()` call surfaces
     as a dbt run-operation error and the row is retried on the next
     invocation. Persistent failures are handled by the one-time
     Python backfill script which has proper retry/error bookkeeping.
   - Idempotent: `circles_avatar_metadata_raw` is a ReplacingMergeTree
     keyed on `(metadata_digest, avatar)`, so re-fetching the same
     pair just refreshes `fetched_at`.

   Usage:
     dbt run-operation fetch_and_insert_circles_metadata \
       --args '{"batch_size": 500}'
================================================================ #}

{% macro fetch_and_insert_circles_metadata(batch_size=500) %}

    {% set queue_sql %}
        SELECT
            t.avatar,
            t.metadata_digest,
            t.ipfs_cid_v0,
            t.gateway_url
        FROM {{ ref('int_execution_circles_v2_avatar_metadata_targets') }} t
        LEFT ANTI JOIN circles_avatar_metadata_raw r
          ON t.avatar = r.avatar
         AND t.metadata_digest = r.metadata_digest
        LIMIT {{ batch_size }}
    {% endset %}

    {% set queue = run_query(queue_sql) %}

    {% if execute %}
        {{ log("Circles metadata fetcher: " ~ queue.rows | length ~ " unresolved digests in this batch", info=true) }}
    {% endif %}

    {% if queue.rows | length == 0 %}
        {{ log("Nothing to fetch.", info=true) }}
        {{ return(none) }}
    {% endif %}

    {% set ok_count = namespace(value=0) %}
    {% set fail_count = namespace(value=0) %}

    {% for row in queue.rows %}
        {% set avatar = row[0] %}
        {% set metadata_digest = row[1] %}
        {% set ipfs_cid_v0 = row[2] %}
        {% set gateway_url = row[3] %}

        {% set insert_sql %}
            INSERT INTO circles_avatar_metadata_raw
                (avatar, metadata_digest, ipfs_cid_v0, gateway_url,
                 http_status, content_type, body, error, fetched_at)
            WITH src AS (
                SELECT body, _headers
                FROM url('{{ gateway_url }}', 'Raw', 'body String')
            )
            SELECT
                '{{ avatar }}',
                '{{ metadata_digest }}',
                '{{ ipfs_cid_v0 }}',
                '{{ gateway_url }}',
                CAST(200 AS UInt16),
                coalesce(_headers['content-type'], _headers['Content-Type'], ''),
                body,
                '',
                now()
            FROM src
        {% endset %}

        {% set ok = true %}
        {% if execute %}
            {% set _ = run_query(insert_sql) %}
        {% endif %}

        {% if ok %}
            {% set ok_count.value = ok_count.value + 1 %}
        {% else %}
            {% set fail_count.value = fail_count.value + 1 %}
        {% endif %}
    {% endfor %}

    {{ log("Circles metadata fetcher complete: " ~ ok_count.value ~ " ok, " ~ fail_count.value ~ " failed", info=true) }}

{% endmacro %}
