{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_id',
    incremental_predicates = ["DYNAMIC_RANGE", "client_tstamp", "hour"]
) }}

WITH raw_data AS (

    SELECT
        *
    FROM
        {{ ref('sample_data') }}
    WHERE
        load_tstamp >= CAST(
            '{{ var("start_date") }}' AS TIMESTAMP
        )
        AND load_tstamp < CAST(
            '{{ var("end_date") }}' AS TIMESTAMP
        )
),
deduped AS (
    SELECT
        event_id,
        ANY_VALUE(client_tstamp) AS client_tstamp,
        ANY_VALUE(derived_tstamp) AS derived_tstamp,
        ANY_VALUE(collector_tstamp) AS collector_tstamp,
        ANY_VALUE(load_tstamp) AS load_tstamp,
        ANY_VALUE(VALUE) AS VALUE
    FROM
        raw_data
    GROUP BY
        event_id
)
SELECT
    *
FROM
    deduped
