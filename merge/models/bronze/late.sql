{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_id',
    incremental_predicates = [ "dbt_internal_dest.derived_tstamp >= '" + var('start_date') + "'", "dbt_internal_dest.derived_tstamp <= '"+var('end_date') + "'"]
) }}

WITH raw_data AS (

    SELECT
        *
    FROM
        {{ ref("sample_events") }}
    WHERE
        (
            load_tstamp >= CAST(
                '{{ var("start_date") }}' AS TIMESTAMP
            )
            AND load_tstamp < CAST(
                '{{ var("end_date") }}' AS TIMESTAMP
            )
        )
        AND EXTRACT(
            epoch
            FROM
                (
                    collector_tstamp - derived_tstamp
                )
        ) :: INTEGER / (
            60 * 60
        ) > 1
),
deduped AS (
    SELECT
        DISTINCT
        ON (event_id) event_id,
        client_tstamp,
        derived_tstamp,
        collector_tstamp,
        load_tstamp
    FROM
        raw_data
    ORDER BY
        event_id,
        derived_tstamp
)
SELECT
    *
FROM
    deduped
