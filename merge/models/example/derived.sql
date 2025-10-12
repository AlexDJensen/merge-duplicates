{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_id',
    incremental_predicates = ["DYNAMIC_RANGE", "client_tstamp", "hour"]
) }}

SELECT
    *
FROM
    {{ ref('staging') }}
WHERE
    client_tstamp BETWEEN CAST(
        '{{ var("start_date") }}' AS TIMESTAMP
    )
    AND CAST(
        '{{ var("end_date") }}' AS TIMESTAMP
    )
