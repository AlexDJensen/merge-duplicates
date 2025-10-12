/*
This macro overrides/extends the default from DBT.
Namely, if you give it two parameters as incremental predicates,
the first one which must be "DYNAMIC_RANGE",
the second one should be the timestamp column you want to use.
It will then use the min and max values from the "source" (i.e. extracted data)
to use as inputs for the predicates.
*/
{% macro get_merge_sql(
        target,
        source,
        unique_key,
        dest_columns,
        incremental_predicates
    ) -%}
    {% set predicate_override = "" %}
    {% if incremental_predicates [0] == "DYNAMIC_RANGE" %}
        -- run some queries to dynamically determine the min + max of this 'date_column' in the new data
        {% set date_column = incremental_predicates [1] %}
        {% set date_agg = incremental_predicates [ 2] %}
        {% set get_limits_query %}
    SELECT
        MIN(
            {{ date_column }}
        ) AS lower_limit,
        MAX(
            {{ date_column }}
        ) AS upper_limit,
        ARRAY_AGG(
            DISTINCT(
                DATE_TRUNC(
                    '{{ date_agg }}',
                    {{ date_column }}
                )
            )
        ) AS chunks
    FROM
        {{ source }}

        {% endset %}
        {% set limits = run_query(get_limits_query) [0] %}
        {{ log(
            limits,
            info = true
        ) }}

        {% set lower_limit,
        upper_limit,
        chunks = limits [0],
        limits [1],
        limits [2] %}
        {{ log(
            chunks,
            info = true
        ) }}

        {% set chunks = fromjson(chunks) %}
        {{ log(
            chunks,
            info = true
        ) }}
        -- use those calculated min + max values to limit 'target' scan, to only the days with new data
        {% set predicate_override %}
        dbt_internal_dest.{{ date_column }} BETWEEN '{{ lower_limit }}'
        AND '{{ upper_limit }}'
        AND DATE_TRUNC(
            '{{ date_agg }}',
            dbt_internal_dest.{{ date_column }}
        ) IN {{ sql_in_list(
            chunks,
            'timestamp'
        ) }}

        {% endset %}
    {% endif %}

    {% set predicates = [predicate_override] if predicate_override else incremental_predicates %}
    -- standard merge from here
    {% set merge_sql = dbt.get_merge_sql(
        target,
        source,
        unique_key,
        dest_columns,
        predicates
    ) %}
    {{ return(merge_sql) }}
{% endmacro %}

{% macro sql_in_list(
        values,
        cast = 'text'
    ) %}
    (
        {% for v in values %}
            {{ cast }}
            '{{ v }}' {% if not loop.last %},
            {% endif %}
        {% endfor %}
    )
{% endmacro %}
