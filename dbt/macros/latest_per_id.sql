{#
  Latest version of each row in an append-only bronze table. On incremental runs
  only recent loads are read. The lookback covers a run that failed after bronze
  loaded but before dbt finished.
#}
{% macro latest_per_id(relation, lookback_hours=6) %}
    select *
    from {{ relation }}
    where true
    {% if is_incremental() %}
        and _extracted_at >= (
            select timestamp_sub(max(_extracted_at), interval {{ lookback_hours }} hour)
            from {{ this }}
        )
    {% endif %}
    qualify row_number() over (
        partition by id order by _source_change_ts desc, _extracted_at desc
    ) = 1
{% endmacro %}
