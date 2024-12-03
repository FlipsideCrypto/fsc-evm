{% test recent_test(model, column_name="BLOCK_TIMESTAMP", days=1) %}
    {{ config(severity='error') }}
    
    with latest_data as (
        select max({{ column_name }}) as max_ts
        from {{ model }}
    )
    select *
    from latest_data
    where max_ts < dateadd('day', -{{ days }}, current_timestamp())
{% endtest %}
