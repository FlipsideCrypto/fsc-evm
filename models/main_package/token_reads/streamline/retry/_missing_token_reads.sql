{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'ephemeral'
) }}

{% if vars.MAIN_SL_NEW_BUILD_ENABLED %}

    SELECT
        -1 AS block_number
    WHERE 0=1
    {% else %}
    select 
        c.contract_address,
        c.latest_event_block,
        c.total_event_count
    from {{ ref('core__dim_contracts') }} d
    left join
        {{ ref('silver__relevant_contracts') }} c
    on
        c.contract_address = d.address
    where 
    name = '' and symbol = ''
    and c.total_event_count > 10000
    and inserted_timestamp <= SYSDATE() - INTERVAL '90 days'
{% endif %}
