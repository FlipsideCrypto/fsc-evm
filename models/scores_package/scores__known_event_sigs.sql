{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['silver','scores','phase_3']
) }}

select * from {{ source('data_science_silver', 'evm_known_event_sigs') }}