{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['silver','scores']
) }}

select * from {{ source('data_science_silver', 'dates') }}