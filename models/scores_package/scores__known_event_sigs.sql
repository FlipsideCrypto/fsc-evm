{{ config (
    materialized = "view",
    tags = ['scores_package']
) }}

select * from {{ source('data_science_silver', 'evm_known_event_sigs') }}