{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = get_path_tags(model)
) }}

select * from {{ source('data_science_silver', 'scoring_activity_categories') }}