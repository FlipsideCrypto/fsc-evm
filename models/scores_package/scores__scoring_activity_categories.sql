{{ config (
    materialized = "view",
    tags = ['scores_package']
) }}

select * from {{ source('data_science_silver', 'scoring_activity_categories') }}