{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['silver','gha_tasks','phase_1']
) }}

SELECT 
    workflow_name,
    inserted_at
FROM
    {{ source(
        'github_actions',
        'workflows'
    ) }}
