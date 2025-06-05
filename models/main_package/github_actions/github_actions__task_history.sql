{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver','gha_tasks']
) }}

SELECT
    *
FROM
    {{ source(
        'snowflake_account_usage',
        'complete_task_graphs'
    ) }}
WHERE
    database_name = UPPER('{{ target.database }}')
    AND schema_name = 'GITHUB_ACTIONS'