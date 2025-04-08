{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver','gha_tasks']
) }}

{{ gha_task_performance_view() }}