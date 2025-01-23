{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['gha_tasks']
) }}

{{ fsc_utils.gha_task_performance_view() }}