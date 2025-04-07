{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver','github_actions','gha_tasks']
) }}

{{ gha_task_current_status_view() }}