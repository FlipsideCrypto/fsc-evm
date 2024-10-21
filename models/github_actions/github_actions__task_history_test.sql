{{ config(
    materialized = 'view',
    tags = ['gha_tasks'],
    enabled = false
) }}
{{ fsc_utils.gha_task_history_view() }}
