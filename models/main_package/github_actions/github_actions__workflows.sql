{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['silver','github_actions','gha_tasks']
) }}

{{ generate_workflow_schedules(vars.MAIN_GHA_STREAMLINE_CHAINHEAD_CRON) }}