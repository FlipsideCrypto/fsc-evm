{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['gha_tasks']
) }}

{{ generate_workflow_schedules(vars.MAIN_GHA_CHAINHEAD_SCHEDULE) }}