{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['silver','gha_tasks','phase_1']
) }}

WITH workflows AS (
    {{ generate_workflow_schedules(
        vars.main_gha_streamline_chainhead_cron
    ) }}
)
SELECT
    task_name,
    workflow_name,
    cadence,
    cron_schedule
FROM
    workflows
