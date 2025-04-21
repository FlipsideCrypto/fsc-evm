{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['silver','gha_tasks','phase_1','recent_tests']
) }}

SELECT
    NAME,
    status,
    created_at,
    updated_at,
    run_started_at,
    run_attempt,
    run_number,
    TIMESTAMPDIFF(seconds, run_started_at, SYSDATE()) / 60 AS run_minutes,
    id,
    workflow_id,
    html_url
FROM
    TABLE(
        github_actions.tf_runs(
            'FlipsideCrypto',
            '{{ vars.GLOBAL_PROJECT_NAME.lower() }}' || '-models',
            {'per_page':'100'}
        )
    )
-- TODO: 
-- Improve run_minutes calc based on cron schedule from github_actions__workflows table or use "queued" status. 
-- Update .yml tests accordingly.