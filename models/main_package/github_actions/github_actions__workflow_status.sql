{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
) }}

SELECT
    NAME,
    status,
    created_at,
    updated_at,
    run_started_at,
    run_attempt,
    run_number,
    CASE
        -- For in-progress workflows: use current time since they're still running
        WHEN LOWER(status) = 'in progress' 
        THEN TIMESTAMPDIFF(seconds, run_started_at, SYSDATE()) / 60
        -- For queued/waiting workflows: use time since creation
        WHEN LOWER(status) IN ('queued', 'waiting') 
        THEN TIMESTAMPDIFF(seconds, created_at, SYSDATE()) / 60
        -- For all other statuses (completed, failed, cancelled, etc): use actual duration
        ELSE TIMESTAMPDIFF(seconds, run_started_at, updated_at) / 60
    END AS run_minutes,
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
    