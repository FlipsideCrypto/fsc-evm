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
        vars.MAIN_GHA_STREAMLINE_CHAINHEAD_CRON
    ) }}
    
    {% for key, value in vars.items() %}
        {% if '_GHA_' in key and key.endswith('_CRON') and value is not none %}
            {% set prefix = key.split('_GHA_')[0] %}
            {% if prefix != 'MAIN' %}
                {% set workflow_part = key.split('_GHA_')[1].replace('_CRON', '') %}
                UNION ALL
                SELECT
                    'dbt_{{ workflow_part.lower() }}' AS workflow_name,
                    '{{ value }}' AS cron_schedule,
                    'custom' AS cadence
            {% endif %}
        {% endif %}
    {% endfor %}
)
SELECT
    concat_ws(
        '_',
        'TRIGGER',
        UPPER(
            w.workflow_name
        )
    ) AS task_name,
    w.workflow_name,
    w.cadence,
    w.cron_schedule
FROM
    workflows w
    INNER JOIN {{ ref('github_actions__relevant_workflows') }}
    t
    ON w.workflow_name = t.workflow_name
