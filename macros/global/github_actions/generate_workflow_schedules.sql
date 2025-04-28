{% macro generate_workflow_schedules(chainhead_schedule) %}

{# Get all variables from return_vars #}
{% set vars = return_vars() %}

{# Parse chainhead schedule with safety checks #}
{% set chainhead_components = chainhead_schedule.split(' ') %}
{% set chainhead_minutes = chainhead_components[0] if chainhead_components|length > 0 else '0' %}
{% set chainhead_minutes_list = chainhead_minutes.split(',') | map('int') | list %}
{% set max_chainhead_minute = chainhead_minutes_list | max if chainhead_minutes_list|length > 0 else 0 %}

{# Generate a repo_id based on database name length to ensure unique schedules #}
{% set db_name = target.database %}
{% set repo_id = db_name|length % 12 %}

{# Helper function for root offset, in minutes #}
{% set root_offset = {} %}
{% for offset in range(0, 60) %}
    {% do root_offset.update({offset: ((max_chainhead_minute + offset) % 60) | string}) %}
{% endfor %}

{# Schedule templates with complete cron format #}
{% set schedule_templates = {
    'hourly': '{minute} * * * *',
    'every_4_hours': '{minute} */4 * * *',
    'daily': '{minute} {hour} * * *',
    'weekly': '{minute} {hour} * * {day}',
    'monthly': '{minute} {hour} 28 * *'
} %}

{# Define workflow definitions #}
{% set workflow_definitions = [
    {'name': 'dbt_run_streamline_chainhead', 'cadence': 'root', 'root_schedule': chainhead_schedule},
    {'name': 'dbt_run_scheduled_main', 'cadence': 'hourly', 'root_offset': 15},
    {'name': 'dbt_run_scheduled_decoder', 'cadence': 'hourly', 'root_offset': 40},
    {'name': 'dbt_run_scheduled_curated', 'cadence': 'every_4_hours', 'root_offset': 30},
    {'name': 'dbt_run_scheduled_abis', 'cadence': 'daily', 'root_offset': 20, 'hour': 1},
    {'name': 'dbt_run_scheduled_scores', 'cadence': 'daily', 'root_offset': 35, 'hour': 2},
    {'name': 'dbt_test_daily', 'cadence': 'daily', 'root_offset': 50, 'hour': 3},
    {'name': 'dbt_test_intraday', 'cadence': 'every_4_hours', 'root_offset': 50},
    {'name': 'dbt_test_monthly', 'cadence': 'monthly', 'root_offset': 20, 'hour': 1},
    {'name': 'dbt_run_heal_models', 'cadence': 'weekly', 'root_offset': 45, 'hour': 6, 'day': 0},
    {'name': 'dbt_run_full_observability', 'cadence': 'monthly', 'root_offset': 25, 'hour': 2},
    {'name': 'dbt_run_dev_refresh', 'cadence': 'weekly', 'root_offset': 40, 'hour': 7, 'day': 1},
    {'name': 'dbt_run_streamline_decoder_history', 'cadence': 'weekly', 'root_offset': 30, 'hour': 3, 'day': 6}
] %}

{# Generate all workflow schedules #}
{% for workflow in workflow_definitions %}

{# Extract workflow name to create variable name for override #}
{% set workflow_name = workflow.name %}
{% if workflow_name.startswith('dbt_run') %}
    {% set workflow_name = workflow_name[8:] %}
{% elif workflow_name.startswith('dbt_test') %}
    {% set workflow_name = workflow_name[4:] %}
{% endif %}

{# Create variable name for override functionality, which matches variable names set in return_vars() #}
{% set override_cron_var = 'MAIN_GHA_' + workflow_name.upper() + '_CRON' %}

{# Helper variables for template replacement #}
{% set template = schedule_templates[workflow.cadence] %}
{% set minute_val = root_offset[workflow.root_offset] %}
{% set hour_val = (workflow.get('hour', 0) + repo_id) % 24 %}
{% set day_val = workflow.get('day', 0) %}

    SELECT 
        '{{ workflow.name }}' AS workflow_name,
        {% if workflow.cadence == 'root' %}
            '{{ workflow.root_schedule }}'
        {% else %}
            {% if vars[override_cron_var] is defined and vars[override_cron_var] is not none %}
                '{{ vars[override_cron_var] }}'
            {% elif workflow.cadence == 'hourly' or workflow.cadence == 'every_4_hours' %}
                '{{ template.replace("{minute}", minute_val) }}'
            {% elif workflow.cadence == 'daily' or workflow.cadence == 'monthly' %}
                '{{ template.replace("{minute}", minute_val).replace("{hour}", hour_val | string) }}'
            {% elif workflow.cadence == 'weekly' %}
                '{{ template.replace("{minute}", minute_val).replace("{hour}", hour_val | string).replace("{day}", day_val | string) }}'
            {% endif %}
        {% endif %} AS cron_schedule,
        '{{ workflow.cadence }}' AS cadence
    
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}

{% endmacro %}