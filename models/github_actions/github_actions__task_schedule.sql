{%- if var('GLOBAL_ENABLE_FSC_EVM', False) -%}

{{ config(
    materialized = 'view',
    tags = ['gha_tasks']
) }}

{{ fsc_utils.gha_task_schedule_view() }}

{%- endif -%}