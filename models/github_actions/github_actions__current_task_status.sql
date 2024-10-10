{%- if var('GLOBAL_ENABLE_FSC_EVM', False) -%}

{{ config(
    materialized = 'view',
    tags = ['gha_tasks']
) }}

{{ fsc_utils.gha_task_current_status_view() }}

{%- endif -%}