{%- if var('GLOBAL_USES_V2_FSC_EVM', False) -%}
{{ config(
    materialized = 'view',
    tags = ['gha_tasks']
) }}

{{ fsc_utils.gha_task_history_view() }}

{%- endif -%}