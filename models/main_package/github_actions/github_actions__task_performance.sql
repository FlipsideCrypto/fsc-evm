{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = get_path_tags(model)
) }}

{{ fsc_utils.gha_task_performance_view() }}