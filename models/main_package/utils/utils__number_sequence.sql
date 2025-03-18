{%- set max_num = get_var('GLOBAL_MAX_SEQUENCE_NUMBER', 1000000000) -%}

{% set post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_id)" %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    cluster_by = 'round(_id,-3)',
    post_hook = post_hook,
    full_refresh = false,
    tags = get_path_tags(model)
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) - 1 :: INT AS _id
FROM
    TABLE(GENERATOR(rowcount => {{ max_num }}))
WHERE 1=1
{% if is_incremental() %}
    AND 1=0
{% endif %}