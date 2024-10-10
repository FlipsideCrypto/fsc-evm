{%- if var('GLOBAL_USES_V2_FSC_EVM', False) -%}

{%- set max_num = var('GLOBAL_MAX_SEQUENCE_NUMBER', 1000000000) -%}

{% set post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_id)" %}

{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("MAX_SEQUENCE_NUMBER: " ~ max_num, info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    cluster_by = ' ~ config.get('cluster_by') ~ ',\n' %}
    {% set config_log = config_log ~ '    post_hook = "' ~ post_hook ~ '",\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{{ config(
    materialized = 'incremental',
    cluster_by = 'round(_id,-3)',
    post_hook = post_hook,
    tags = ['utils']
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
{%- endif -%}