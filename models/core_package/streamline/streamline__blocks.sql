{%- set min_block = var('START_UP_BLOCK', 0) -%}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("START_UP_BLOCK: " ~ min_block, info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{{ config (
    materialized = "view",
    tags = ['streamline_core_complete']
) }}

SELECT
    _id AS block_number,
    utils.udf_int_to_hex(_id) AS block_number_hex
FROM
    {{ ref('utils__number_sequence') }}
WHERE
    _id BETWEEN {{ min_block }} AND (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            {{ ref("streamline__get_chainhead") }}
    )
ORDER BY
    _id ASC