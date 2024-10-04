{% macro number_sequence() %}

{%- set max_num = var('MAX_SEQUENCE_NUMBER', 1000000000) -%}

{# Log configuration details if in execution mode #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("MAX_SEQUENCE_NUMBER: " ~ max_num, info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    cluster_by = ' ~ config.get('cluster_by') ~ ',\n' %}
    {% set config_log = config_log ~ '    post_hook = "' ~ config.get('post_hook') ~ '",\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{{ config(
    materialized = 'table',
    cluster_by = 'round(_id,-3)',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_id)"
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) - 1 :: INT AS _id
FROM
    TABLE(GENERATOR(rowcount => {{ max_num }}))
{% endmacro %}

{% macro block_sequence() %}

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
    {{ ref(
        'silver__number_sequence'
    ) }}
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
{% endmacro %}

{% macro block_ranges() %}
SELECT
    block_number,
    CASE
        WHEN RIGHT(
            block_number,
            1
        ) = 0 THEN block_number
    END AS block_number_10,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (
            00,
            25,
            50,
            75
        ) THEN block_number
    END AS block_number_25,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (
            00,
            50
        ) THEN block_number
    END AS block_number_50,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (00) THEN block_number
    END AS block_number_100,
    CASE
        WHEN RIGHT(
            block_number,
            3
        ) IN (000) THEN block_number
    END AS block_number_1000,
    CASE
        WHEN RIGHT(
            block_number,
            4
        ) IN (0000) THEN block_number
    END AS block_number_10000,
    block_timestamp,
    TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ ref("silver__blocks") }}
{% endmacro %}
