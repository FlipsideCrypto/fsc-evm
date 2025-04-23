{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['streamline','core','chainhead','phase_1']
) }}

SELECT
    _id,
    (
        ({{ vars.MAIN_SL_BLOCKS_PER_HOUR }} / 60) * {{ vars.MAIN_SL_CHAINHEAD_DELAY_MINUTES }}
    ) :: INT AS block_number_delay, --minute-based block delay
    (_id - block_number_delay) :: INT AS block_number,
    utils.udf_int_to_hex(block_number) AS block_number_hex
FROM
    {{ ref('admin__number_sequence') }}
WHERE
    _id <= (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            {{ ref("streamline__get_chainhead") }}
    )