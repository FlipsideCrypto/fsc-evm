{# Set variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "intent_id",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH start_epoch AS (

    SELECT
        DATE_PART(epoch_second, MIN(block_timestamp :: DATE)) AS min_epoch
    FROM
        {{ ref('silver_bridge__everclear_intent_added') }}
    WHERE
        destination_count > 1
),

{% if is_incremental() %}
in_progress_epoch AS (
    SELECT
        DATE_PART(
            epoch_second,
            MIN(
                intent_created_timestamp :: DATE
            )
        ) AS min_progress_epoch,
        DATE_PART(
            epoch_second,
            MIN(
                intent_created_timestamp :: DATE
            ) + INTERVAL '1 day'
        ) AS min_progress_epoch_plus_1_day,
        DATE_PART(
            epoch_second,
            MAX(
                intent_created_timestamp :: DATE
            )
        ) AS max_progress_epoch
    FROM
        {{ this }}
),
{% endif %}

requests AS (
    SELECT
        vars.GLOBAL_PROJECT_NAME as chain,
        chainid,
        min_epoch,

{% if is_incremental() %}
{% if var(
        'backfill',
        false
    ) %}
    live.udf_api(
        CONCAT(
            'https://api.everclear.org/intents?limit=',
            {{ var(
                'backfill_limit',
                2500
            ) }},
            '&origins=',
            chainid,
            '&endDate=',
            min_progress_epoch_plus_1_day
        )
    ) AS response,
{% else %}
    live.udf_api(
        CONCAT(
            'https://api.everclear.org/intents?limit=500&origins=',
            chainid,
            '&startDate=',
            max_progress_epoch
        )
    ) AS response,
{% endif %}
{% else %}
    live.udf_api(
        CONCAT(
            'https://api.everclear.org/intents?limit=2500&origins=',
            chainid,
            '&startDate=',
            min_epoch
        )
    ) AS response,
{% endif %}

VALUE,
LOWER(
    VALUE :output_asset :: STRING
) AS output_asset,
VALUE :status :: STRING AS status,
VALUE :hub_settlement_domain :: STRING AS destination_chain_id,
TO_TIMESTAMP(
    VALUE :intent_created_timestamp :: INT
) AS intent_created_timestamp,
VALUE :auto_id :: INT AS cursor_id,
VALUE :intent_id :: STRING AS intent_id,
SYSDATE() AS inserted_timestamp,
SYSDATE() AS modified_timestamp
FROM
    start_epoch,

{% if is_incremental() %}
in_progress_epoch,
{% endif %}

LATERAL FLATTEN (
    input => response :data :intents
)
left join {{ ref('silver_bridge__everclear_chain_seed') }}
on vars.GLOBAL_PROJECT_NAME = chain
)
SELECT
    min_epoch,
    output_asset,
    status,
    destination_chain_id,
    intent_created_timestamp,
    cursor_id,
    intent_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    requests
