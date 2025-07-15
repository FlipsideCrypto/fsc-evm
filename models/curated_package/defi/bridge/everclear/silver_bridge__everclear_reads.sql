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
start_epoch_chain AS (
    SELECT
        min_epoch,
        chainid
    FROM
        start_epoch,
        {{ ref('silver_bridge__everclear_chain_seed') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
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

{% if is_incremental() and var(
    'backfill_2',
    false
) %}
intent_list_to_requests AS (
    SELECT
        intent_id
    FROM
        {{ ref('silver_bridge__everclear_intent_added') }}
    WHERE
        destination_count > 1
        AND intent_id NOT IN (
            SELECT
                intent_id
            FROM
                {{ this }}
        ) qualify ROW_NUMBER() over (
            ORDER BY
                block_timestamp ASC,
                intent_id ASC
        ) <= 20
),
{% endif %}

requests AS (
    SELECT
        chainid,
        min_epoch,

{% if is_incremental() %}
-- backfill run mode 1
{% if var(
        'backfill_1',
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
    -- backfill run mode 2
    {% elif var(
        'backfill_2',
        false
    ) %}
    live.udf_api(
        CONCAT(
            'https://api.everclear.org/intents/',
            intent_id
        )
    ) AS response,
{% else %}
    -- regular incremental run
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
    -- full refresh run
    live.udf_api(
        CONCAT(
            'https://api.everclear.org/intents?limit=',
            {{ var(
                'backfill_limit',
                2500
            ) }},
            '&origins=',
            chainid,
            '&startDate=',
            min_epoch
        )
    ) AS response
{% endif %}
FROM
    start_epoch_chain

{% if is_incremental() %},
in_progress_epoch
{% endif %}

{% if is_incremental() and var(
    'backfill_2',
    false
) %},
intent_list_to_requests
{% endif %}
),
results AS (

{% if is_incremental() and var(
    'backfill_2',
    false
) %}
SELECT
    chainid,
    min_epoch,
    response :data :intent AS VALUE,
    LOWER(
        VALUE :output_asset :: STRING
    ) AS output_asset,
    VALUE :status :: STRING AS status,
    VALUE :hub_settlement_domain :: STRING AS destination_chain_id,
    TO_TIMESTAMP(
        VALUE :intent_created_timestamp :: INT
    ) AS intent_created_timestamp,
    VALUE :auto_id :: INT AS cursor_id,
    VALUE :intent_id :: STRING AS intent_id
FROM
    response
{% else %}
SELECT
    chainid,
    min_epoch,
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
    VALUE :intent_id :: STRING AS intent_id requests,
    LATERAL FLATTEN (
        input => response :data :intents
    )
{% endif %}
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
