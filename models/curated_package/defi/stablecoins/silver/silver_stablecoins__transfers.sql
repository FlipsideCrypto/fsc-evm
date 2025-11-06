{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_transfers_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated']
) }}

WITH verified_stablecoins AS (

    SELECT
        contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND contract_address IS NOT NULL
),

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
newly_verified_stablecoins AS (
    SELECT
        contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        IFNULL(
            is_verified_modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ) > DATEADD(
            'day',
            -8,
            (
                SELECT
                    MAX(modified_timestamp) :: DATE
                FROM
                    {{ this }}
            )
        )
),
newly_verified_transfers AS (
    SELECT
        block_timestamp,
        block_timestamp :: DATE AS block_date,
        block_number,
        tx_hash,
        event_index,
        from_address,
        to_address,
        contract_address,
        amount
    FROM
        {{ ref('core__ez_token_transfers') }}
        INNER JOIN newly_verified_stablecoins USING (contract_address)
    WHERE
        block_date >= (
            SELECT
                MIN(block_date)
            FROM
                {{ ref('silver_stablecoins__supply_by_address') }}
        )
),
{% endif %}

transfers AS (
    SELECT
        block_timestamp,
        block_timestamp :: DATE AS block_date,
        block_number,
        tx_hash,
        event_index,
        from_address,
        to_address,
        contract_address,
        amount
    FROM
        {{ ref('core__ez_token_transfers') }}
        INNER JOIN verified_stablecoins USING (contract_address)
    WHERE
        block_date >= (
            SELECT
                MIN(block_date)
            FROM
                {{ ref('silver_stablecoins__supply_by_address') }}
        )

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_transfers AS (
    SELECT
        *
    FROM
        transfers

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
UNION
SELECT
    *
FROM
    newly_verified_transfers
{% endif %}
)
SELECT
    block_timestamp,
    block_date,
    block_number,
    tx_hash,
    event_index,
    from_address,
    to_address,
    contract_address,
    amount,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS stablecoins_transfers_id
FROM
    all_transfers
