{# Log configuration details #}
{{ log_model_details() }}
{{ config (
    materialized = "ephemeral"
) }}

WITH retry AS (

    SELECT
        r.contract_address,
        GREATEST(
            latest_call_block,
            latest_event_block
        ) AS block_number,
        total_interaction_count
    FROM
        {{ ref("silver__relevant_contracts") }}
        r
        LEFT JOIN {{ source(
            'abis_silver',
            'verified_abis'
        ) }}
        v USING (contract_address)
        LEFT JOIN {{ source(
            'complete_streamline',
            'complete_contract_abis'
        ) }} C
        ON r.contract_address = C.contract_address
        AND C._inserted_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- avoid retrying the same contract within the last 30 days
    WHERE
        r.total_interaction_count >= 250 -- high interaction count
        AND GREATEST(
            max_inserted_timestamp_logs,
            max_inserted_timestamp_traces
        ) >= CURRENT_DATE - INTERVAL '30 days' -- recent activity
        AND v.contract_address IS NULL -- no verified abi
        AND C.contract_address IS NULL
    ORDER BY
        total_interaction_count DESC
    LIMIT
        5
), FINAL AS (
    SELECT
        implementation_contract AS contract_address,
        start_block AS block_number
    FROM
        {{ ref("silver__proxies") }}
        p
        JOIN retry r USING (contract_address)
        LEFT JOIN {{ source(
            'abis_silver',
            'verified_abis'
        ) }}
        v
        ON v.contract_address = p.implementation_contract
        LEFT JOIN {{ source(
            'complete_streamline',
            'complete_contract_abis'
        ) }} C
        ON p.implementation_contract = C.contract_address
        AND C._inserted_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- avoid retrying the same contract within the last 30 days
    WHERE
        v.contract_address IS NULL
        AND C.contract_address IS NULL
    UNION ALL
    SELECT
        contract_address,
        block_number
    FROM
        retry
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY contract_address
        ORDER BY
            block_number DESC
    ) = 1
