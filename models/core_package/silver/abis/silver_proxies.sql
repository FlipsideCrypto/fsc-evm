{% macro silver_proxies() %}
    WITH base AS (
        SELECT
            from_address,
            to_address,
            MIN(block_number) AS start_block,
            MIN(block_timestamp) AS start_timestamp,
            MAX(_inserted_timestamp) AS _inserted_timestamp
        FROM
            {{ ref('silver__traces') }}
        WHERE
            TYPE = 'DELEGATECALL'
            AND trace_status = 'SUCCESS'
            AND tx_status = 'SUCCESS'
            AND from_address != to_address -- exclude self-calls

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    from_address,
    to_address
),
create_id AS (
    SELECT
        from_address AS contract_address,
        to_address AS implementation_contract,
        start_block,
        start_timestamp,
        CONCAT(
            from_address,
            '-',
            to_address
        ) AS _id,
        _inserted_timestamp
    FROM
        base
),
heal AS (
    SELECT
        contract_address,
        implementation_contract,
        start_block,
        start_timestamp,
        _id,
        _inserted_timestamp
    FROM
        create_id

{% if is_incremental() %}
UNION ALL
SELECT
    contract_address,
    implementation_contract,
    start_block,
    start_timestamp,
    _id,
    _inserted_timestamp
FROM
    {{ this }}
    JOIN create_id USING (
        contract_address,
        implementation_contract
    )
{% endif %}
),
FINAL AS (
    SELECT
        contract_address,
        implementation_contract,
        start_block,
        start_timestamp,
        _id,
        _inserted_timestamp
    FROM
        heal qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            implementation_contract
            ORDER BY
                start_block ASC
        ) = 1
)
SELECT
    f.contract_address,
    f.implementation_contract,
    f.start_block,
    f.start_timestamp,
    f._id,
    f._inserted_timestamp,
    COALESCE(
        C.block_number,
        0
    ) AS created_block,
    COALESCE(
        p.block_number,
        0
    ) AS implementation_created_block
FROM
    FINAL f
    LEFT JOIN {{ ref('silver__created_contracts') }} C
    ON f.contract_address = C.created_contract_address
    LEFT JOIN {{ ref('silver__created_contracts') }}
    p
    ON f.implementation_contract = p.created_contract_address
{% endmacro %}
