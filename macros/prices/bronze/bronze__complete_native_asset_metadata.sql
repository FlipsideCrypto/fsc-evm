{% macro bronze_complete_native_asset_metadata(
        blockchain = target.database | replace(
            '_dev',
            ''
        ),
        symbol
    ) %}
SELECT
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_native_asset_metadata'
    ) }}
WHERE
    blockchain = {{ blockchain }}
    AND symbol = {{ symbol }}
{% endmacro %}
