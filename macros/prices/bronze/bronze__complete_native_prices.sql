{% macro bronze_complete_native_prices(
        symbols,
        blockchains = target.database | lower | replace(
            '_dev',
            ''
        )
    ) %}
SELECT
    HOUR,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_native_prices'
    ) }}
WHERE
    blockchain IN ({% if blockchains is string %}
        '{{ blockchains }}'
    {% else %}
        {{ blockchains | replace('[', '') | replace(']', '') }}
    {% endif %})
    AND symbol IN ({% if symbols is string %}
        '{{ symbols }}'
    {% else %}
        {{ symbols | replace('[', '') | replace(']', '') }}
    {% endif %})
{% endmacro %}
