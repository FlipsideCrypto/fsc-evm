WITH apr AS (
    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://gateway.prod.vertexprotocol.com/v2/apr'
            )
        ):data AS response
)
SELECT
    daSYSDATE() AS hour,
    f.value:borrow_apr::float AS borrow_apr,
    f.value:deposit_apr::float AS deposit_apr,
    f.value:product_id::string AS product_id,
    f.value:symbol::string AS symbol,
    f.value:tvl::float AS tvl
FROM
    apr A,
    LATERAL FLATTEN(
        input => response
    ) AS f;

select * from mantle_dev.silver.vertex_money_markets