{% macro create_fsc_evm_livequery() %}
    {% if var("UPDATE_UDFS_AND_SPS", false) and target.database.lower() in ['fsc_evm', 'fsc_evm_dev'] %}

        {% set drop_schemas_sql %}
            DROP SCHEMA IF EXISTS _LIVE;
            DROP SCHEMA IF EXISTS _UTILS;
            DROP SCHEMA IF EXISTS LIVE;
            DROP SCHEMA IF EXISTS UTILS;
        {% endset %}
        {% do run_query(drop_schemas_sql) %}

        {% set create_schemas_sql %}
            CREATE SCHEMA IF NOT EXISTS _LIVE;
            CREATE SCHEMA IF NOT EXISTS _UTILS;
            CREATE SCHEMA IF NOT EXISTS LIVE;
            CREATE SCHEMA IF NOT EXISTS UTILS;
        {% endset %}
        {% do run_query(create_schemas_sql) %}

        {% if target.name == 'prod' %}
            {% set create_internal_live %}
                CREATE OR REPLACE EXTERNAL FUNCTION _LIVE.UDF_API(
                    "METHOD" VARCHAR(16777216),
                    "URL" VARCHAR(16777216),
                    "HEADERS" OBJECT,
                    "DATA" VARIANT,
                    "USER_ID" VARCHAR(16777216),
                    "SECRET" VARCHAR(16777216)
                )
                RETURNS VARIANT
                API_INTEGRATION = "AWS_EVM_API_PROD_V2"
                AS 'https://rjh2boxrr2.execute-api.us-east-1.amazonaws.com/prod/udf_api';
            {% endset %}
        {% else %}
            {% set create_internal_live %}
                CREATE OR REPLACE EXTERNAL FUNCTION _LIVE.UDF_API(
                    "METHOD" VARCHAR(16777216),
                    "URL" VARCHAR(16777216),
                    "HEADERS" OBJECT,
                    "DATA" VARIANT,
                    "USER_ID" VARCHAR(16777216),
                    "SECRET" VARCHAR(16777216)
                )
                RETURNS VARIANT
                API_INTEGRATION = "AWS_EVM_API_STG_V2"
                AS 'https://n7mq5wo54j.execute-api.us-east-1.amazonaws.com/stg/udf_api';
            {% endset %}
        {% endif %}

        {% set create_whoami_sql %}
            CREATE OR REPLACE SECURE FUNCTION _UTILS.UDF_WHOAMI()
            RETURNS VARCHAR(16777216)
            LANGUAGE SQL
            STRICT
            IMMUTABLE
            MEMOIZABLE
            AS '
                SELECT COALESCE(
                    PARSE_JSON(GETVARIABLE(''LIVEQUERY_CONTEXT'')):userId::STRING,
                    CURRENT_USER()
                )
            ';
        {% endset %}

        {% set create_udf_api_sql %}
            -- Standard API call with method, url, headers, and data
            CREATE OR REPLACE FUNCTION LIVE.UDF_API(
                "METHOD" VARCHAR(16777216),
                "URL" VARCHAR(16777216),
                "HEADERS" OBJECT,
                "DATA" VARIANT
            )
            RETURNS VARIANT
            LANGUAGE SQL
            AS '
                SELECT _live.UDF_API(
                    method,
                    url,
                    headers,
                    data,
                    _utils.UDF_WHOAMI(),
                    ''''
                )
            ';

            -- API call with secret name
            CREATE OR REPLACE FUNCTION LIVE.UDF_API(
                "METHOD" VARCHAR(16777216),
                "URL" VARCHAR(16777216),
                "HEADERS" OBJECT,
                "DATA" VARIANT,
                "SECRET_NAME" VARCHAR(16777216)
            )
            RETURNS VARIANT
            LANGUAGE SQL
            AS '
                SELECT _live.UDF_API(
                    method,
                    url,
                    headers,
                    data,
                    _utils.UDF_WHOAMI(),
                    secret_name
                )
            ';

            -- Simple GET request
            CREATE OR REPLACE FUNCTION LIVE.UDF_API(
                "URL" VARCHAR(16777216)
            )
            RETURNS VARIANT
            LANGUAGE SQL
            AS '
                SELECT _live.UDF_API(
                    ''GET'',
                    url,
                    {},
                    {},
                    _utils.UDF_WHOAMI(),
                    ''''
                )
            ';

            -- POST request with data
            CREATE OR REPLACE FUNCTION LIVE.UDF_API(
                "URL" VARCHAR(16777216),
                "DATA" VARIANT
            )
            RETURNS VARIANT
            LANGUAGE SQL
            AS '
                SELECT _live.UDF_API(
                    ''POST'',
                    url,
                    {''Content-Type'': ''application/json''},
                    data,
                    _utils.UDF_WHOAMI(),
                    ''''
                )
            ';

            -- POST request with data and secret
            CREATE OR REPLACE FUNCTION LIVE.UDF_API(
                "URL" VARCHAR(16777216),
                "DATA" VARIANT,
                "SECRET_NAME" VARCHAR(16777216)
            )
            RETURNS VARIANT
            LANGUAGE SQL
            AS '
                SELECT _live.UDF_API(
                    ''POST'',
                    url,
                    {''Content-Type'': ''application/json''},
                    data,
                    _utils.UDF_WHOAMI(),
                    secret_name
                )
            ';

            -- GET request with secret
            CREATE OR REPLACE FUNCTION LIVE.UDF_API(
                "URL" VARCHAR(16777216),
                "SECRET_NAME" VARCHAR(16777216)
            )
            RETURNS VARIANT
            LANGUAGE SQL
            AS '
                SELECT _live.UDF_API(
                    ''GET'',
                    url,
                    {},
                    {},
                    _utils.UDF_WHOAMI(),
                    secret_name
                )
            ';
        {% endset %}

        {% set create_utils_sql %}
            -- Hex to int conversion with encoding
            CREATE OR REPLACE FUNCTION UTILS.UDF_HEX_TO_INT(
                "ENCODING" VARCHAR(16777216),
                "HEX" VARCHAR(16777216)
            )
            RETURNS VARCHAR(16777216)
            LANGUAGE PYTHON
            STRICT
            IMMUTABLE
            RUNTIME_VERSION = '3.8'
            HANDLER = 'hex_to_int'
            AS '
def hex_to_int(encoding, hex) -> str:
    if not hex:
        return None
    if encoding.lower() == ''s2c'':
        if hex[0:2].lower() != ''0x'':
            hex = f''0x{hex}''

        bits = len(hex[2:])*4
        value = int(hex, 0)
        if value & (1 << (bits-1)):
            value -= 1 << bits
        return str(value)
    else:
        return str(int(hex, 16))
            ';

            -- Simple hex to int conversion
            CREATE OR REPLACE FUNCTION UTILS.UDF_HEX_TO_INT(
                "HEX" VARCHAR(16777216)
            )
            RETURNS VARCHAR(16777216)
            LANGUAGE PYTHON
            STRICT
            IMMUTABLE
            RUNTIME_VERSION = '3.8'
            HANDLER = 'hex_to_int'
            AS '
def hex_to_int(hex) -> str:
    return (str(int(hex, 16)) if hex and hex != "0x" else None)
            ';

            -- Int to hex conversion
            CREATE OR REPLACE FUNCTION UTILS.UDF_INT_TO_HEX(
                "INT" NUMBER(38,0)
            )
            RETURNS VARCHAR(16777216)
            LANGUAGE SQL
            STRICT
            IMMUTABLE
            AS '
                SELECT CONCAT(''0x'', TRIM(TO_CHAR(int, ''XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'')))
            ';
        {% endset %}

        {% set permissions_sql %}
            GRANT USAGE on SCHEMA _LIVE to INTERNAL_DEV;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA _LIVE TO INTERNAL_DEV; 
            GRANT USAGE on SCHEMA _UTILS to INTERNAL_DEV;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA _UTILS TO INTERNAL_DEV; 
            GRANT USAGE on SCHEMA LIVE to INTERNAL_DEV;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA LIVE TO INTERNAL_DEV; 
            GRANT USAGE on SCHEMA UTILS to INTERNAL_DEV;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA UTILS TO INTERNAL_DEV; 
            GRANT USAGE on SCHEMA _LIVE to DBT_CLOUD_FSC_EVM;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA _LIVE TO DBT_CLOUD_FSC_EVM; 
            GRANT USAGE on SCHEMA _UTILS to DBT_CLOUD_FSC_EVM;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA _UTILS TO DBT_CLOUD_FSC_EVM; 
            GRANT USAGE on SCHEMA LIVE to DBT_CLOUD_FSC_EVM;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA LIVE TO DBT_CLOUD_FSC_EVM; 
            GRANT USAGE on SCHEMA UTILS to DBT_CLOUD_FSC_EVM;
            GRANT USAGE ON ALL FUNCTIONS IN SCHEMA UTILS TO DBT_CLOUD_FSC_EVM; 
        {% endset %}

        {% do run_query(create_internal_live) %}
        {% do run_query(create_whoami_sql) %}
        {% do run_query(create_udf_api_sql) %}
        {% do run_query(create_utils_sql) %}
        {% do run_query(permissions_sql) %}
    {% endif %}
{% endmacro %}
