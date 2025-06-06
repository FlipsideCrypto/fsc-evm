{% macro create_fsc_evm_gha_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS", false) and target.database.lower() in ['fsc_evm', 'fsc_evm_dev'] %}

        {% set create_github_utils_schema %}
            CREATE SCHEMA IF NOT EXISTS GITHUB_UTILS;
        {% endset %}
        {% do run_query(create_github_utils_schema) %}


        {% set create_github_utils_headers %}
            CREATE OR REPLACE FUNCTION GITHUB_UTILS.HEADERS()
            RETURNS VARCHAR
            LANGUAGE SQL
            IMMUTABLE
            MEMOIZABLE AS '
                SELECT ''{"Authorization": "Bearer {TOKEN}",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "Accept": "application/vnd.github+json"}''

                ';
        {% endset %}
        {% do run_query(create_github_utils_headers) %}

        {% set post_sql %}
            CREATE OR REPLACE FUNCTION GITHUB_UTILS.POST("ROUTE" VARCHAR, "DATA" OBJECT)
            RETURNS VARIANT
            LANGUAGE SQL
            COMMENT='List all workflow runs for a workflow. You can replace workflow_id with the workflow file name. You can use parameters to narrow the list of results. [Docs](https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#list-workflow-runs-for-a-workflow).'
            AS '
                SELECT
                    live.udf_api(
                        ''POST'',
                        CONCAT_WS(''/'', ''https://api.github.com'', route),
                        PARSE_JSON(github_utils.headers()),
                        data,
                        IFF(_utils.udf_whoami() <> CURRENT_USER(), ''_FSC_SYS/GITHUB'', ''vault/github/api'')
                    )
                ';
        {% endset %}
        {% do run_query(post_sql) %}

        {% set dispatch_workflow_sql %}
        CREATE OR REPLACE FUNCTION GITHUB_ACTIONS.WORKFLOW_DISPATCHES("OWNER" VARCHAR, "REPO" VARCHAR, "WORKFLOW_ID" VARCHAR, "BODY" OBJECT)
        RETURNS OBJECT
        LANGUAGE SQL
        COMMENT='You can use this endpoint to manually trigger a GitHub Actions workflow run. You can replace workflow_id with the workflow file name. For example, you could use main.yaml. [Docs](https://docs.github.com/en/rest/actions/workflows?apiVersion=2022-11-28#create-a-workflow-dispatch-event).'
        AS '
            SELECT
                github_utils.POST(
                    CONCAT_WS(''/'', ''repos'', owner, repo, ''actions/workflows'', workflow_id, ''dispatches''),
                    COALESCE(body, {''ref'': ''main''})::OBJECT
                )::OBJECT
            ';
        {% endset %}
        {% do run_query(dispatch_workflow_sql) %}

        {% set dispatch_workflow_sql_overload %}
            CREATE OR REPLACE FUNCTION GITHUB_ACTIONS.WORKFLOW_DISPATCHES("OWNER" VARCHAR, "REPO" VARCHAR, "WORKFLOW_ID" VARCHAR)
            RETURNS OBJECT
            LANGUAGE SQL
            COMMENT='You can use this endpoint to manually trigger a GitHub Actions workflow run. You can replace workflow_id with the workflow file name. For example, you could use main.yaml. [Docs](https://docs.github.com/en/rest/actions/workflows?apiVersion=2022-11-28#create-a-workflow-dispatch-event).'
            AS '
                SELECT
                    github_actions.workflow_dispatches(owner, repo, workflow_id, NULL)
                ';
        {% endset %}
        {% do run_query(dispatch_workflow_sql_overload) %}

    {% endif %}
{% endmacro %}