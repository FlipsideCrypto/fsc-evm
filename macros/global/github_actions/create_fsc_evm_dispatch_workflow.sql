{% macro create_fsc_evm_dispatch_workflow() %}
    {% if var("UPDATE_UDFS_AND_SPS", false) and target.database.lower() in ['fsc_evm', 'fsc_evm_dev'] %}
        {% set dispatch_workflow_sql %}
            CREATE OR REPLACE PROCEDURE utils.dispatch_workflow(
                workflow_name STRING,       -- e.g. 'dbt_run_adhoc'
                input_repos ARRAY,          -- e.g. ARRAY_CONSTRUCT('mantle') or ARRAY_CONSTRUCT('all')
                command STRING DEFAULT NULL  -- only used when workflow_name = 'dbt_run_adhoc'
            )
            RETURNS ARRAY
            LANGUAGE SQL
            AS
            $$
            DECLARE
                repo_names ARRAY;
                body_params OBJECT;
                results ARRAY;
                first_element STRING;
                repo_count INTEGER;
                workflow_name_with_suffix STRING;
            BEGIN
                -- Add .yml suffix to workflow name
                workflow_name_with_suffix := workflow_name || '.yml';

                -- Determine repo list and append -models suffix
                repo_count := ARRAY_SIZE(input_repos);
                first_element := LOWER(input_repos[0]);

                -- If the input is 'all', get all active repos from the repos table
                IF (repo_count = 1 AND first_element = 'all') THEN
                    SELECT ARRAY_AGG(DISTINCT github_repo) INTO repo_names
                    FROM admin.repos
                    WHERE is_active = TRUE;
                ELSE
                    -- Append -models to each repo name
                    repo_names := (
                        SELECT ARRAY_AGG(VALUE || '-models')
                        FROM TABLE(FLATTEN(input => :input_repos))
                    );
                END IF;

                -- Conditionally build input payload
                IF (workflow_name_with_suffix = 'dbt_run_adhoc.yml' AND command IS NOT NULL) THEN
                    body_params := OBJECT_CONSTRUCT(
                        'ref', 'main',
                        'inputs', OBJECT_CONSTRUCT(
                            'dbt_command', command
                        )
                    );
                ELSE
                    body_params := OBJECT_CONSTRUCT(
                        'ref', 'main',
                        'inputs', OBJECT_CONSTRUCT()
                    );
                END IF;

                -- Dispatch workflow per repo
                results := (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                        'repo', repo,
                        'workflow', :workflow_name_with_suffix,
                        'status', CASE 
                            WHEN response:status_code = 204 THEN 'success'
                            ELSE 'failed'
                        END,
                        'status_code', response:status_code
                    ))
                    FROM (
                        SELECT 
                            VALUE as repo,
                            github_actions.workflow_dispatches(
                                'FlipsideCrypto',
                                VALUE,
                                :workflow_name_with_suffix,
                                :body_params
                            ) as response
                        FROM TABLE(FLATTEN(input => :repo_names))
                    )
                );

                RETURN results;
            END;
            $$;

        {% endset %}
        {% do run_query(dispatch_workflow_sql) %}
    {% endif %}
{% endmacro %}