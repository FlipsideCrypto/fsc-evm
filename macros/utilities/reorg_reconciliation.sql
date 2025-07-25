{% macro reorg_reconciliation() %}

    {% set check_for_reorg_blocks_query %}
        SELECT 
            block_number
        FROM
            {{ ref('silver__logs') }} t
        WHERE
            t._inserted_timestamp > DATEADD(
                'day',
                -7,
                SYSDATE()
            )
            AND NOT EXISTS (
                SELECT
                    1
                FROM
                    {{ ref('silver__transactions') }}
                    s
                WHERE s.block_number = t.block_number
                AND s.tx_hash = t.tx_hash
            )
        GROUP BY block_number
    {% endset %}

    {% set results = run_query(check_for_reorg_blocks_query) %}

    {% if execute %}
        {% set impacted_blocks = results.columns[0].values() %}
        {{ log("Impacted blocks found:" ~ impacted_blocks | join(','), info=True) }}
        
        {% if impacted_blocks %}
            {% set reorg_models = [] %}
            {% for node in graph.nodes.values() if "reorg" in node.tags %}
                {% do reorg_models.append(node.name) %}
            {% endfor %}

            {% if reorg_models | length == 0 %}
                {{ log("No models found with the 'reorg' tag.", info=True) }}
                {% do return() %}
            {% endif %}

            {{ log("Checking for potentially impacted models:", info=True) }}
            {% set union_query %}
                {% for model in reorg_models %}
                    {% if not loop.first %}
                        UNION ALL
                    {% endif %}
                    SELECT 
                        '{{ model }}' as model_name,
                        COUNT(*) AS count_of_condition_met 
                    FROM {{ ref(model) }} 
                    WHERE 
                        block_number IN ({{ impacted_blocks | join(',') }})
                        AND _inserted_timestamp > DATEADD(
                            'day',
                            -7,
                            SYSDATE()
                        )
                    GROUP BY 1
                    HAVING count_of_condition_met > 0
                {% endfor %}
            {% endset %}

            {% set results = run_query(union_query) %}
            {% for row in results.rows %}
                {{ log("Model '" ~ row[0] ~ "' has " ~ row[1] ~ " potentially impacted records", info=True) }}
            {% endfor %}
            
            {% if execute %}
                {% set deletion_counts = {} %}
                
                {% for row in results.rows %}
                    
                    {# Get delete count before executing delete #}
                    {% set delete_count_query %}
                        SELECT 
                            COUNT(*) as records_to_delete
                        FROM {{ ref(row[0]) }} t
                        WHERE t._inserted_timestamp > DATEADD(
                                'day',
                                -7,
                                SYSDATE()
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                FROM {{ ref('silver__transactions') }} s
                                WHERE s.block_number = t.block_number
                                AND s.tx_hash = t.tx_hash
                            )
                            AND block_number IN ({{ impacted_blocks | join(',') }})
                    {% endset %}
                    
                    {% set delete_count = run_query(delete_count_query).columns[0].values()[0] %}
                    
                    {% if delete_count > 0 %}
                        {% set delete_sql %}
                            DELETE FROM {{ ref(row[0]) }} t
                            WHERE t._inserted_timestamp > DATEADD(
                                    'day',
                                    -7,
                                    SYSDATE()
                                )
                                AND NOT EXISTS (
                                    SELECT 1
                                    FROM {{ ref('silver__transactions') }} s
                                    WHERE s.block_number = t.block_number
                                    AND s.tx_hash = t.tx_hash
                                )
                                AND block_number IN ({{ impacted_blocks | join(',') }})
                        {% endset %}
                        
                        {{ log("Executing delete for model '" ~ row[0] ~ "'...", info=True) }}
                        {% do run_query(delete_sql) %}
                        {{ log("Deleted " ~ delete_count ~ " records from model '" ~ row[0] ~ "'", info=True) }}
                        
                        {# Store deletion count #}
                        {% do deletion_counts.update({row[0]: delete_count}) %}
                    {% else %}
                        {{ log("Skipping delete for model '" ~ row[0] ~ "' - no records to delete", info=True) }}
                        {% do deletion_counts.update({row[0]: 0}) %}
                    {% endif %}
                {% endfor %}

                {# Summary log at the end #}
                {% set models_with_deletions = deletion_counts.items() | selectattr(1, '>', 0) | list %}
                {% if models_with_deletions | length > 0 %}
                    {{ log("=== DELETION SUMMARY ===", info=True) }}
                    {% for model, count in models_with_deletions %}
                        {{ log("Model '" ~ model ~ "': " ~ count ~ " records deleted", info=True) }}
                    {% endfor %}
                    {{ log("=====================", info=True) }}
                {% else %}
                    {{ log("No records were deleted from any models", info=True) }}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endif %}

{% endmacro %}