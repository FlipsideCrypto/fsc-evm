{% macro get_children(parent_key) %}
    {#
        Retrieve all child configuration values for a parent key.
        This is useful for hierarchical configurations like token mappings.
        
        Args:
            parent_key (str): The parent configuration key
            
        Returns:
            dict: A dictionary of child key:value pairs
    #}
    
    {% if execute %}
        -- Look up the hierarchical config
        {% set hier_config_path %}{{project_path}}/../logs/config_cache/config_hierarchical_{{target.name}}.json{% endset %}
        
        {% if modules.os.path.exists(hier_config_path) %}
            {% set config_file = modules.open(hier_config_path, 'r') %}
            {% set hier_config = modules.json.loads(config_file.read()) %}
            {% do config_file.close() %}
            
            {% if parent_key in hier_config and 'children' in hier_config[parent_key] %}
                {% set result = {} %}
                {% for child_key, child_data in hier_config[parent_key]['children'].items() %}
                    {% do result.update({child_key: child_data['value']}) %}
                {% endfor %}
                {{ return(result) }}
            {% endif %}
        {% endif %}
        
        -- If not found, return empty dict
        {{ return({}) }}
    {% else %}
        {{ return({}) }}
    {% endif %}
{% endmacro %}