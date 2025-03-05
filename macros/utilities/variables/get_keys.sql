{% macro master_keys_config() %}
    {# Flattens a nested dictionary of configuration keys into a single-level dictionary
       for easier access to key configurations throughout the project #}
    
    {# Get the master keys dictionary #}
    {% set master_keys = master_vars_keys() %}
    
    {# Initialize empty dictionary for flattened keys #}
    {% set flattened_keys = {} %}
    
    {# Iterate through the nested structure and flatten it #}
    {% for package, categories in master_keys.items() %}
        {% for category, keys in categories.items() %}
            {% for key_name, key_config in keys.items() %}
                {# Simply copy each key configuration to the flattened dictionary #}
                {% do flattened_keys.update({key_name: key_config}) %}
            {% endfor %}
        {% endfor %}
    {% endfor %}
    
    {# Return the flattened dictionary #}
    {{ return(flattened_keys) }}
{% endmacro %} 
