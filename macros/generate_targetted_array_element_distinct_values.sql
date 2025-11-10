{% macro generate_targetted_array_element_distinct_values(table_name, array_col, lookup_val, distint_value) %}
    {#- 
    Macro to generate logic for getting distinct values based on array type value 
    filers. 
    Input:
        table_name: The source table
        array_col: Column which contains array type values
        lookup_val: Which value to look for in the array. Works only for string types
        distint_value: Which column distinct values to be returned when lookup value is present in the array
     -#}
    SELECT distinct {{distint_value}}
    FROM {{ ref(table_name) }} AS l,
    LATERAL FLATTEN(INPUT => l.{{array_col}}) AS f
    where LOWER(f.VALUE::string) LIKE lower('%{{lookup_val}}%')
{% endmacro %}

 