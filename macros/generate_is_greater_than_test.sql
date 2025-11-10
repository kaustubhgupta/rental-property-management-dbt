{% macro generate_is_greater_than_test(table_name, left_side_col, right_side_col, dummy_val_search) %}
    {#- 
    Macro to generate logic for testing if a value is greater than other
    Input:
        table_name: The source table
        left_side_col: Column which has to be compared on higher side
        right_side_col: Column which has to be compared on lower side
        dummy_val_search: Any col which can be selected from soure table
     -#}
    select {{dummy_val_search}}
    from {{ ref(table_name) }}
    where {{left_side_col}}>{{right_side_col}}
{% endmacro %}