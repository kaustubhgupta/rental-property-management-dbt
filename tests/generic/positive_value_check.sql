{% test positive_value_check(model, column_name)%}
{#- 
    Custom generic test to detect negative values in a column
 -#}
select *
from {{model}}
where {{column_name}} <0
{%endtest%}