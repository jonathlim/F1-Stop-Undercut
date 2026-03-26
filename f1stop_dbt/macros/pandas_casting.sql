{#
    cast_nan_double: Casts a columnm as DOUBLE and converts NaN to NULL
    Use for: any numerical float value sourced from pandas
#}

{% macro cast_nan_double(col) %}
    NULLIF(CAST({{ col }} AS DOUBLE), CAST('NaN' AS DOUBLE))
{% endmacro %}

{#
    cast_nan_int: Casts a column to INT and converts 'nan' to NULL
    Use for: Integer numbers that  may arrive as floats from pandas
#}

{% macro cast_nan_int(col) %}
    CAST(CAST(NULLIF({{ col }}, 'nan') AS DOUBLE) AS INT)
{% endmacro %}

