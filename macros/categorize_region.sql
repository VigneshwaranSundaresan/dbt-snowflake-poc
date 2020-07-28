{% macro nation_key_val(key) %}
  CASE
    WHEN {{ key }} BETWEEN 0 and 7 THEN 'NORTH AMERICA'
    WHEN {{ key }} BETWEEN 7 and 13 THEN 'SOUTH AMERICA'
    WHEN {{ key }} BETWEEN 14 and 18 THEN 'ASIA'
    WHEN {{ key }} BETWEEN 19 and 21 THEN 'EUROPE'
    WHEN {{ key }} BETWEEN 22 and 25 THEN 'ASIA PACFIC'
    ELSE 'UNKNOWN'
  END
{% endmacro %}
