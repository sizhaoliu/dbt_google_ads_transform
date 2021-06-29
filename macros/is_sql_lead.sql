{% macro is_sql_lead(lead_status, country) %}
    case when   regexp_like(lead_status, '5a. Qualified.*','i')
          AND not regexp_like(lead_status, 'partner.*','i')
          AND not regexp_like(lead_status, 'Media/Analyst.*','i')
          AND not regexp_like(lead_status, 'Professor.*','i')
          AND not regexp_like(lead_status, 'Student.*','i')
          and NOT regexp_like(country, 'india.*','i')
          then 1 else 0 end
{% endmacro %}