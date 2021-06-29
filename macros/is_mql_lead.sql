{% macro is_mql_lead(lattice_priority, lead_classifications, country) %}
    case when   regexp_like(lattice_priority, '(Best|High)','i')
          AND not regexp_like(lead_classifications, '(partner|Media/Analyst|Professor/Student)','i')
          and NOT regexp_like(country, '(india)','i')
          then 1 else 0 end

{% endmacro %}

{% macro is_sfdc_mql_lead(mql_last_date) %}
    case when  mql_last_date IS NOT NULL  
          then 1 else 0 end
{% endmacro %}