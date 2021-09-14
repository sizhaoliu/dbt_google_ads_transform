with click_perf as
(
  select account, campaign, adgroup, ADID, keywordid, keywordplacement, matchtype, googleclickid, sum(clicks)
  from "INCUBATION"."DBT_GOOGLE_ADS"."CLICK_PERFORMANCE_REPORT"
  group by 1,2,3,4,5,6,7,8
  order by 1,2,3,4,5
)

select l.lead_id, l.email, cp.*
from click_perf cp INNER JOIN  {{ ref('sfdc_lead') }} l ON cp.googleclickid = l.gclid
order by email