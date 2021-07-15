with click_perf as
(select account, campaign, adgroup, ADID, keywordid, keywordplacement, matchtype, googleclickid
  ,sum(clicks)
  
from "INCUBATION"."DBT_GOOGLE_ADS"."CLICK_PERFORMANCE_REPORT"

--where googleclickid = 'Cj0KCQjw--GFBhDeARIsACH_kdYAvwPWMQXx1OUHrq0zrUq0s8W7BjrycypGtSWJETNVx53K678J_YUaAhITEALw_wcB'
group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5
)

select
-- count(*)
 l.lead_id, l.email, cp.*
from click_perf cp INNER JOIN  {{ ref('stg_sfdc_lead') }} l ON cp.googleclickid = l.gclid
order by email