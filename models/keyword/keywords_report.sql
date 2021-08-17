with keyword_perf as (
  select keyword
  , sum(cost/1000000) as spend
  , sum(clicks) as clicks
  , sum(impressions) as impressions
from  {{ var('database_name') }}.{{ var('source_schema_google_ads') }}."KEYWORDS_PERFORMANCE_REPORT"
group by 1
),

pipeline_on_lead as (
  select term, sum(pipeline_generated_usd) as pipeline_generated_usd, sum(pipeline_won_usd) as pipeline_won_usd from {{ ref('pipeline_on_lead') }}
  group by 1
)

select kp.keyword, kp.spend, kp.clicks, kp.impressions, pl.pipeline_generated_usd, pl.pipeline_won_usd from keyword_perf kp inner join pipeline_on_lead pl on kp.keyword = pl.term
order by pipeline_generated_usd desc