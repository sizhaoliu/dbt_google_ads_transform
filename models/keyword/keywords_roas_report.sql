
with pipeline_on_lead as (
  select term, sum(pipeline_generated_usd) as pipeline_generated_usd, sum(pipeline_won_usd) as pipeline_won_usd from {{ ref('pipeline_on_lead') }}
  group by 1
)

select kp.keyword, kp.spend, kp.clicks, kp.impressions, pl.pipeline_generated_usd, pl.pipeline_won_usd from {{ ref('keyword_perf_union')}} kp inner join pipeline_on_lead pl on kp.keyword = pl.term
order by pipeline_generated_usd desc