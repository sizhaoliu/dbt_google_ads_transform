with union_data as (
select * from {{ ref('combined_ads_spend') }} 
union all 
select * from {{ ref('sfdc_leads_and_oppttys') }} 
),

final as(
    select date,
        date_part('week',date) as week
        ,date_part('month',date) as month
        ,account
        ,platform
        ,campaign
        ,ad_group
        ,'na' as theatre
        ,sum(impressions) as impressions
        ,sum(clicks) as clicks
        ,sum(spend) as spend
        ,sum(leads) as leads
        ,sum(became_mql) as became_mql
        ,sum(became_sal) as became_sal
        ,sum(became_opportunity) as became_opportunity
        ,sum(became_closed_won) as became_closed_won
        ,sum(pipeline_generated) as pipeline_generated
        ,sum(pipeline_won) as pipeline_won
    from union_data
    where date >= '{{ var('start_date') }}'
    group by 1,2,3,4,5,6,7--,8
)

select * from final