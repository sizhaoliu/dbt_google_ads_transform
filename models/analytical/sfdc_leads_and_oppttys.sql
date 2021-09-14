WITH salesforce_leads_join_opportunities AS 
(
    SELECT l.*,
        o.NUMBER_OPPORTUNITIES,
        o.CLOSED_WON,
        o.PIPELINE_GENERATED,
        o.PIPELINE_WON,
        o.region,
        o.currency,
        o.pipeline_generated_usd,
        o.pipeline_won_usd
    FROM  {{ ref('sfdc_lead') }} l
    LEFT JOIN  {{ ref('sfdc_opportunity') }} o ON o.EMAIL=l.EMAIL
),

leads_and_oppttys AS 
(
    select
        date
        ,account
        ,platform
        ,campaign
        ,content as ad_group
        ,region
        ,0 as impressions
        ,0 as clicks
        ,0 as spend 
        ,count(distinct lead_id) as leads 
        ,sum(became_mql) as became_mql
        ,sum(became_sal) as became_sal
        ,sum(NUMBER_OPPORTUNITIES) as became_opportunity
        ,sum(CLOSED_WON) as became_closed_won
        ,sum(PIPELINE_GENERATED) as pipeline_generated
        ,sum(PIPELINE_GENERATED_USD) as pipeline_generated_usd
        ,sum(PIPELINE_WON) as pipeline_won
        ,sum(PIPELINE_WON_USD) as pipeline_won_usd
    from salesforce_leads_join_opportunities
    group by 1,2,3,4,5,6
)

select * from leads_and_oppttys