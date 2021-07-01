WITH salesforce_leads_join_opportunities AS 
(
    SELECT l.*,
        o.NUMBER_OPPORTUNITIES,
        o.CLOSED_WON,
        o.PIPELINE_GENERATED,
        o.PIPELINE_WON,
        o.region,
        o.currency
    FROM  {{ ref('stg_sfdc_lead') }} l
    LEFT JOIN  {{ ref('stg_sfdc_opptty') }} o ON o.EMAIL=l.EMAIL
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
        ,sum(PIPELINE_WON) as pipeline_won
    from salesforce_leads_join_opportunities
    group by 1,2,3,4,5,6
)

select * from leads_and_oppttys