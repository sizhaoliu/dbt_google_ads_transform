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
    FROM  {{ ref('stg_sfdc_opptty') }} o
    INNER JOIN  {{ ref('stg_sfdc_lead') }} l ON o.EMAIL=l.EMAIL
)

select * from salesforce_leads_join_opportunities