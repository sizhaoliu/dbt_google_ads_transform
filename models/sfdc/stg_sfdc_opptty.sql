WITH op_data AS 
(
    SELECT 
        op.ACCOUNTID,
        ac."NAME" ,
        cc.email,
        CASE WHEN op.ISWON = TRUE THEN 1 ELSE 0 END AS iswon, 
        op.AMOUNT,
        op.BOOKED_AMOUNT__C,
        op.REGION__C AS region,
        op.Opportunity_Currency__c AS currency,
        er.conversionrate
    FROM 
        {{ var('sfdc_opportunity_source') }} op
        JOIN {{ var('sfdc_account_source') }} ac ON op.ACCOUNTID=ac.ACCOUNT_ID_LONG__C
        JOIN {{ var('sfdc_contact_source') }} cc ON cc.ACCOUNTID =ac.ACCOUNT_ID_LONG__C
        JOIN {{ var('exchange_rates_source') }} er ON op.Opportunity_Currency__c = er.isocode
),

aggregated as (
    select 
     {% if var('use_hashed_email', false)  %}
        md5(op_data.email)
     {% else %}
        op_data.email
     {% endif %} as email
        ,region
        ,currency
        ,conversionrate
        ,count(DISTINCT op_data.email) number_opportunities
        ,count(distinct op_data.ACCOUNTID) as number_accounts
        ,case when sum(op_data.ISWON) >= 1 then 1 else 0 end as closed_won
        ,sum(op_data.AMOUNT) as pipeline_generated
        ,sum(case when op_data.ISWON = 1 then op_data.BOOKED_AMOUNT__C else 0 end) as pipeline_won
        ,sum(op_data.AMOUNT/conversionrate) as pipeline_generated_usd
        ,sum(case when op_data.ISWON = 1 then op_data.BOOKED_AMOUNT__C/conversionrate else 0 end) as pipeline_won_usd
    from op_data
    group by 1,2,3,4
--    order by email
)

select * from aggregated