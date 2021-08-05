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
        er.CONVERSIONRATE AS exchange_rate
    FROM 
        {{ var('database_name') }}.{{ var('source_schema_sfdc') }}."OPPORTUNITY" op
        JOIN {{ var('database_name') }}.{{ var('source_schema_sfdc') }}."ACCOUNT" ac ON op.ACCOUNTID=ac.ACCOUNT_ID_LONG__C
        JOIN {{ var('database_name') }}.{{ var('source_schema_sfdc') }}."CONTACT" cc ON cc.ACCOUNTID =ac.ACCOUNT_ID_LONG__C
        JOIN {{ var('database_name') }}.{{ var('source_schema_sfdc') }}."CURRENCYTYPE" er ON op.Opportunity_Currency__c = er.isocode
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
        ,exchange_rate
        ,count(DISTINCT op_data.email) number_opportunities
        ,count(distinct op_data.ACCOUNTID) as number_accounts
        ,case when sum(op_data.ISWON) >= 1 then 1 else 0 end as closed_won
        ,sum(op_data.AMOUNT) as pipeline_generated
        ,sum(case when op_data.ISWON = 1 then op_data.BOOKED_AMOUNT__C else 0 end) as pipeline_won
        ,sum(op_data.AMOUNT/exchange_rate) as pipeline_generated_usd
        ,sum(case when op_data.ISWON = 1 then op_data.BOOKED_AMOUNT__C/exchange_rate else 0 end) as pipeline_won_usd
    from op_data
    group by 1,2,3,4
--    order by email
)

select * from aggregated