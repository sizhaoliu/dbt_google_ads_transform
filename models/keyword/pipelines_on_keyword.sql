select lk.account, lk.campaign, lk.adgroup, lk.adid, lk.keywordid, lk.keywordplacement, lk.googleclickid,
  pl.pipeline_generated_usd, pl.pipeline_won_usd
from  {{ ref('leads_on_keyword') }} lk
inner join {{ ref('pipeline_on_lead') }} pl
on lk.email = pl.email
