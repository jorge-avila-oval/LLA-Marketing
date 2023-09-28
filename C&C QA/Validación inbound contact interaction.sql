with 
comm_hist_interaction as 
(
select
    *
from "db_dev_cdp_project"."communications_hist"
where channel = 'Inbound_Interactions' 
and conv_flg = true 
),

interaction as (
select 
    *
from "db-stage-prod-lf"."interactions_lcpr" 
-- limit 100
),

conteo_interactions as
(select 
    -- *
    comm_hist_interaction.account_id,
    sub_acct_no_sbb, 
    sent_dt, 
    contact_dt,
    interaction_id, 
    interaction_start_time, 
    interaction_channel, 
    other_interaction_info10,
    case 
        when regexp_like(lower(other_interaction_info10), 'phone|contact.+center') and regexp_like(lower(interaction_channel), 'c2max|teleperformance|dispute|live|nps|assurance|outbound.+project|^retention^|social|tsi|voip.+cssr|callcenter|sales.+cssr') then 1 else 0 end as cumple_condicion

from comm_hist_interaction inner join interaction
on sub_acct_no_sbb = customer_id and date(sent_dt) = date(interaction_start_time)
)

select distinct account_id, sum(cumple_condicion) from conteo_interactions group by 1 order by 2 asc
