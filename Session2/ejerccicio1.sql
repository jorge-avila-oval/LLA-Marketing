select
     date_trunc('month', date(fix.dt))as mes,
     interaction.interaction_purpose_descrip as purpose,
     count(distinct fix.act_acct_cd) as num_users
    
from "db-analytics-prod"."fixed_cwp" as fix
    left join "db-stage-prod"."interactions_cwp"  as interaction on interaction.account_id = fix.act_acct_cd
where interaction.interaction_purpose_descrip = 'TICKET'
    and pd_bb_prod_cd is not null 
group by 1,2 order by  1
