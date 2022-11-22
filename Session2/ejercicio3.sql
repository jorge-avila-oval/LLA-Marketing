select 
    date_trunc('month', date(fix.dt)) as mes,
    count(distinct fix.act_acct_cd) as num_users
from "db-analytics-prod"."fixed_cwp" as fix
    inner join "db-stage-dev"."so_hdr_cwc" as service on cast(fix.act_acct_cd as bigint) = service.account_id
    inner join "db-stage-prod"."interactions_cwp"  as interaction on interaction.account_id = fix.act_acct_cd
group by 1 order by 1 
