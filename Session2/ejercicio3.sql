select 
    date_trunc('month', date(fix.dt)) as mes,
    count(distinct fix.act_acct_cd) as num_users
from "db-analytics-prod"."fixed_cwp" as fix
    inner join "db-stage-dev"."so_hdr_cwc" as service on cast(fix.act_acct_cd as bigint) = service.account_id
    inner join "db-stage-prod"."interactions_cwp"  as interaction on cast(interaction.account_id as bigint) = service.account_id
where date(fix.dt) = date_trunc('month', date(fix.dt))
    and date(fix.dt) >= date('2022-01-01')
group by 1 order by 1 
