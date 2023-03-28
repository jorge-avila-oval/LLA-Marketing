with all_months as (
SELECT * FROM "lcpr.sandbox.dev"."lcpr_inboundcall_performance_112022" 
union all
SELECT * FROM "lcpr.sandbox.dev"."lcpr_inboundcall_performance_122022"
union all
SELECT * FROM "lcpr.sandbox.dev"."lcpr_inboundcall_performance_012023"
union all
SELECT * FROM "lcpr.sandbox.dev"."lcpr_inboundcall_performance_022023"
UNION ALL    
SELECT * FROM "lcpr.sandbox.dev"."lcpr_inboundcall_performance_032023"
),

accounts_filter as(
select * from 
(select *, 
row_number() over (partition by sub_acct_no order by Migration_prev_to_sent,Migration_prev_to_call,call_after_rec desc,MIGRATED desc,date_recs desc) as rown
from all_months)
where rown = 1
),

mail_migrations as (
select account, date_sent as date_sent_mail,ls_chg_dte_ocr as ls_chg_dte_ocr_mail , month_migration as month_migration_mail
FROM "lcpr.sandbox.dev"."lcpr_clicktoacept_performance"
where MIGRATED_ONE_MONTH = 1
),

outbound_call_migrations as (
SELECT sub_acct_no as sub_acct_no_oc,date_recs as date_recs_outbound,ls_chg_dte_ocr as ls_chg_dte_ocr_outbound,
month_outbound_migration
FROM "lcpr.sandbox.dev"."lcpr_outboundcall_performance" 
where MIGRATED_ONE_MONTH = 1 and exclude_mail_migration = 0 and call_bef_mig = 1 and
    regexp_like(lower(disposicion),'ofiti.+cliente.acepta')
),

join_mail_mig as
(select a.*, b.*,c.*
from accounts_filter a
left join mail_migrations b
on a.sub_acct_no = cast(b.account as bigint) --and a.month_recs = b.month_migration 
left join outbound_call_migrations c
on a.sub_acct_no = cast(c.sub_acct_no_oc as bigint)
),



final_table as 
(
select *,
case when account is not null and month_migration_mail >= month_recs then 1 else 0 end as exclude_mail_migration,
case when account is not null and month_migration_mail < month_recs then 1 else 0 end as prev_mail_migration,
case when sub_acct_no_oc is not null and month_outbound_migration >= month_recs then 1 else 0 end as exclude_outbound_call_migration
from join_mail_mig 
)

SELECT
  *
, (CASE WHEN (((((account_call IS NOT NULL) AND ((migration_prev_to_sent = 0) OR (migration_prev_to_sent IS NULL))) AND ((Migration_prev_to_call = 0) OR (Migration_prev_to_call IS NULL))) AND (exclude_mail_migration = 0)) AND (exclude_outbound_call_migration = 0)) THEN 1 ELSE 0 END) contacted
, (CASE WHEN (((((((MIGRATED = 1) AND (MIGRATED_HIGH_PRICE = 1)) AND (call_after_rec = 1)) AND ((migration_prev_to_sent = 0) OR (migration_prev_to_sent IS NULL))) AND ((Migration_prev_to_call = 0) OR (Migration_prev_to_call IS NULL))) AND (exclude_mail_migration = 0)) AND (exclude_outbound_call_migration = 0)) THEN 1 ELSE 0 END) converted
, (CASE WHEN (((((((MIGRATED = 1) AND (Final_ARPU_DIF < 0)) AND (call_after_rec = 1)) AND ((migration_prev_to_sent = 0) OR (migration_prev_to_sent IS NULL))) AND ((Migration_prev_to_call = 0) OR (Migration_prev_to_call IS NULL))) AND (exclude_mail_migration = 0)) AND (exclude_outbound_call_migration = 0)) THEN 1 ELSE 0 END) converted_downgrade
FROM
  final_table
