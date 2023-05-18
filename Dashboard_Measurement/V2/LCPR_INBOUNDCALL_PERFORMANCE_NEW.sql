CREATE OR REPLACE VIEW "lpcr_inboundcall_performance_new" AS 
WITH
  all_months AS (
   SELECT *
   FROM
     "lcpr.sandbox.dev"."lcpr_inboundcall_performance_022023_partial_2"
UNION ALL    SELECT *
   FROM
     "lcpr.sandbox.dev"."lcpr_inboundcall_performance_032023"
UNION ALL    SELECT *
   FROM
     "lcpr.sandbox.dev"."lcpr_inboundcall_performance_042023"
UNION ALL    SELECT *
   FROM
     "lcpr.sandbox.dev"."lcpr_inboundcall_performance_052023"
) 
, accounts_filter AS (
   SELECT *
   FROM
     (
      SELECT
        *
      , "row_number"() OVER (PARTITION BY sub_acct_no ORDER BY Migration_prev_to_sent ASC, Migration_prev_to_call ASC, call_after_rec DESC, MIGRATED DESC, date_recs DESC) rown
      FROM
        all_months
   ) 
   WHERE (rown = 1)
) 
, mail_migrations AS (
   SELECT
     account
   , date_sent date_sent_mail
   , ls_chg_dte_ocr ls_chg_dte_ocr_mail
   , month_migration month_migration_mail
   FROM
     "lcpr.sandbox.dev"."lcpr_clicktoacept_performance"
   WHERE (MIGRATED_ONE_MONTH = 1)
) 
, outbound_call_migrations AS (
   SELECT
     sub_acct_no sub_acct_no_oc
   , date_recs date_recs_outbound
   , ls_chg_dte_ocr ls_chg_dte_ocr_outbound
   , month_outbound_migration
   FROM
     "lcpr.sandbox.dev"."lcpr_outboundcall_performance"
   WHERE ((((MIGRATED_ONE_MONTH = 1) AND (exclude_mail_migration = 0)) AND (call_bef_mig = 1)) AND "regexp_like"("lower"(disposicion), 'ofiti.+cliente.acepta'))
) 
, join_mail_mig AS (
   SELECT
     a.*
   , b.*
   , c.*
   FROM
     ((accounts_filter a
   LEFT JOIN mail_migrations b ON (a.sub_acct_no = CAST(b.account AS bigint)))
   LEFT JOIN outbound_call_migrations c ON (a.sub_acct_no = CAST(c.sub_acct_no_oc AS bigint)))
) 
, final_table AS (
   SELECT
     *
   , (CASE WHEN ((account IS NOT NULL) AND (month_migration_mail >= month_recs)) THEN 1 ELSE 0 END) exclude_mail_migration
   , (CASE WHEN ((account IS NOT NULL) AND (month_migration_mail < month_recs)) THEN 1 ELSE 0 END) prev_mail_migration
   , (CASE WHEN ((sub_acct_no_oc IS NOT NULL) AND (month_outbound_migration >= month_recs)) THEN 1 ELSE 0 END) exclude_outbound_call_migration
   FROM
     join_mail_mig
) 
SELECT
  *
, ' ' email_id
, (CASE WHEN (((((account_call IS NOT NULL) AND ((migration_prev_to_sent = 0) OR (migration_prev_to_sent IS NULL))) AND ((Migration_prev_to_call = 0) OR (Migration_prev_to_call IS NULL))) AND (exclude_mail_migration = 0)) AND (exclude_outbound_call_migration = 0)) THEN 1 ELSE 0 END) contacted
, (CASE WHEN (((((((MIGRATED = 1) AND (MIGRATED_HIGH_PRICE = 1)) AND (call_after_rec = 1)) AND ((migration_prev_to_sent = 0) OR (migration_prev_to_sent IS NULL))) AND ((Migration_prev_to_call = 0) OR (Migration_prev_to_call IS NULL))) AND (exclude_mail_migration = 0)) AND (exclude_outbound_call_migration = 0)) THEN 1 ELSE 0 END) converted
, (CASE WHEN (((((((MIGRATED = 1) AND (Final_ARPU_DIF < 0)) AND (call_after_rec = 1)) AND ((migration_prev_to_sent = 0) OR (migration_prev_to_sent IS NULL))) AND ((Migration_prev_to_call = 0) OR (Migration_prev_to_call IS NULL))) AND (exclude_mail_migration = 0)) AND (exclude_outbound_call_migration = 0)) THEN 1 ELSE 0 END) converted_downgrade
FROM
  final_table
