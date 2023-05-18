CREATE OR REPLACE VIEW "lcpr_outboundcall_performance_17022023" AS 
WITH
  outbound_recs AS (
   SELECT
     *
   , CAST("substring"(CAST(date AS varchar), 1, 10) AS date) date_recs
   FROM
     "lcpr.sandbox.dev"."outbound_call"
   WHERE ((date <> '0') AND (CAST("substring"(CAST(date AS varchar), 1, 10) AS date) BETWEEN "date"('2022-11-19') AND "date"('2023-02-17')))
   ORDER BY date DESC
) 
, outbound_feedback AS (
   SELECT
     cuenta
   , CAST("date_parse"(intento_discado_1, '%m/%d/%Y') AS date) intento_discado_1
   , hora_llamada
   , disposicion
   , comentarios
   FROM
     "lcpr.sandbox.dev"."lcpr_outbound_feedback"
   WHERE (intento_discado_1 <> '')
   ORDER BY intento_discado_1 DESC
) 
, outbound_feedback_account AS (
   SELECT
     cuenta
   , "min"(intento_discado_1) intento_discado_1
   , "min"(hora_llamada) hora_llamada
   , "min"(disposicion) disposicion
   , "min"(comentarios) comentarios
   FROM
     (
      SELECT
        cuenta
      , "first_value"("date"(intento_discado_1)) OVER (PARTITION BY cuenta ORDER BY "date"(intento_discado_1) DESC) intento_discado_1
      , "first_value"(hora_llamada) OVER (PARTITION BY cuenta ORDER BY "date"(intento_discado_1) DESC) hora_llamada
      , "first_value"(disposicion) OVER (PARTITION BY cuenta ORDER BY "date"(intento_discado_1) DESC) disposicion
      , "first_value"(comentarios) OVER (PARTITION BY cuenta ORDER BY "date"(intento_discado_1) DESC) comentarios
      FROM
        outbound_feedback
   ) 
   WHERE ("date"(intento_discado_1) BETWEEN "date"('2022-11-20') AND "date"('2023-02-17'))
   GROUP BY 1
) 
, outbound_table AS (
   SELECT
     a.*
   , b.*
   FROM
     (outbound_recs a
   INNER JOIN outbound_feedback_account b ON (CAST(a.sub_acct_no AS varchar) = CAST(b.cuenta AS varchar)))
) 
, billing_table AS (
   SELECT
     sub_acct_no_ooi SUB_ACCT_NO_SBB_max
   , ls_chg_dte_ocr
   FROM
     (
      SELECT
        *
      , "substring"(CABLE_UP, 1, 5) cable_bundle
      , "substring"(INTERNET_UP, 1, 5) internet_bundle
      , "substring"(PHONE_UP, 1, 5) phone_bundle
      FROM
        "lcpr.sandbox.dev"."transactions_orderactivity"
      WHERE (NOT (ord_typ IN ('CONNECT', 'V_DISCO', 'NON PAY', 'RESTART')))
   ) 
) 
, group_billing_table AS (
   SELECT
     SUB_ACCT_NO_SBB_max
   , "max"(ls_chg_dte_ocr) MAX_ORD_COMP_DATE
   FROM
     (
      SELECT
        a.SUB_ACCT_NO_SBB_max
      , a.ls_chg_dte_ocr
      FROM
        (billing_table a
      INNER JOIN outbound_table b ON (a.SUB_ACCT_NO_SBB_max = CAST(b.sub_acct_no AS bigint)))
      WHERE (("date_diff"('day', "date"(b.date_recs), "date"(a.ls_chg_dte_ocr)) <= 30) AND ("date_diff"('day', "date"(b.date_recs), "date"(a.ls_chg_dte_ocr)) >= 0))
   ) 
   GROUP BY SUB_ACCT_NO_SBB_max
) 
, billing_panel AS (
   SELECT
     *
   , "substring"(a.CABLE_UP, 1, 5) cable_bundle
   , "substring"(a.INTERNET_UP, 1, 5) internet_bundle
   , "substring"(a.PHONE_UP, 1, 5) phone_bundle
   , (CASE WHEN ("lower"(a.stb_sc) LIKE '%dvr%') THEN 1 ELSE 0 END) new_dvr_flag
   FROM
     ("lcpr.sandbox.dev"."transactions_orderactivity" a
   INNER JOIN group_billing_table b ON ((a.sub_acct_no_ooi = b.SUB_ACCT_NO_SBB_max) AND (a.ls_chg_dte_ocr = b.MAX_ORD_COMP_DATE)))
   WHERE (("date"(b.MAX_ORD_COMP_DATE) BETWEEN "date"('2022-11-23') AND "date"('2023-02-17')) AND (NOT (ord_typ IN ('CONNECT', 'V_DISCO', 'NON PAY', 'RESTART'))))
) 
, join_billing AS (
   SELECT
     a.*
   , b.sub_acct_no_ooi
   , b.ls_chg_dte_ocr
   , b.ord_typ
   , b.cable_bundle
   , b.internet_bundle
   , b.phone_bundle
   , b.new_dvr_flag
   , b.order_no_ooi order_id
   , (CASE WHEN (((((cable_bundle IS NOT NULL) AND (internet_bundle IS NOT NULL)) AND (phone_bundle IS NOT NULL)) AND (cable_bundle = internet_bundle)) AND (cable_bundle = phone_bundle)) THEN cable_bundle WHEN ((((cable_bundle IS NOT NULL) AND (internet_bundle IS NOT NULL)) AND (phone_bundle = ' ')) AND (cable_bundle = internet_bundle)) THEN cable_bundle WHEN ((((cable_bundle IS NOT NULL) AND (internet_bundle = ' ')) AND (phone_bundle IS NOT NULL)) AND (cable_bundle = phone_bundle)) THEN cable_bundle WHEN ((((cable_bundle = ' ') AND (internet_bundle IS NOT NULL)) AND (phone_bundle IS NOT NULL)) AND (internet_bundle = phone_bundle)) THEN internet_bundle WHEN (((cable_bundle IS NOT NULL) AND (internet_bundle = ' ')) AND (phone_bundle = ' ')) THEN cable_bundle WHEN (((internet_bundle IS NOT NULL) AND (cable_bundle = ' ')) AND (phone_bundle = ' ')) THEN internet_bundle WHEN (((phone_bundle IS NOT NULL) AND (internet_bundle = ' ')) AND (cable_bundle = ' ')) THEN phone_bundle ELSE null END) new_bundle
   FROM
     (outbound_table a
   LEFT JOIN billing_panel b ON (CAST(a.sub_acct_no AS bigint) = b.sub_acct_no_ooi))
) 
, package_price_panel AS (
   SELECT
     package_code
   , "arbitrary"(stmt_descr_line1l_pkg) stmt_descr_line1l_pkg
   , "arbitrary"(online_desc_pkg) online_desc_pkg
   , "min"(total_chrg_pkg) total_chrg_pkg
   FROM
     "lcpr.sandbox.dev"."servicepackagedictionary"
   GROUP BY 1
) 
, front_book AS (
   SELECT csg_codes
   FROM
     "lcpr.sandbox.dev"."2023_lcpr_frontbook"
) 
, join_package_price AS (
   SELECT
     a.*
   , (CASE WHEN ("lower"(a.stb) LIKE '%dvr%') THEN 1 ELSE 0 END) old_dvr_flag
   , (CASE WHEN (d.csg_codes IS NOT NULL) THEN 1 ELSE 0 END) old_plan_front_book
   , (CASE WHEN (("lower"(a.stb) LIKE '%dvr%') AND (d.csg_codes IS NOT NULL)) THEN (b.total_chrg_pkg + 5) ELSE b.total_chrg_pkg END) OLD_PRICE
   , (CASE WHEN (e.csg_codes IS NOT NULL) THEN 1 ELSE 0 END) new_plan_front_book
   , (CASE WHEN ((a.new_dvr_flag = 1) AND (e.csg_codes IS NOT NULL)) THEN (c.total_chrg_pkg + 5) ELSE c.total_chrg_pkg END) NEW_PRICE
   FROM
     ((((join_billing a
   LEFT JOIN package_price_panel b ON (a.pkg_cde = b.package_code))
   LEFT JOIN package_price_panel c ON (a.new_bundle = c.package_code))
   LEFT JOIN front_book d ON (a.pkg_cde = d.csg_codes))
   LEFT JOIN front_book e ON (a.new_bundle = e.csg_codes))
) 
, order_repeated_reg AS (
   SELECT
     *
   , "row_number"() OVER (PARTITION BY sub_acct_no ORDER BY exact_target ASC, NEW_PRICE DESC, date_contacted_diff DESC, date_trans_diff DESC, date_recs DESC) rown
   FROM
     (
      SELECT
        *
      , (CASE WHEN (recommendedpkg = new_bundle) THEN 1 ELSE 2 END) exact_target
      , (CASE WHEN ("date_diff"('day', "date"(date_recs), "date"(intento_discado_1)) >= 0) THEN 1 ELSE 0 END) date_contacted_diff
      , (CASE WHEN ("date_diff"('day', "date"(intento_discado_1), "date"(ls_chg_dte_ocr)) >= 0) THEN 1 ELSE 0 END) date_trans_diff
      FROM
        join_package_price
   ) 
) 
, Final_table AS (
   SELECT
     *
   , (CASE WHEN (new_bundle IS NOT NULL) THEN "substr"(CAST(new_bundle AS varchar), 2, 1) ELSE '0' END) new_bundle_p_code
   , (CASE WHEN (pkg_cde IS NOT NULL) THEN "substr"(CAST(pkg_cde AS varchar), 2, 1) ELSE '0' END) pkg_cde_p_code
   FROM
     order_repeated_reg
   WHERE (((new_bundle <> 'RJ2F5') OR (new_bundle IS NULL)) AND (rown = 1))
) 
, analytics_columns AS (
   SELECT
     *
   , (CASE WHEN (((Migration_prev_to_sent = 0) AND (Migration_prev_to_call = 0)) AND (call_bef_mig = 1)) THEN 1 ELSE 0 END) MIGRATED
   , (CASE WHEN ((((Migration_prev_to_sent = 0) AND ("date"(date) <= "date"(intento_discado_1))) AND ("date"(ls_chg_dte_ocr) <= ("date"(date) + INTERVAL  '30' DAY))) AND ("date"(intento_discado_1) <= "date"(ls_chg_dte_ocr))) THEN 1 ELSE 0 END) MIGRATED_ONE_MONTH
   , (CASE WHEN ((((Migration_prev_to_sent = 0) OR (Migration_prev_to_sent IS NULL)) AND ("date"(date_recs) <= "date"(intento_discado_1))) AND "regexp_like"("lower"(disposicion), 'ofiti.+cliente.+acepta')) THEN 1 ELSE 0 END) contacted
   , (CASE WHEN (Final_ARPU_DIF > 0) THEN 1 ELSE 0 END) POSITIVE_PRICE_DIF_FLAG
   , (CASE WHEN (Final_ARPU_DIF < 0) THEN 1 ELSE 0 END) NEGATIVE_PRICE_DIF_FLAG
   , (CASE WHEN (new_bundle IS NOT NULL) THEN (CAST(p_new_bundle AS int) - CAST(p_from_pkg_cde AS int)) END) total_rgu_change
   FROM
     (
      SELECT
        *
      , (CASE WHEN ((NEW_PRICE - OLD_PRICE) > 0) THEN 'UP' ELSE (CASE WHEN ((NEW_PRICE - OLD_PRICE) < 0) THEN 'DOWN' ELSE 'EQUAL' END) END) UP_DOWN
      , (CASE WHEN (NEW_PRICE IS NULL) THEN 0 ELSE (NEW_PRICE - OLD_PRICE) END) Final_ARPU_DIF
      , (CASE WHEN (CAST(ls_chg_dte_ocr AS date) < "date"(date)) THEN 1 ELSE (CASE WHEN (new_bundle IS NOT NULL) THEN 0 END) END) Migration_prev_to_sent
      , (CASE WHEN ("date"(LS_CHG_DTE_OCR) < "date"(intento_discado_1)) THEN 1 ELSE (CASE WHEN (new_bundle IS NOT NULL) THEN 0 END) END) Migration_prev_to_call
      , (CASE WHEN ("date"(intento_discado_1) <= "date"(ls_chg_dte_ocr)) THEN 1 ELSE 0 END) call_bef_mig
      , (CASE WHEN (RecommendedPkg <> new_bundle) THEN 1 ELSE 0 END) MIGRATED_TO_A_DIFFERENT_OFFER
      , (CASE WHEN (((new_bundle_p_code = '1') OR (new_bundle_p_code = '2')) OR (new_bundle_p_code = '4')) THEN '1' WHEN (((new_bundle_p_code = '3') OR (new_bundle_p_code = '5')) OR (new_bundle_p_code = '6')) THEN '2' WHEN (new_bundle_p_code = '7') THEN '3' ELSE '0' END) p_new_bundle
      , (CASE WHEN (((pkg_cde_p_code = '1') OR (pkg_cde_p_code = '2')) OR (pkg_cde_p_code = '4')) THEN '1' WHEN (((pkg_cde_p_code = '3') OR (pkg_cde_p_code = '5')) OR (pkg_cde_p_code = '6')) THEN '2' WHEN (pkg_cde_p_code = '7') THEN '3' ELSE '0' END) p_from_pkg_cde
      FROM
        Final_table
   ) 
) 
, category_change AS (
   SELECT
     *
   , (CASE WHEN (total_rgu_change = 0) THEN 'upsell' WHEN (total_rgu_change > 0) THEN 'crossell_up' WHEN (total_rgu_change < 0) THEN 'crossell_down' END) category_rgu_migration
   FROM
     analytics_columns
) 
, dna_info AS (
   SELECT
     sub_acct_no_sbb
   , "min"(res_city_hse) res_city_hse
   , "min"(addr1_hse) addr1_hse
   , (CASE WHEN ("min"(tenure) BETWEEN 0 AND 5E-1) THEN '0_6_months' WHEN (("min"(tenure) > 5E-1) AND ("min"(tenure) <= 1E0)) THEN '6_12_months' WHEN (("min"(tenure) > 1) AND ("min"(tenure) <= 2E0)) THEN '1_2_years' WHEN ("min"(tenure) > 2E0) THEN 'more_than_2_years' END) tenure
   FROM
     (
      SELECT
        sub_acct_no_sbb
      , "first_value"(res_city_hse) OVER (PARTITION BY sub_acct_no_sbb ORDER BY dt DESC) res_city_hse
      , "first_value"(addr1_hse) OVER (PARTITION BY sub_acct_no_sbb ORDER BY dt DESC) addr1_hse
      , "first_value"(tenure) OVER (PARTITION BY sub_acct_no_sbb ORDER BY dt DESC) tenure
      FROM
        "lcpr.stage.dev"."customer_services_rate_lcpr"
      WHERE (sub_acct_no_sbb IN (SELECT CAST(sub_acct_no AS bigint)
FROM
  category_change
))
   ) 
   GROUP BY 1
) 
, final_join AS (
   SELECT
     a.*
   , b.res_city_hse
   , b.addr1_hse
   , b.tenure
   , (CASE WHEN (a.MIGRATED_ONE_MONTH = 1) THEN "date_trunc"('month', "date"(a.ls_chg_dte_ocr)) END) month_outbound_migration
   FROM
     (category_change a
   LEFT JOIN dna_info b ON (CAST(a.sub_acct_no AS bigint) = b.sub_acct_no_sbb))
) 
, mail_migrations AS (
   SELECT
     account
   , date_sent date_sent_mail
   , ls_chg_dte_ocr ls_chg_dte_ocr_mail
   , month_migration month_migration_mail
   FROM
     "lcpr.sandbox.dev"."lcpr_clicktoaccept_performance_17022023"
   WHERE (MIGRATED_ONE_MONTH = 1)
) 
, join_mail_mig AS (
   SELECT
     a.*
   , b.*
   FROM
     (final_join a
   LEFT JOIN mail_migrations b ON (a.sub_acct_no = CAST(b.account AS bigint)))
) 
, final_table_mail AS (
   SELECT
     *
   , (CASE WHEN ((account IS NOT NULL) AND (month_migration_mail >= month_outbound_migration)) THEN 1 ELSE 0 END) exclude_mail_migration
   , (CASE WHEN ((account IS NOT NULL) AND (month_migration_mail < month_outbound_migration)) THEN 1 ELSE 0 END) prev_mail_migration
   FROM
     join_mail_mig
) 
SELECT
  *
, ' ' email_id
, (CASE WHEN ((((MIGRATED_ONE_MONTH = 1) AND (exclude_mail_migration = 0)) AND (POSITIVE_PRICE_DIF_FLAG = 1)) AND "regexp_like"("lower"(disposicion), 'ofiti.+cliente.acepta')) THEN 1 ELSE 0 END) converted
, (CASE WHEN ((((MIGRATED_ONE_MONTH = 1) AND (exclude_mail_migration = 0)) AND (NEGATIVE_PRICE_DIF_FLAG = 1)) AND "regexp_like"("lower"(disposicion), 'ofiti.+cliente.acepta')) THEN 1 ELSE 0 END) converted_downgrade
, 'Offerfit' Final_Group
FROM
  final_table_mail
