CREATE OR REPLACE VIEW "lcpr_inboundcall_performance_032023" AS 
WITH
  parameters AS (
   SELECT
     "date"('2023-03-01') start_date
   , "date"('2023-03-31') end_date
   , "date_trunc"('month', "date"('2022-11-01')) month_date

) 
, inbound_recs AS (
   SELECT
     *
   , CAST("substring"(CAST(date AS varchar), 1, 10) AS date) date_recs
   , (CASE WHEN (group_id IN ('24', '99', '67', '41', '19')) THEN 'control_A' WHEN (group_id IN ('25', '47', '79', '22', '93')) THEN 'control_X' ELSE 'offerfit' END) group_type
   FROM
     (
      SELECT
        *
      , "substring"(CAST(Sub_Acct_No AS varchar), 15, 16) group_id
      FROM
        "lcpr.sandbox.dev"."outbound_inbound"
      WHERE (date <> '0')
   ) 
   WHERE (CAST("substring"(CAST(date AS varchar), 1, 10) AS date) BETWEEN (SELECT start_date
FROM
  parameters
) AND (SELECT end_date
FROM
  parameters
))
) 
, interactions_table AS (
   SELECT
     *
   , "first_value"(interaction_start_time) OVER (PARTITION BY account_id ORDER BY CAST(interaction_start_time AS date) ASC) first_interaction_start_time
   , "first_value"(interaction_start_time) OVER (PARTITION BY account_id ORDER BY CAST(interaction_start_time AS date) DESC) last_interaction_start_time
   FROM
     "lcpr.stage.prod"."lcpr_interactions_csg"
   WHERE (("regexp_like"("lower"(interaction_channel), 'c2max|teleperformance|dispute|live|nps|assurance|outbound.+project|^retention^|social|tsi|voip.+cssr|callcenter|sales.+cssr') AND "regexp_like"("lower"(other_interaction_info10), 'phone|contact.+center')) AND (CAST(interaction_start_time AS date) BETWEEN (SELECT start_date
FROM
  parameters
) AND (SELECT end_date
FROM
  parameters
)))
) 
, interactions_by_account AS (
   SELECT
     account_id
   , "min"(CAST(first_interaction_start_time AS date)) first_interaction_start_time
   , "max"(CAST(last_interaction_start_time AS date)) last_interaction_start_time
   FROM
     interactions_table
   GROUP BY 1
) 
, join_calls AS (
   SELECT
     a.*
   , b.account_id account_call
   , b.first_interaction_start_time
   , b.last_interaction_start_time
   , CAST(b.interaction_start_time AS date) interaction_start_time
   FROM
     (inbound_recs a
   LEFT JOIN interactions_table b ON (a.Sub_Acct_No = CAST(b.account_id AS bigint)))
   WHERE ((CAST(b.interaction_start_time AS date) >= "date"(a.date_recs)) AND (CAST(b.interaction_start_time AS date) <= ("date"(a.date_recs) + INTERVAL  '30' DAY)))
) 
, billing_table AS (
   SELECT
     sub_acct_no_ooi SUB_ACCT_NO_SBB_max
   , ls_chg_dte_ocr
   , CABLE_UP
   , INTERNET_UP
   , PHONE_UP
   , (CASE WHEN (((((cable_bundle IS NOT NULL) AND (internet_bundle IS NOT NULL)) AND (phone_bundle IS NOT NULL)) AND (cable_bundle = internet_bundle)) AND (cable_bundle = phone_bundle)) THEN cable_bundle WHEN ((((cable_bundle IS NOT NULL) AND (internet_bundle IS NOT NULL)) AND (phone_bundle = ' ')) AND (cable_bundle = internet_bundle)) THEN cable_bundle WHEN ((((cable_bundle IS NOT NULL) AND (internet_bundle = ' ')) AND (phone_bundle IS NOT NULL)) AND (cable_bundle = phone_bundle)) THEN cable_bundle WHEN ((((cable_bundle = ' ') AND (internet_bundle IS NOT NULL)) AND (phone_bundle IS NOT NULL)) AND (internet_bundle = phone_bundle)) THEN internet_bundle WHEN (((cable_bundle IS NOT NULL) AND (internet_bundle = ' ')) AND (phone_bundle = ' ')) THEN cable_bundle WHEN (((internet_bundle IS NOT NULL) AND (cable_bundle = ' ')) AND (phone_bundle = ' ')) THEN internet_bundle WHEN (((phone_bundle IS NOT NULL) AND (internet_bundle = ' ')) AND (cable_bundle = ' ')) THEN phone_bundle ELSE null END) new_bundle
   FROM
     (
      SELECT
        *
      , "substring"(CABLE_UP, 1, 5) cable_bundle
      , "substring"(INTERNET_UP, 1, 5) internet_bundle
      , "substring"(PHONE_UP, 1, 5) phone_bundle
      FROM
        "lcpr.sandbox.dev"."transactions_orderactivity"
      WHERE (("regexp_like"("lower"(salesrep_area), 'c2max|teleperformance|dispute|live|nps|assurance|outbound.+project|^retention^|social|tsi|voip.+cssr|callcenter|sales.+cssr') AND (NOT (ord_typ IN ('CONNECT', 'V_DISCO', 'NON PAY', 'RESTART')))) AND ("date"(ls_chg_dte_ocr) BETWEEN (SELECT start_date
FROM
  parameters
) AND (SELECT end_date
FROM
  parameters
)))
   ) 
) 
, group_billing_table AS (
   SELECT
     SUB_ACCT_NO_SBB_max
   , "max"(ls_chg_dte_ocr) MAX_ORD_COMP_DATE
   FROM
     billing_table
   WHERE (new_bundle IS NOT NULL)
   GROUP BY SUB_ACCT_NO_SBB_max
) 
, billing_panel AS (
   SELECT
     *
   , "substring"(a.CABLE_UP, 1, 5) cable_bundle
   , "substring"(a.INTERNET_UP, 1, 5) internet_bundle
   , "substring"(a.PHONE_UP, 1, 5) phone_bundle
   , (CASE WHEN ("lower"(a.stb_sc) LIKE '%dvr%') THEN 1 ELSE 0 END) new_dvr_flag
   , salesrep_area sales_area
   , oper_area op_area
   FROM
     ("lcpr.sandbox.dev"."transactions_orderactivity" a
   INNER JOIN group_billing_table b ON ((a.sub_acct_no_ooi = b.SUB_ACCT_NO_SBB_max) AND (a.ls_chg_dte_ocr = b.MAX_ORD_COMP_DATE)))
   WHERE (((("date"(b.MAX_ORD_COMP_DATE) >= "date"('2022-11-20')) AND "regexp_like"("lower"(salesrep_area), 'c2max|teleperformance|dispute|live|nps|assurance|outbound.+project|^retention^|social|tsi|voip.+cssr|callcenter|sales.+cssr')) AND (NOT (ord_typ IN ('CONNECT', 'V_DISCO', 'NON PAY', 'RESTART')))) AND ("date"(ls_chg_dte_ocr) BETWEEN (SELECT start_date
FROM
  parameters
) AND (SELECT end_date
FROM
  parameters
)))
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
   , b.sales_area
   , b.op_area
   , "date_diff"('day', "date"(a.date_recs), "date"(b.ls_chg_dte_ocr)) day_mig_rec
   FROM
     (join_calls a
   LEFT JOIN billing_panel b ON (a.sub_acct_no = b.sub_acct_no_ooi))
) 
, call_filter AS (
   SELECT *
   FROM
     (
      SELECT
        *
      , "row_number"() OVER (PARTITION BY sub_acct_no ORDER BY (CASE WHEN (day_mig_rec >= 0) THEN 1 ELSE 0 END) DESC, day_mig_rec ASC, date_recs DESC, interaction_start_time DESC) row_num_call
      FROM
        join_billing
   ) 
   WHERE (row_num_call = 1)
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
     ((((call_filter a
   LEFT JOIN package_price_panel b ON (a.pkg_cde = b.package_code))
   LEFT JOIN package_price_panel c ON (a.new_bundle = c.package_code))
   LEFT JOIN front_book d ON (a.pkg_cde = d.csg_codes))
   LEFT JOIN front_book e ON (a.new_bundle = e.csg_codes))
) 
, filter_table AS (
   SELECT
     *
   , (CASE WHEN (((Migration_prev_to_sent = 0) AND (Migration_prev_to_call = 0)) AND (call_after_rec = 1)) THEN 1 ELSE 0 END) MIGRATED
   , (CASE WHEN (recommendedpkg = new_bundle) THEN 1 ELSE 0 END) accept_recommended
   , (CASE WHEN ((((Migration_prev_to_sent = 0) AND ("date"(date_recs) <= "date"(last_interaction_start_time))) AND ("date"(ls_chg_dte_ocr) <= ("date"(date_recs) + INTERVAL  '30' DAY))) AND ("date"(first_interaction_start_time) <= "date"(ls_chg_dte_ocr))) THEN 1 ELSE 0 END) MIGRATED_ONE_MONTH
   FROM
     (
      SELECT
        *
      , (CASE WHEN (NEW_PRICE IS NULL) THEN 0 ELSE (NEW_PRICE - OLD_PRICE) END) Final_ARPU_DIF
      , (CASE WHEN ((NEW_PRICE - OLD_PRICE) > 0) THEN 'UP' ELSE (CASE WHEN ((NEW_PRICE - OLD_PRICE) < 0) THEN 'DOWN' ELSE 'EQUAL' END) END) UP_DOWN
      , (CASE WHEN ("date"(LS_CHG_DTE_OCR) < "date"(date_recs)) THEN 1 ELSE (CASE WHEN (new_bundle IS NOT NULL) THEN 0 END) END) Migration_prev_to_sent
      , (CASE WHEN ("date"(LS_CHG_DTE_OCR) < "date"(first_interaction_start_time)) THEN 1 ELSE (CASE WHEN (new_bundle IS NOT NULL) THEN 0 END) END) Migration_prev_to_call
      , (CASE WHEN ("date"(date_recs) <= "date"(last_interaction_start_time)) THEN 1 ELSE 0 END) call_after_rec
      , (CASE WHEN (new_bundle <> recommendedpkg) THEN 1 ELSE 0 END) MIGRATED_TO_A_DIFFERENT_OFFER
      FROM
        join_package_price
   ) 
) 
, final_table AS (
   SELECT
     *
   , (CASE WHEN (((new_bundle_p_code = '1') OR (new_bundle_p_code = '2')) OR (new_bundle_p_code = '4')) THEN '1' WHEN (((new_bundle_p_code = '3') OR (new_bundle_p_code = '5')) OR (new_bundle_p_code = '6')) THEN '2' WHEN (new_bundle_p_code = '7') THEN '3' ELSE '0' END) p_new_bundle
   , (CASE WHEN (((pkg_cde_p_code = '1') OR (pkg_cde_p_code = '2')) OR (pkg_cde_p_code = '4')) THEN '1' WHEN (((pkg_cde_p_code = '3') OR (pkg_cde_p_code = '5')) OR (pkg_cde_p_code = '6')) THEN '2' WHEN (pkg_cde_p_code = '7') THEN '3' ELSE '0' END) p_pkg_cde
   FROM
     (
      SELECT
        *
      , (CASE WHEN (Final_ARPU_DIF > 0) THEN 1 ELSE 0 END) MIGRATED_HIGH_PRICE
      , (CASE WHEN (new_bundle IS NOT NULL) THEN "substr"(CAST(new_bundle AS varchar), 2, 1) ELSE '0' END) new_bundle_p_code
      , (CASE WHEN (pkg_cde IS NOT NULL) THEN "substr"(CAST(pkg_cde AS varchar), 2, 1) ELSE '0' END) pkg_cde_p_code
      , "row_number"() OVER (PARTITION BY sub_acct_no ORDER BY call_after_rec DESC, Migration_prev_to_sent ASC, Migration_prev_to_call ASC, accept_recommended DESC, date_recs DESC) rownum
      FROM
        filter_table
   ) 
) 
, category_change AS (
   SELECT
     *
   , (CASE WHEN (total_rgu_change = 0) THEN 'upsell' WHEN (total_rgu_change > 0) THEN 'crossell_up' WHEN (total_rgu_change < 0) THEN 'crossell_down' END) category_rgu_migration
   , "date_trunc"('month', "date"(date_recs)) month_recs
   FROM
     (
      SELECT
        *
      , (CASE WHEN (new_bundle IS NOT NULL) THEN (CAST(p_new_bundle AS int) - CAST(p_pkg_cde AS int)) END) total_rgu_change
      FROM
        final_table
      WHERE (rownum = 1)
   ) 
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
   FROM
     (category_change a
   LEFT JOIN dna_info b ON (CAST(a.sub_acct_no AS bigint) = b.sub_acct_no_sbb))
) 
SELECT *
FROM
  final_join
