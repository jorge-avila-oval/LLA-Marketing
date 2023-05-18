CREATE OR REPLACE VIEW "lcpr_clicktoacept_performance" AS 
WITH
  emarsys_table AS (
   SELECT
     account
   , campaign_title
   , email_id
   , clicked
   , total_clicks
   , CAST("date_parse"(date_sent, '%Y%m%d') AS date) date_sent
   , CAST("date_parse"(date_opened, '%Y%m%d') AS date) date_opened
   , CAST("date_parse"(date_clicked, '%Y%m%d') AS date) date_clicked
   , dt
   , CAST("date_parse"(data_extraction_date, '%Y%m%d') AS date) data_extraction_date
   FROM
     (
      SELECT
        account
      , campaign_title
      , email_id
      , clicked
      , total_clicks
      , "substring"(CAST(date_sent AS varchar), 1, 8) date_sent
      , "substring"(CAST(date_opened AS varchar), 1, 8) date_opened
      , "substring"(CAST(date_clicked AS varchar), 1, 8) date_clicked
      , "substring"(CAST(data_extraction_date AS varchar), 1, 8) data_extraction_date
      , dt
      FROM
        "lcpr.emarsys.dev"."offerfit_feedback_view"
   ) 
   WHERE (CAST("date_parse"(date_sent, '%Y%m%d') AS date) >= "date"('2023-04-01'))
) 
, last_register_emarsys AS (
   SELECT a.*
   FROM
     (emarsys_table a
   INNER JOIN (
      SELECT
        campaign_title
      , "max"(dt) max_dt
      FROM
        "lcpr.emarsys.dev"."offerfit_feedback_view" b
      GROUP BY campaign_title
   )  b ON ((a.campaign_title = b.campaign_title) AND (a.dt = b.max_dt)))
) 
, table_offerfit AS (
   SELECT
     *
   , (CASE WHEN (NEW_SPEED = '1 Giga') THEN '1000 Megas' ELSE NEW_SPEED END) NEW_SPEED_MEGAS
   , "regexp_replace"(old_speed, '[^0-9 ]', '') OLD_SPEED_NUMBER
   , "regexp_replace"((CASE WHEN (NEW_SPEED = '1 Giga') THEN '1000 Megas' ELSE NEW_SPEED END), '[^0-9 ]', '') NEW_SPEED_NUMBER
   FROM
     "lcpr.sandbox.dev"."outbound_email"
   WHERE ("date"(date) >= "date"('2023-04-01'))
) 
, join_emarsys_offerfit AS (
   SELECT
     a.*
   , b.sub_acct_no
   , b.regime
   , b.from_csg_code
   , b.to_csg_code
   , CAST(b.payment_dif AS integer) payment_dif
   , b.discount
   , b.old_speed
   , b.NEW_SPEED_MEGAS
   , b.call_to_action
   , b.time_frame
   , b.stb
   , (CAST("trim"(b.NEW_SPEED_NUMBER) AS int) - CAST("trim"(b.OLD_SPEED_NUMBER) AS int)) SPEED_DIF
   , "date"(b.date) date
   FROM
     (last_register_emarsys a
   INNER JOIN table_offerfit b ON (CAST(a.account AS bigint) = b.sub_acct_no))
   WHERE (("date_diff"('day', "date"(b.date), "date"(a.date_sent)) <= 1) AND ("date_diff"('day', "date"(b.date), "date"(a.date_sent)) >= 0))
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
      WHERE ((NOT (ord_typ IN ('CONNECT', 'V_DISCO', 'NON PAY', 'RESTART'))) AND (sub_acct_no_ooi IN (SELECT CAST(account AS bigint)
FROM
  join_emarsys_offerfit
)))
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
      INNER JOIN join_emarsys_offerfit b ON (a.SUB_ACCT_NO_SBB_max = CAST(b.account AS bigint)))
      WHERE (("date_diff"('day', "date"(b.date_sent), "date"(a.ls_chg_dte_ocr)) <= 30) AND ("date_diff"('day', "date"(b.date_sent), "date"(a.ls_chg_dte_ocr)) >= 0))
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
   WHERE (("date"(b.MAX_ORD_COMP_DATE) >= "date"('2023-04-01')) AND (NOT (ord_typ IN ('CONNECT', 'V_DISCO', 'NON PAY', 'RESTART'))))
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
     (join_emarsys_offerfit a
   LEFT JOIN billing_panel b ON (CAST(a.account AS bigint) = b.sub_acct_no_ooi))
) 
, package_price_panel AS (
   SELECT
     package_code
   , "arbitrary"(stmt_descr_line1l_pkg) stmt_descr_line1l_pkg
   , "arbitrary"(online_desc_pkg) online_desc_pkg
   , "max"(total_chrg_pkg) total_chrg_pkg
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
   LEFT JOIN package_price_panel b ON (a.from_csg_code = b.package_code))
   LEFT JOIN package_price_panel c ON (a.new_bundle = c.package_code))
   LEFT JOIN front_book d ON (a.from_csg_code = d.csg_codes))
   LEFT JOIN front_book e ON (a.new_bundle = e.csg_codes))
) 
, order_repeated_reg AS (
   SELECT
     *
   , "row_number"() OVER (PARTITION BY account ORDER BY exact_target ASC, NEW_PRICE DESC, date_trans_diff DESC, date_sent DESC) rown
   FROM
     (
      SELECT
        *
      , (CASE WHEN (TO_CSG_CODE = new_bundle) THEN 1 ELSE 2 END) exact_target
      , (CASE WHEN ("date_diff"('day', "date"(date_sent), "date"(ls_chg_dte_ocr)) >= 0) THEN 1 ELSE 0 END) date_trans_diff
      FROM
        join_package_price
   ) 
) 
, Final_table AS (
   SELECT
     *
   , "concat"('t', account) t_account
   , (CASE WHEN (new_bundle IS NOT NULL) THEN "substr"(CAST(new_bundle AS varchar), 2, 1) ELSE '0' END) new_bundle_p_code
   , (CASE WHEN (from_csg_code IS NOT NULL) THEN "substr"(CAST(from_csg_code AS varchar), 2, 1) ELSE '0' END) from_csg_code_p_code
   FROM
     order_repeated_reg
   WHERE (((new_bundle <> 'RJ2F5') OR (new_bundle IS NULL)) AND (rown = 1))
) 
, Analytics_columns AS (
   SELECT
     *
   , (CASE WHEN (Migration_prev_to_sent = 0) THEN 1 ELSE 0 END) MIGRATED
   , (CASE WHEN ((Migration_prev_to_sent = 0) AND ("date"(ls_chg_dte_ocr) <= ("date"(date_sent) + INTERVAL  '30' DAY))) THEN 1 ELSE 0 END) MIGRATED_ONE_MONTH
   , (CASE WHEN (((Migration_prev_to_sent = 0) AND (discount = 15)) AND (new_bundle = TO_CSG_CODE)) THEN 1 ELSE 0 END) discount_applied
   , (CASE WHEN (UP_DOWN = 'DOWN') THEN 'A. DOWNGRADE' ELSE (CASE WHEN (UP_DOWN = 'EQUAL') THEN 'B. NO PRICE INCREASE' ELSE (CASE WHEN ((PRICE_DIF - payment_dif) < 0) THEN 'C. MIG TO PLAN LOWER THAN TARGET' ELSE 'D. MIG TO TARGET' END) END) END) Category_price_migration
   , (CASE WHEN (PRICE_DIF > 0) THEN 1 ELSE 0 END) POSITIVE_PRICE_DIF_FLAG
   , (CASE WHEN (PRICE_DIF < 0) THEN 1 ELSE 0 END) NEGATIVE_PRICE_DIF_FLAG
   , (CASE WHEN (new_bundle IS NOT NULL) THEN (CAST(p_new_bundle AS int) - CAST(p_from_csg_code AS int)) END) total_rgu_change
   FROM
     (
      SELECT
        *
      , (CASE WHEN (NEW_PRICE IS NULL) THEN 0 ELSE (NEW_PRICE - OLD_PRICE) END) PRICE_DIF
      , (CASE WHEN ((NEW_PRICE - OLD_PRICE) > 0) THEN 'UP' ELSE (CASE WHEN ((NEW_PRICE - OLD_PRICE) < 0) THEN 'DOWN' ELSE 'EQUAL' END) END) UP_DOWN
      , (CASE WHEN (regime = 'control_x') THEN 'control_x' ELSE (CASE WHEN (regime = 'control_j') THEN 'control_j' ELSE (CASE WHEN (discount = 1.5E1) THEN 'offerfit_disc' ELSE 'offer_fit_no_disc' END) END) END) Final_Group
      , (CASE WHEN ((discount = 1.5E1) AND (new_bundle = TO_CSG_CODE)) THEN ((((NEW_PRICE - OLD_PRICE) * 12) - 15) / 12) ELSE (CASE WHEN (NEW_PRICE IS NULL) THEN 0 ELSE (NEW_PRICE - OLD_PRICE) END) END) Final_ARPU_DIF
      , (CASE WHEN (date_opened IS NULL) THEN 0 ELSE 1 END) OPEN
      , (CASE WHEN (date_clicked IS NULL) THEN 0 ELSE 1 END) CLICK
      , (CASE WHEN (CAST(ls_chg_dte_ocr AS date) < date_sent) THEN 1 ELSE (CASE WHEN (new_bundle IS NOT NULL) THEN 0 END) END) Migration_prev_to_sent
      , (CASE WHEN (TO_CSG_CODE <> new_bundle) THEN 1 ELSE 0 END) MIGRATED_TO_A_DIFFERENT_OFFER
      , (CASE WHEN (((new_bundle_p_code = '1') OR (new_bundle_p_code = '2')) OR (new_bundle_p_code = '4')) THEN '1' WHEN (((new_bundle_p_code = '3') OR (new_bundle_p_code = '5')) OR (new_bundle_p_code = '6')) THEN '2' WHEN (new_bundle_p_code = '7') THEN '3' ELSE '0' END) p_new_bundle
      , (CASE WHEN (((from_csg_code_p_code = '1') OR (from_csg_code_p_code = '2')) OR (from_csg_code_p_code = '4')) THEN '1' WHEN (((from_csg_code_p_code = '3') OR (from_csg_code_p_code = '5')) OR (from_csg_code_p_code = '6')) THEN '2' WHEN (from_csg_code_p_code = '7') THEN '3' ELSE '0' END) p_from_csg_code
      FROM
        Final_table
   ) 
) 
, category_rgu_mgration AS (
   SELECT
     *
   , (CASE WHEN (total_rgu_change = 0) THEN 'upsell' WHEN (total_rgu_change > 0) THEN 'crossell_up' WHEN (total_rgu_change < 0) THEN 'crossell_down' END) category_rgu_mgration
   , (CASE WHEN ((Migration_prev_to_sent = 0) OR (Migration_prev_to_sent IS NULL)) THEN 1 ELSE 0 END) contacted
   , (CASE WHEN (((MIGRATED_ONE_MONTH = 1) AND (POSITIVE_PRICE_DIF_FLAG = 1)) AND (Migration_prev_to_sent = 0)) THEN 1 ELSE 0 END) converted
   , (CASE WHEN (((MIGRATED_ONE_MONTH = 1) AND (NEGATIVE_PRICE_DIF_FLAG = 1)) AND (Migration_prev_to_sent = 0)) THEN 1 ELSE 0 END) converted_downgrade
   , (CASE WHEN (MIGRATED = 1) THEN "date_trunc"('month', "date"(ls_chg_dte_ocr)) END) month_migration
   FROM
     Analytics_columns
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
      WHERE (sub_acct_no_sbb IN (SELECT CAST(account AS bigint)
FROM
  category_rgu_mgration
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
     (category_rgu_mgration a
   LEFT JOIN dna_info b ON (CAST(a.account AS bigint) = b.sub_acct_no_sbb))
) 
SELECT *
FROM
  final_join
