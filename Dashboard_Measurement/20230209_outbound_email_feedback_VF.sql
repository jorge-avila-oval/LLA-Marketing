WITH emarsys_table as (
select account, campaign_title, clicked,total_clicks,
cast(date_parse(date_sent, '%Y%m%d') as date) as date_sent,
cast(date_parse(date_opened, '%Y%m%d') as date) as date_opened,
cast(date_parse(date_clicked, '%Y%m%d') as date) as date_clicked,dt,
cast(date_parse(data_extraction_date, '%Y%m%d') as date) as data_extraction_date
from
(select account, campaign_title, clicked,total_clicks,
substring(cast (date_sent as varchar),1,8) as date_sent,
substring(cast (date_opened as varchar),1,8) as date_opened,
substring(cast (date_clicked as varchar),1,8) as date_clicked,
substring(cast (data_extraction_date as varchar),1,8) as data_extraction_date,
dt
FROM "lcpr.emarsys.dev"."offerfit_offer_fit" )
where cast(date_parse(date_sent, '%Y%m%d') as date) >= date('2022-11-23') --and date('2023-02-02')
),

last_register_emarsys as (
SELECT a.* FROM emarsys_table as a
JOIN (
        SELECT campaign_title,  MAX(dt) as max_dt
        FROM "lcpr.emarsys.dev"."offerfit_offer_fit" as b
        GROUP BY campaign_title
    ) as b
    on a.campaign_title = b.campaign_title
    AND a.dt = b.max_dt
),

table_offerfit as (SELECT *, 
CASE WHEN NEW_SPEED = '1 Giga' then '1000 Megas' else NEW_SPEED end as NEW_SPEED_MEGAS,
REGEXP_REPLACE(old_speed,'[^0-9 ]','') AS OLD_SPEED_NUMBER,
REGEXP_REPLACE(CASE WHEN NEW_SPEED = '1 Giga' then '1000 Megas' else NEW_SPEED end,'[^0-9 ]','') AS NEW_SPEED_NUMBER

FROM "lcpr.sandbox.dev"."outbound_email" 
where date(date) >= date('2022-11-23') --and date(('2023-02-02'))
),

join_emarsys_offerfit as(
    SELECT a.*,b.sub_acct_no, b.regime,b.from_csg_code,b.to_csg_code,cast(b.payment_dif as integer) as payment_dif,
    b.discount,b.old_speed,b.NEW_SPEED_MEGAS,b.call_to_action,b.time_frame,
    CAST(trim(b.NEW_SPEED_NUMBER) AS INT) - CAST(trim(b.OLD_SPEED_NUMBER) AS INT) AS SPEED_DIF, date(b.date) as date
    FROM last_register_emarsys a  join table_offerfit b
    ON cast(a.account as bigint)=b.sub_acct_no  
    where date_diff('day',date(b.date),date(a.date_sent)) <=1 and date_diff('day',date(b.date),date(a.date_sent)) >=0
),

billing_table as(
    select sub_acct_no_ooi as SUB_ACCT_NO_SBB_max, ls_chg_dte_ocr
    from (
        select *,
        SUBSTRING(CABLE_UP,1,5) as cable_bundle,
        SUBSTRING(INTERNET_UP,1,5) as internet_bundle,
        SUBSTRING(PHONE_UP,1,5) as phone_bundle
        FROM "lcpr.sandbox.dev"."transactions_orderactivity"
        where ord_typ not in ('CONNECT','V_DISCO' ,'NON PAY','RESTART')
        and sub_acct_no_ooi in (select cast(account as bigint) from join_emarsys_offerfit)
        )
   --- WHERE CHANGED_PACKAGE IS NOT NULL
    --group by SUB_ACCT_NO_SBB_max
),

group_billing_table as (
select SUB_ACCT_NO_SBB_max,max(ls_chg_dte_ocr) as MAX_ORD_COMP_DATE
from (select a.SUB_ACCT_NO_SBB_max,a.ls_chg_dte_ocr 
from billing_table a
join join_emarsys_offerfit b
 on a.SUB_ACCT_NO_SBB_max = cast(b.account as bigint)
     where date_diff('day',date(b.date_sent),date(a.ls_chg_dte_ocr)) <=30 and date_diff('day',date(b.date_sent),date(a.ls_chg_dte_ocr)) >=0
)
--where new_bundle is not null
group by SUB_ACCT_NO_SBB_max
),

billing_panel as(
    select *, SUBSTRING(a.CABLE_UP,1,5) as cable_bundle, SUBSTRING(a.INTERNET_UP,1,5) as internet_bundle, SUBSTRING(a.PHONE_UP,1,5) as phone_bundle
    FROM "lcpr.sandbox.dev"."transactions_orderactivity" a join group_billing_table b 
    ON a.sub_acct_no_ooi  = b.SUB_ACCT_NO_SBB_max and a.ls_chg_dte_ocr = b.MAX_ORD_COMP_DATE
    WHERE date(b.MAX_ORD_COMP_DATE) >= date('2022-11-23') and 
    ord_typ not in ('CONNECT','V_DISCO' ,'NON PAY','RESTART')
),

join_billing AS(
SELECT a.*,b.sub_acct_no_ooi,b.ls_chg_dte_ocr,b.ord_typ,b.cable_bundle,b.internet_bundle,b.phone_bundle,
    b.order_no_ooi as order_id,
    case when cable_bundle is not null and internet_bundle is not null and phone_bundle is not null
    and cable_bundle = internet_bundle and cable_bundle = phone_bundle then cable_bundle
    
    when cable_bundle is not null and internet_bundle is not null and phone_bundle =' '
    and cable_bundle = internet_bundle then cable_bundle
    
    when cable_bundle is not null and internet_bundle =' ' and phone_bundle is not null
    and cable_bundle = phone_bundle then cable_bundle
    
    when cable_bundle =' ' and internet_bundle is not null and phone_bundle is not null
    and internet_bundle = phone_bundle then internet_bundle
    
    when cable_bundle is not null and internet_bundle =' ' and phone_bundle =' ' then cable_bundle
    when internet_bundle is not null and cable_bundle =' ' and phone_bundle =' ' then internet_bundle
    when phone_bundle is not null and internet_bundle =' ' and cable_bundle =' ' then phone_bundle
    else null end as new_bundle
FROM join_emarsys_offerfit a left join billing_panel b
on cast(a.account as bigint) = b.sub_acct_no_ooi
),

package_price_panel as (
select package_code, arbitrary(stmt_descr_line1l_pkg) as stmt_descr_line1l_pkg,arbitrary(online_desc_pkg) as online_desc_pkg, arbitrary(total_chrg_pkg) as total_chrg_pkg
from "lcpr.sandbox.dev"."servicepackagedictionary"
group by 1),

join_package_price as (
    select a.*, 
    b.total_chrg_pkg as OLD_PRICE,
    c.total_chrg_pkg as NEW_PRICE
    from join_billing a
    left join package_price_panel b
    on a.from_csg_code = b.package_code
    left join package_price_panel c
    on a.new_bundle = c.package_code
),

order_repeated_reg as (
    select *,
    row_number() over (partition by account order by exact_target,NEW_PRICE desc, date_trans_diff desc, date_sent desc) as rown
    from (select *,
    case when TO_CSG_CODE = new_bundle then 1 else 2 end as exact_target,
    case when date_diff('day',date(date_sent),date(ls_chg_dte_ocr)) >=0 then 1 else 0 end as date_trans_diff
    from join_package_price)
),

Final_table as(
select *,concat('t',account) as t_account,
case when new_bundle is not null then substr(cast(new_bundle as varchar),2,1) else '0' end as new_bundle_p_code,
case when from_csg_code is not null then substr(cast(from_csg_code as varchar),2,1) else '0' end as from_csg_code_p_code
from order_repeated_reg 
where (new_bundle <> 'RJ2F5' or new_bundle is null) and rown = 1
),

Analytics_columns as(
    select *,
    case when Migration_prev_to_sent = 0 then 1 else 0 end as MIGRATED,
    case when Migration_prev_to_sent = 0 and date(ls_chg_dte_ocr) <= date(date_sent) + interval '30' day then 1 else 0 end as MIGRATED_ONE_MONTH,
    case when Migration_prev_to_sent = 0 and discount = 15 and new_bundle = TO_CSG_CODE then 1 else 0 end as discount_applied,
    case when UP_DOWN = 'DOWN' THEN 'A. DOWNGRADE' ELSE (CASE WHEN UP_DOWN = 'EQUAL' THEN 'B. NO PRICE INCREASE' ELSE 
    (CASE WHEN PRICE_DIF - payment_dif<0 THEN 'C. MIG TO PLAN LOWER THAN TARGET' ELSE 'D. MIG TO TARGET' END)END) END AS Category_price_migration,
    case when PRICE_DIF > 0 THEN 1 ELSE 0 END AS POSITIVE_PRICE_DIF_FLAG,
    (CASE WHEN (PRICE_DIF < 0) THEN 1 ELSE 0 END) NEGATIVE_PRICE_DIF_FLAG,
    case when new_bundle is not null then cast(p_new_bundle as int) - cast(p_from_csg_code as int) end as total_rgu_change
    from 
    (select *,
    case when NEW_PRICE IS NULL THEN 0 ELSE NEW_PRICE - OLD_PRICE END AS PRICE_DIF,
    case when NEW_PRICE - OLD_PRICE >0 then 'UP' else (case when NEW_PRICE - OLD_PRICE <0 then 'DOWN'ELSE 'EQUAL' end) end as UP_DOWN,
    case when regime = 'control_x'  then 'control_x' else (case when discount = 15 then 'offerfit_disc' else 'offer_fit_no_disc' end) end as Final_Group,
    case when discount = 15 and new_bundle = TO_CSG_CODE then ((NEW_PRICE - OLD_PRICE)*12-15)/12 else (case when NEW_PRICE is null then 0 else NEW_PRICE - OLD_PRICE end) end as Final_ARPU_DIF,
    case when date_opened is null then 0 else 1 end as OPEN,
    case when date_clicked is null then 0 else 1 end as CLICK,
    case when cast(ls_chg_dte_ocr as date) < date_sent THEN 1 ELSE (case when new_bundle IS NOT NULL THEN 0 end) end as Migration_prev_to_sent,
    case when TO_CSG_CODE<>new_bundle THEN 1 ELSE 0 end as MIGRATED_TO_A_DIFFERENT_OFFER,
    
    
    
    case when new_bundle_p_code = '1' or new_bundle_p_code = '2' or new_bundle_p_code = '4' then '1'
    when new_bundle_p_code = '3' or new_bundle_p_code = '5' or new_bundle_p_code = '6' then '2'
    when new_bundle_p_code = '7' then '3' else '0' end as p_new_bundle,
    case when from_csg_code_p_code = '1' or from_csg_code_p_code = '2' or from_csg_code_p_code = '4' then '1'
    when from_csg_code_p_code = '3' or from_csg_code_p_code = '5' or from_csg_code_p_code = '6' then '2'
    when from_csg_code_p_code = '7' then '3' else '0'end as p_from_csg_code
    From Final_table)

),
--select count(*), count(distinct account), count(distinct sub_acct_no_ooi)
--select count(*), --count(distinct sub_acct_no),count(distinct account) 

category_rgu_mgration as (
select *, case when total_rgu_change = 0 then 'upsell'
when total_rgu_change > 0 then 'crossell_up'
when total_rgu_change < 0 then 'crossell_down' end as category_rgu_mgration,
CASE WHEN Migration_prev_to_sent = 0 OR Migration_prev_to_sent IS NULL THEN 1 ELSE 0 END as contacted,
case when MIGRATED_ONE_MONTH = 1 and POSITIVE_PRICE_DIF_FLAG =1 and Migration_prev_to_sent = 0  then 1 else 0 end as POSITIVE_MIGRATION,
CASE WHEN Migration_prev_to_sent = 0 OR Migration_prev_to_sent IS NULL THEN 1 ELSE 0 END as conctacted, 
CASE WHEN MIGRATED_ONE_MONTH = 1 AND POSITIVE_PRICE_DIF_FLAG = 1 AND Migration_prev_to_sent = 0 THEN 1 ELSE 0 END as converted,
  , (CASE WHEN (((MIGRATED_ONE_MONTH = 1) AND (NEGATIVE_PRICE_DIF_FLAG = 1)) AND (Migration_prev_to_sent = 0)) THEN 1 ELSE 0 END) converted_downgrade,
case when MIGRATED = 1 then date_trunc('month', date(ls_chg_dte_ocr)) end as month_migration
from Analytics_columns
),

dna_info as( 
SELECT sub_acct_no_sbb,min(res_city_hse) as res_city_hse,min(addr1_hse) as addr1_hse,
case when min(tenure) between 0 and 0.5 then '0_6_months'
 when min(tenure) > 0.5 and min(tenure) <= 1.0 then '6_12_months'
 when min(tenure) > 1 and min(tenure) <= 2.0 then '1_2_years'
 when min(tenure) > 2.0 then 'more_than_2_years' end as tenure
from (select sub_acct_no_sbb,
FIRST_VALUE(res_city_hse) OVER (PARTITION BY sub_acct_no_sbb ORDER BY dt desc ) as res_city_hse,
FIRST_VALUE(addr1_hse) OVER (PARTITION BY sub_acct_no_sbb ORDER BY dt desc ) as addr1_hse,
FIRST_VALUE(tenure) OVER (PARTITION BY sub_acct_no_sbb ORDER BY dt desc ) as tenure
FROM "lcpr.stage.dev"."customer_services_rate_lcpr"
where sub_acct_no_sbb in (select cast(account as bigint) from category_rgu_mgration)
)
group by 1
),

final_join as (
select a.*, b.res_city_hse,b.addr1_hse,b.tenure
from category_rgu_mgration a
left join dna_info b
on cast(a.account as bigint) = b.sub_acct_no_sbb
)

select * from final_join
