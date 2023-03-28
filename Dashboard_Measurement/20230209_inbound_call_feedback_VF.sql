with parameters as (
select
--##############################################################
--### Change Date in this line to define period #########
date('2023-02-02') as start_date,
date('2023-02-28') as end_date,
date_trunc('month', date('2022-11-01')) as month_date
--##############################################################
),

inbound_recs as (
  select *,
  cast(SUBSTRING(cast (date as varchar), 1,10) as date) as date_recs,
  case when group_id in ('24','99','67','41','19') then 'control_A' 
  WHEN group_id in ('25','47','79','22','93') THEN 'control_X' else 'offerfit' end as group_type
  from 
  (select * , SUBSTRING(cast(Sub_Acct_No as varchar), 15,16) as group_id 
  from "lcpr.sandbox.dev"."outbound_inbound"
  where date <> '0')
  where cast(SUBSTRING(cast (date as varchar), 1,10) as date) between (select start_date from parameters) and (select end_date from parameters) 
),

interactions_table as (SELECT 	*,
first_value(interaction_start_time) over(partition by account_id order by cast(interaction_start_time as date) ) as first_interaction_start_time,
first_value(interaction_start_time) over(partition by account_id order by cast(interaction_start_time as date) desc) as last_interaction_start_time
 FROM "lcpr.stage.prod"."lcpr_interactions_csg" 
where regexp_like(lower(interaction_channel),'c2max|teleperformance|dispute|live|nps|assurance|outbound.+project|^retention^|social|tsi|voip.+cssr|callcenter|sales.+cssr') and regexp_like(lower(other_interaction_info10),'phone|contact.+center')
and cast(interaction_start_time as date) between (select start_date from parameters) and (select end_date from parameters) 
),

interactions_by_account as (
select account_id,
min(cast(first_interaction_start_time as date)) as first_interaction_start_time, max(cast(last_interaction_start_time as date)) as last_interaction_start_time
from interactions_table
group by 1),

join_calls as (
select a.*, b.account_id as account_call, b.first_interaction_start_time, b.last_interaction_start_time, cast(b.interaction_start_time as date) as interaction_start_time
from inbound_recs a
left join interactions_table b --interactions_by_account b
on a.Sub_Acct_No = cast(b.account_id as bigint) 
where cast(b.interaction_start_time as date) >= date(a.date_recs) and cast(b.interaction_start_time as date) <= date(a.date_recs) + interval '30' day 
),

billing_table as(
    select sub_acct_no_ooi as SUB_ACCT_NO_SBB_max, ls_chg_dte_ocr, --max(ls_chg_dte_ocr) as MAX_ORD_COMP_DATE,
    CABLE_UP, INTERNET_UP, PHONE_UP,
    
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
    from (
        select *,
        SUBSTRING(CABLE_UP,1,5) as cable_bundle,
        SUBSTRING(INTERNET_UP,1,5) as internet_bundle,
        SUBSTRING(PHONE_UP,1,5) as phone_bundle
        FROM "lcpr.sandbox.dev"."transactions_orderactivity"
        where regexp_like(lower(salesrep_area),'c2max|teleperformance|dispute|live|nps|assurance|outbound.+project|^retention^|social|tsi|voip.+cssr|callcenter|sales.+cssr')
        and ord_typ not in ('CONNECT','V_DISCO' ,'NON PAY','RESTART') and date(ls_chg_dte_ocr) between (select start_date from parameters) and (select end_date from parameters)
        )
),

group_billing_table as (
select SUB_ACCT_NO_SBB_max,max(ls_chg_dte_ocr) as MAX_ORD_COMP_DATE
from billing_table
where new_bundle is not null
group by SUB_ACCT_NO_SBB_max
),

billing_panel as(
    select *, SUBSTRING(a.CABLE_UP,1,5) as cable_bundle, SUBSTRING(a.INTERNET_UP,1,5) as internet_bundle, SUBSTRING(a.PHONE_UP,1,5) as phone_bundle,salesrep_area as sales_area,oper_area as op_area
    FROM "lcpr.sandbox.dev"."transactions_orderactivity" a join group_billing_table b 
    ON a.sub_acct_no_ooi  = b.SUB_ACCT_NO_SBB_max and a.ls_chg_dte_ocr = b.MAX_ORD_COMP_DATE
    WHERE date(b.MAX_ORD_COMP_DATE) >= date('2022-11-20')
    AND regexp_like(lower(salesrep_area),'c2max|teleperformance|dispute|live|nps|assurance|outbound.+project|^retention^|social|tsi|voip.+cssr|callcenter|sales.+cssr')
    and  ord_typ not in ('CONNECT','V_DISCO' ,'NON PAY','RESTART') and date(ls_chg_dte_ocr) between (select start_date from parameters) and (select end_date from parameters)
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
    else null end as new_bundle,b.sales_area,b.op_area, DATE_DIFF('day',date(a.date_recs),date(b.ls_chg_dte_ocr)) as day_mig_rec
FROM join_calls a left join billing_panel b
on a.sub_acct_no = b.sub_acct_no_ooi
),

call_filter as (
select * from (
select *, row_number() over (partition by sub_acct_no order by (case when day_mig_rec>=0 then 1 else 0 end) desc,
day_mig_rec, date_recs DESC,interaction_start_time DESC) as row_num_call
from join_billing) 
WHERE row_num_call = 1
),

package_price_panel as (
select package_code, arbitrary(stmt_descr_line1l_pkg) as stmt_descr_line1l_pkg,arbitrary(online_desc_pkg) as online_desc_pkg, arbitrary(total_chrg_pkg) as total_chrg_pkg
from "lcpr.sandbox.dev"."servicepackagedictionary"
group by 1),

join_package_price as (
    select a.*, 
    b.total_chrg_pkg as OLD_PRICE,
    c.total_chrg_pkg as NEW_PRICE
    from call_filter a
    left join package_price_panel b
    on a.pkg_cde = b.package_code
    left join package_price_panel c
    on a.new_bundle = c.package_code
),
 
filter_table as (
select *,
    case when Migration_prev_to_sent = 0 and Migration_prev_to_call = 0 and call_after_rec = 1 then 1 else 0 end as MIGRATED,
    case when (recommendedpkg = new_bundle) then 1 else 0 end as accept_recommended,
    case when Migration_prev_to_sent = 0 and date(date_recs) <= date(last_interaction_start_time) and date(ls_chg_dte_ocr) <= date(date_recs) + interval '30' day and date(first_interaction_start_time) <= date(ls_chg_dte_ocr)
    then 1 else 0 end as MIGRATED_ONE_MONTH
       -- case when UP_DOWN = 'DOWN' THEN 'A. DOWNGRADE' ELSE (CASE WHEN UP_DOWN = 'EQUAL' THEN 'B. NO PRICE INCREASE' ELSE 
   -- (CASE WHEN Final_ARPU_DIF - payment_dif<0 THEN 'C. MIG TO PLAN LOWER THAN TARGET' ELSE 'D. MIG TO TARGET' END)END) END AS Category_Migration
from 
(select *,
case when NEW_PRICE is null then 0 else NEW_PRICE - OLD_PRICE end as Final_ARPU_DIF,
 case when NEW_PRICE - OLD_PRICE >0 then 'UP' else (case when NEW_PRICE - OLD_PRICE <0 then 'DOWN'ELSE 'EQUAL' end) end as UP_DOWN,
case when date(LS_CHG_DTE_OCR) <date(date_recs) THEN 1 ELSE (case when new_bundle IS NOT NULL THEN 0 end) end as Migration_prev_to_sent,
case when date(LS_CHG_DTE_OCR) < date(first_interaction_start_time) THEN 1 ELSE (case when new_bundle IS NOT NULL THEN 0 end) end as Migration_prev_to_call,
case when date(date_recs) <= date(last_interaction_start_time) THEN 1 ELSE 0 end as call_after_rec,
case when new_bundle<>recommendedpkg THEN 1 ELSE 0 end as MIGRATED_TO_A_DIFFERENT_OFFER
from join_package_price)
  ),
  
final_table as (
select *,
case when new_bundle_p_code = '1' or new_bundle_p_code = '2' or new_bundle_p_code = '4' then '1'
when new_bundle_p_code = '3' or new_bundle_p_code = '5' or new_bundle_p_code = '6' then '2'
when new_bundle_p_code = '7' then '3' else '0' end as p_new_bundle,
case when pkg_cde_p_code = '1' or pkg_cde_p_code = '2' or pkg_cde_p_code = '4' then '1'
when pkg_cde_p_code = '3' or pkg_cde_p_code = '5' or pkg_cde_p_code = '6' then '2'
when pkg_cde_p_code = '7' then '3' else '0'end as p_pkg_cde
    from (select *, Case when Final_ARPU_DIF >0 THEN 1 ELSE 0 END AS MIGRATED_HIGH_PRICE,
    case when new_bundle is not null then substr(cast(new_bundle as varchar),2,1) else '0' end as new_bundle_p_code,
    case when pkg_cde is not null then substr(cast(pkg_cde as varchar),2,1) else '0' end as pkg_cde_p_code,
    row_number() over (partition by sub_acct_no order by call_after_rec desc,  Migration_prev_to_sent, Migration_prev_to_call, accept_recommended desc, date_recs DESC ) as rownum
from filter_table
)
 )  ,

category_change as (
select *, case when total_rgu_change = 0 then 'upsell'
when total_rgu_change > 0 then 'crossell_up'
when total_rgu_change < 0 then 'crossell_down' end as category_rgu_migration,
date_trunc('month', date(date_recs)) end as month_recs
from
(select *,
case when new_bundle is not null then cast(p_new_bundle as int) - cast(p_pkg_cde as int) end as total_rgu_change
from final_table 
 
where rownum = 1)
),

---select * from category_change

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
where sub_acct_no_sbb in (select cast(sub_acct_no as bigint) from category_change)
)
group by 1
),

final_join as (
select a.*, b.res_city_hse,b.addr1_hse,b.tenure
from category_change a
left join dna_info b
on cast(a.sub_acct_no as bigint) = b.sub_acct_no_sbb
)

select * from final_join
