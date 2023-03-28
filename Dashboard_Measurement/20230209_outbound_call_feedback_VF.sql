with outbound_recs as (
select *,
cast(SUBSTRING(cast (date as varchar), 1,10) as date) as date_recs
from "lcpr.sandbox.dev"."outbound_call"
where date <> '0'
and cast(SUBSTRING(cast (date as varchar), 1,10) as date) > date('2022-11-18')
order by date desc
),


outbound_feedback as (
select cuenta,
cast(date_parse(intento_discado_1, '%m/%d/%Y') as date) as intento_discado_1,
hora_llamada, disposicion, comentarios
FROM "lcpr.sandbox.dev"."lcpr_outbound_feedback"
where intento_discado_1!=''
--and date(intento_discado_1) >= date('2022-11-20')
--and intento_discado_1 <= '12/26/2022'
order by intento_discado_1 desc
),

outbound_feedback_account as (
select cuenta,
min(intento_discado_1) as intento_discado_1, min(hora_llamada) as hora_llamada, min(disposicion) as disposicion, min(comentarios) as comentarios
from (select cuenta, 
first_value(date(intento_discado_1)) over (partition by cuenta order by date(intento_discado_1) desc) as intento_discado_1,
first_value(hora_llamada) over (partition by cuenta order by date(intento_discado_1) desc) as hora_llamada,
first_value(disposicion) over (partition by cuenta order by date(intento_discado_1) desc) as disposicion,
first_value(comentarios) over (partition by cuenta order by date(intento_discado_1) desc) as comentarios
from outbound_feedback)
where date(intento_discado_1) >= date('2022-11-20')
group by 1

),

outbound_table as (
select a.*, b.*
from outbound_recs a
join outbound_feedback_account  b
on cast(a.sub_acct_no as varchar) = cast(b.cuenta as varchar)
),

billing_table as(
    select sub_acct_no_ooi as SUB_ACCT_NO_SBB_max, ls_chg_dte_ocr--,
    /*CABLE_UP, INTERNET_UP, PHONE_UP,
    
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
    else null end as new_bundle*/
    from (
        select *,
        SUBSTRING(CABLE_UP,1,5) as cable_bundle,
        SUBSTRING(INTERNET_UP,1,5) as internet_bundle,
        SUBSTRING(PHONE_UP,1,5) as phone_bundle
        FROM "lcpr.sandbox.dev"."transactions_orderactivity"
        where ord_typ not in ('CONNECT','V_DISCO' ,'NON PAY','RESTART')
        )
   --- WHERE CHANGED_PACKAGE IS NOT NULL
    --group by SUB_ACCT_NO_SBB_max
),

group_billing_table as (
select SUB_ACCT_NO_SBB_max,max(ls_chg_dte_ocr) as MAX_ORD_COMP_DATE
from (select a.SUB_ACCT_NO_SBB_max,a.ls_chg_dte_ocr 
from billing_table a
join outbound_table b
 on a.SUB_ACCT_NO_SBB_max = cast(b.sub_acct_no as bigint)
     where date_diff('day',date(b.date_recs),date(a.ls_chg_dte_ocr)) <=30 and date_diff('day',date(b.date_recs),date(a.ls_chg_dte_ocr)) >=0
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
FROM outbound_table a left join billing_panel b
on cast(a.sub_acct_no as bigint) = b.sub_acct_no_ooi
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
    on a.pkg_cde = b.package_code
    left join package_price_panel c
    on a.new_bundle = c.package_code
),

order_repeated_reg as (
    select *,
    row_number() over (partition by sub_acct_no order by exact_target,NEW_PRICE desc, date_contacted_diff desc,date_trans_diff desc, date_recs desc) as rown
    from (select *,
    case when recommendedpkg = new_bundle then 1 else 2 end as exact_target,
    case when date_diff('day',date(date_recs),date(intento_discado_1)) >=0 then 1 else 0 end as date_contacted_diff,
    case when date_diff('day',date(intento_discado_1),date(ls_chg_dte_ocr)) >=0 then 1 else 0 end as date_trans_diff
    from join_package_price)
),

Final_table as(
select *,
case when new_bundle is not null then substr(cast(new_bundle as varchar),2,1) else '0' end as new_bundle_p_code,
case when pkg_cde is not null then substr(cast(pkg_cde as varchar),2,1) else '0' end as pkg_cde_p_code
from order_repeated_reg 
where (new_bundle <> 'RJ2F5' or new_bundle is null) and rown = 1
),

analytics_columns as(
    select *,
    case when Migration_prev_to_sent = 0 and Migration_prev_to_call = 0 and call_bef_mig = 1
    then 1 else 0 end as MIGRATED,
    case when Migration_prev_to_sent = 0 and date(date) <= date(intento_discado_1) and date(ls_chg_dte_ocr) <= date(date) + interval '30' day and date(intento_discado_1) <= date(ls_chg_dte_ocr)
    then 1 else 0 end as MIGRATED_ONE_MONTH,
    case when (Migration_prev_to_sent = 0 or Migration_prev_to_sent is null)  and date(date_recs) <= date(intento_discado_1) and 
    regexp_like(lower(disposicion),'ofiti.+cliente.+acepta') then 1 else 0 end as contacted,
    case when Final_ARPU_DIF > 0 THEN 1 ELSE 0 END AS POSITIVE_PRICE_DIF_FLAG,
    CASE WHEN Final_ARPU_DIF < 0 THEN 1 ELSE 0 END NEGATIVE_PRICE_DIF_FLAG,
    case when new_bundle is not null then cast(p_new_bundle as int) - cast(p_from_pkg_cde as int) end as total_rgu_change
    from 
    (select *,
    case when NEW_PRICE - OLD_PRICE >0 then 'UP' else (case when NEW_PRICE - OLD_PRICE <0 then 'DOWN'ELSE 'EQUAL' end) end as UP_DOWN,
    case when NEW_PRICE is null then 0 else NEW_PRICE - OLD_PRICE end as Final_ARPU_DIF,
    case when cast(ls_chg_dte_ocr as date) < date(date) THEN 1 ELSE (case when new_bundle IS NOT NULL THEN 0 end) end as Migration_prev_to_sent,
    case when date(LS_CHG_DTE_OCR) < date(intento_discado_1) THEN 1 ELSE (case when new_bundle IS NOT NULL THEN 0 end) end as Migration_prev_to_call,
    case when date(intento_discado_1) <= date(ls_chg_dte_ocr) THEN 1 ELSE 0 end as call_bef_mig,
    case when RecommendedPkg<>new_bundle THEN 1 ELSE 0 end as MIGRATED_TO_A_DIFFERENT_OFFER,
    case when new_bundle_p_code = '1' or new_bundle_p_code = '2' or new_bundle_p_code = '4' then '1'
    when new_bundle_p_code = '3' or new_bundle_p_code = '5' or new_bundle_p_code = '6' then '2'
    when new_bundle_p_code = '7' then '3' else '0' end as p_new_bundle,
    case when pkg_cde_p_code = '1' or pkg_cde_p_code = '2' or pkg_cde_p_code = '4' then '1'
    when pkg_cde_p_code = '3' or pkg_cde_p_code = '5' or pkg_cde_p_code = '6' then '2'
    when pkg_cde_p_code = '7' then '3' else '0'end as p_from_pkg_cde
    From Final_table)

),

category_change as (
select *, case when total_rgu_change = 0 then 'upsell'
when total_rgu_change > 0 then 'crossell_up'
when total_rgu_change < 0 then 'crossell_down' end as category_rgu_migration
from analytics_columns

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
select a.*, b.res_city_hse,b.addr1_hse,b.tenure,
case when a.MIGRATED_ONE_MONTH= 1 then date_trunc('month', date(a.ls_chg_dte_ocr)) end as month_outbound_migration
from category_change a
left join dna_info b
on cast(a.sub_acct_no as bigint) = b.sub_acct_no_sbb
),

--select * from final_join

mail_migrations as (
select account, date_sent as date_sent_mail,ls_chg_dte_ocr as ls_chg_dte_ocr_mail , month_migration as month_migration_mail
FROM "lcpr.sandbox.dev"."lcpr_clicktoacept_performance"
where MIGRATED_ONE_MONTH = 1
),

join_mail_mig as
(select a.*, b.*
from final_join a
left join mail_migrations b
on a.sub_acct_no = cast(b.account as bigint) --and a.month_recs = b.month_migration 
),

final_table_mail as 
(
select *,
case when account is not null and month_migration_mail >= month_outbound_migration then 1 else 0 end as exclude_mail_migration,
case when account is not null and month_migration_mail < month_outbound_migration then 1 else 0 end as prev_mail_migration
from join_mail_mig 
)

select *, case when MIGRATED_ONE_MONTH = 1 and exclude_mail_migration = 0 and POSITIVE_PRICE_DIF_FLAG =1 and
    regexp_like(lower(disposicion),'ofiti.+cliente.acepta') then 1 else 0 end as converted
, CASE WHEN MIGRATED_ONE_MONTH = 1 AND exclude_mail_migration = 0 AND NEGATIVE_PRICE_DIF_FLAG = 1 AND regexp_like(lower(disposicion), 'ofiti.+cliente.acepta') THEN 1 ELSE 0 END converted_downgrade
from final_table_mail
