with 
communication_hist as 
(
SELECT 
    *
FROM "db_dev_cdp_project"."conv_communications_hist" 
where 
    -- day='20230830'-- vista del día
    comm_dt >= date('2023-08-29')
    and conv_flg = true
),

contact_data as (
    select 
        lcpr_aiq_id,
        lcpr_lla_id, 
        externalid,
        offer_from_pck_code
    FROM "db_dev_cdp_project"."emarsys_contact_data"
    limit 100
),

aiq_converted_candidates as (
    SELECT 
        -- *,
        -- distinct 
        from_unixtime(floor(cast(conv_dte as bigint) / 1000))  conv_date_yy_mm_dd
    FROM "db_dev_cdp_project"."aiq_conversion"
    where date(from_unixtime(floor(cast(conv_dte as bigint) / 1000))) >= date('2023-08-29')
    -- LIMIT 10
),

transaction_order as 
(
    SELECT 
        -- month_amt_ocr - bef_value_amt as delta_arpu_TO,
        order_no_ooi,
        SUBSTRING(PHONE_UP,1,5) as phone_bundle,
        SUBSTRING(CABLE_UP,1,5) as cable_bundle,
        SUBSTRING(INTERNET_UP,1,5) as internet_bundle
    FROM "db-stage-dev"."transactions_orderactivity"
),


assets_max_dt as (
    SELECT 
        DISTINCT pkg_cde_pks,
        MAX(dt) max_dt
    FROM "db-stage-prod"."insights_assets_lcpr"
    group by 1
),

assets_pck_price as 
(
select 
    assets."pkg_cde_pks" as pkg_cde_pks,
    sum(assets.chrg_pks) as total_pkg_price
from "db-stage-prod"."insights_assets_lcpr" assets 
    inner join assets_max_dt on assets.pkg_cde_pks = assets_max_dt.pkg_cde_pks and 
    dt = max_dt
group by 1
),

from_pck_code as (
    select 
        lcpr_lla_id,
        externalid,
        lcpr_aiq_id,
        offer_from_pck_code
    from "db_dev_cdp_project"."emarsys_contact_data"
    where lcpr_aiq_id is not null
    -- limit 100
),

from_to_pck_migrations as
(
    select 
        distinct 
        account_id,
        lcpr_aiq_id,
        order_id,
        offer_from_pck_code,
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
        
    from  communication_hist
        inner join from_pck_code on communication_hist.account_id = from_pck_code.lcpr_aiq_id 
        inner join transaction_order on order_id = cast(transaction_order.order_no_ooi as varchar)
    -- where conv_flg = true
),

final_delta_arpu as 
(
select 
    from_to_pck_migrations.*,
    from_pck_price.total_pkg_price,
    to_pck_price.total_pkg_price,
    to_pck_price.total_pkg_price - from_pck_price.total_pkg_price as delta_arpu
from from_to_pck_migrations 
    inner join assets_pck_price from_pck_price on offer_from_pck_code = from_pck_price.pkg_cde_pks
    inner join assets_pck_price to_pck_price on new_bundle = to_pck_price.pkg_cde_pks
)

select 
    sum (delta_arpu)
from final_delta_arpu
