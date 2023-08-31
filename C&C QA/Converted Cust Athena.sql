with 
communication_hist as 
(
    SELECT 
      * 
    FROM "db_dev_cdp_project"."communications_hist" where day='20230824'
),

contact_data as (
    select 
        lcpr_aiq_id,
        lcpr_lla_id
        externalid,
        offer_from_pck_code
    FROM "db_dev_cdp_project"."emarsys_contact_data"
),

transaction_order as 
(
    SELECT 
        month_amt_ocr - bef_value_amt as delta_arpu_TO,
        SUBSTRING(PHONE_UP,1,5) as phone_bundle,
        SUBSTRING(CABLE_UP,1,5) as cable_bundle,
        SUBSTRING(INTERNET_UP,1,5) as internet_bundle,
        *
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
    assets."pkg_cde_pks",
    sum(assets.chrg_pks) as total_pkg_price
from "db-stage-prod"."insights_assets_lcpr" assets 
    inner join assets_max_dt on assets.pkg_cde_pks = assets_max_dt.pkg_cde_pks and 
    dt = max_dt
group by 1
)

select 
    delta_arpu_TO,
    total_pkg_price as final_price
from  transaction_order
    -- inner join communication_hist on communication_hist.order_id = cast(transaction_order.order_no_ooi as varchar)
    left join assets_pck_price on (phone_bundle = pkg_cde_pks or cable_bundle = pkg_cde_pks or internet_bundle = pkg_cde_pks)
