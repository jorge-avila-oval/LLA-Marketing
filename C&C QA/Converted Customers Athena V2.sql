with 
communication_hist as 
(
SELECT 
    *
FROM "db_dev_cdp_project"."communications_hist" 
where 
    -- day='20230830'-- vista del día
    comm_dt >= date('2023-08-29')
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
        *,
        from_unixtime(floor(cast(conv_dte as bigint) / 1000))  conv_date_yy_mm_dd
    FROM "db_dev_cdp_project"."aiq_conversion"
    where day > '2023829'
    LIMIT 10
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
),

from_pck_code as (
    select 
        distinct 
        sub_acct_no,
        pkg_cde
    from "db_dev_cdp_project"."lcpr_offers_last"
)

select 
    *
from  aiq_converted_candidates
    inner join communication_hist on communication_hist.account_id = communication_hist.account_id 
where date(conv_date_yy_mm_dd) >= comm_dt
