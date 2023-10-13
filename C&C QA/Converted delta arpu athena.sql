with 
communication_hist as 
(
SELECT 
    *
FROM 
    "db_dev_cdp_project"."communications_hist" 
where 
    -- day='20230830'-- vista del día
    comm_dt >= date('2023-08-29')
    and conv_flg = true
    -- and channel = 'Outbound_call'
    and channel = 'email'
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
        order_no_ooi,
        CASE 
            WHEN ord_typ <> 'DOWNGRADE' THEN SUBSTRING(PHONE_UP,1,5) 
            ELSE SUBSTRING(phone_down,1,5) END as phone_bundle,
        CASE    
            WHEN ord_typ <> 'DOWNGRADE' THEN SUBSTRING(CABLE_UP,1,5)
            ELSE SUBSTRING(CABLE_DOWN,1,5) END AS cable_bundle,
        CASE 
            WHEN ord_typ <> 'DOWNGRADE' THEN SUBSTRING(INTERNET_UP,1,5) 
            ELSE SUBSTRING(INTERNET_DOWN,1,5) END as internet_bundle,
        (CASE WHEN ("lower"(stb_sc) LIKE '%dvr%') THEN 1 ELSE 0 END) new_dvr_flag
    FROM "db-stage-dev"."transactions_orderactivity"
    -- WHERE order_no_ooi IN (1001142244081102, 1001142239001107)
    -- limit 100
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
    round(sum(assets.chrg_pks), 2) as total_pkg_price
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
        offer_from_pck_code,
        cmp1_upselling_offerfit_discount as discount,
        cmp1_upselling_offerfit_to_csg_code as recomm_pkg
    from "db_dev_cdp_project"."emarsys_contact_data"
    where lcpr_aiq_id is not null
    -- limit 100
),

from_to_pck_migrations as
(
    select 
        distinct 
        account_id,
        sent_dt,
        contact_dt,
        conversion_dt,
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
        else null end as new_bundle,
        new_dvr_flag,
        cast(discount as double) as discount,
        recomm_pkg
        
    from  communication_hist
        LEFT join from_pck_code on communication_hist.account_id = from_pck_code.lcpr_aiq_id 
        LEFT join transaction_order on order_id = cast(transaction_order.order_no_ooi as varchar)
    -- where conv_flg = true
),

final_delta_arpu as 
(
select 
    from_to_pck_migrations.*,
    from_pck_price.total_pkg_price as from_price,
    -- to_pck_price.total_pkg_price as to_price,
    CASE WHEN (from_to_pck_migrations.new_dvr_flag = 1) THEN (to_pck_price.total_pkg_price + 5) ELSE (to_pck_price.total_pkg_price) END AS TO_PRICE_DVR,
    -- to_pck_price.total_pkg_price - from_pck_price.total_pkg_price as delta_arpu,
    CASE 
        WHEN new_bundle = recomm_pkg then 
            (
                CASE 
                    WHEN (from_to_pck_migrations.new_dvr_flag = 1) and discount = 15 THEN (to_pck_price.total_pkg_price + 5 - (15/12)) -from_pck_price.total_pkg_price
                    WHEN (from_to_pck_migrations.new_dvr_flag = 0) and (discount = 15) then (to_pck_price.total_pkg_price - (15/12)) - from_pck_price.total_pkg_price 
                    WHEN (from_to_pck_migrations.new_dvr_flag = 1) THEN (to_pck_price.total_pkg_price + 5) - from_pck_price.total_pkg_price 
                    ELSE (to_pck_price.total_pkg_price) - from_pck_price.total_pkg_price   END
            )
        WHEN (from_to_pck_migrations.new_dvr_flag = 1)  THEN (to_pck_price.total_pkg_price + 5) - from_pck_price.total_pkg_price
        WHEN (from_to_pck_migrations.new_dvr_flag = 0) THEN to_pck_price.total_pkg_price - from_pck_price.total_pkg_price END as delta_arpu_adjusted

from from_to_pck_migrations 
    LEFT join assets_pck_price from_pck_price on offer_from_pck_code = from_pck_price.pkg_cde_pks
    LEFT join assets_pck_price to_pck_price on new_bundle = to_pck_price.pkg_cde_pks
)

select 
    *
from final_delta_arpu
