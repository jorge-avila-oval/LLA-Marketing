with 
csr_attributes as (
        SELECT 
        sub_acct_no_sbb as cust_acct,
        as_of as csr_dte,
        CASE WHEN welcome_offer = 'X' THEN true ELSE false END as welcome_offer,
        CASE WHEN acp = 'X' THEN true ELSE false END as subsidize_flag,
        CASE WHEN joint_customer = 'X' THEN true ELSE false END as joint_customer,
        CASE 
            WHEN  
                substring(bill_code ,1,1) IN ('R','F') AND 
                substring(bill_code ,2,1) IN ('1','2','3','4','5','6','7') 
                THEN true ELSE false END    as valid_pckg,
        CASE 
            WHEN substring(cast(sub_acct_no_sbb as varchar)  , length(cast(sub_acct_no_sbb as varchar)  )-1,2) in ('24', '99', '67', '41', '19') then 'Control A'
            WHEN substring(cast(sub_acct_no_sbb as varchar)  , length(cast(sub_acct_no_sbb as varchar)  )-1,2) in ('25', '47', '79', '22', '93') then 'Control X' 
            WHEN  substring(cast(sub_acct_no_sbb as varchar)  , length(cast(sub_acct_no_sbb as varchar)   )-1,2) in ('26', '48', '80', '23', '94') then 'Control J' ELSE 'Target'
        END as regime,
        addr1_hse as CUST_ADDR1,
        home_phone_sbb as PHONE_1,
        bus_phone_sbb as PHONE_2,
        cyc_cde_sbb as INVOICE_DAY,
        email as EMAIL,
        cust_typ_sbb AS CUST_TYPE,
        delinquency_days AS DELINQUENCY_DAYS,
        hsd AS HSD,
        bill_code AS PCK_CODE,
        res_name_sbb AS CUST_NAME
        substring(res_name_sbb, POSITION(',' IN res_name_sbb)+1, length(res_name_sbb) - POSITION(',' IN res_name_sbb)) as cust_first_name,
        substring(res_name_sbb, 1, POSITION(',' IN res_name_sbb)-1) as cust_last_name
            from "db-stage-prod"."insights_customer_services_rates_lcpr" 
    where as_of =  current_date - interval '3' day -- valido para los clientes de 30-07-2023
    -- limit 100
),

offers_attributes as (
select 
    max(pkg_cde) as from_pck_code,
    MAX (CASE WHEN lower(channel ) = 'email' then hsd_service ELSE NULL END ) as from_hsd_speed,
    MAX (CASE WHEN lower(channel ) = 'email' then recommendedpkg ELSE NULL END ) as email_to_pck_code,
    MAX (CASE WHEN lower(channel ) = 'email' THEN tocsgcodefriendlyname ELSE NULL END ) as EMAIL_TO_HSD_SPEED,
    MAX (CASE WHEN lower( channel )= 'email' then discount ELSE NULL END) as EMAIL_DISCOUNT,
    MAX ( CASE WHEN lower(offer_type ) = 'single' THEN date_recomm_ms ELSE NULL END ) as SINGLE_REC_DTE ,
    MAX ( CASE WHEN lower(offer_type ) = 'multiple' THEN date_recomm_ms ELSE NULL END ) as MULTIPLE_REC_DTE ,
    rank as RANK_ORDER,
    MAX (CASE WHEN lower( channel )= 'email' then stb ELSE NULL END) as EMAIL_STB_CODE,
    MAX (CASE WHEN lower( channel )= 'email' then delta_arpu ELSE NULL END) as EMAIL_PAY_DIFF, 
    max(regime) as regime,
    reward AS reward,
    offer_type AS type,
    lower(channel) as channel, 
    MAX ( CASE WHEN lower( channel ) = 'email' THEN  next_best_action_date_ms ELSE NULL END ) as EMAIL_NEXT_BST_ACTION_DTE,
    MAX (CASE WHEN lower( channel ) = 'email' then time_frame ELSE NULL END) as EMAIL_TIME_FRAME,
    MAX (CASE WHEN lower( channel )= 'email' then additional_param_1 ELSE NULL END) as EMAIL_TEMPLATE_TYPE,
    MAX (CASE WHEN lower( channel )= 'email'   then additional_param_2  ELSE NULL  END) as EMAIL_MESSAGE_TEXT,
    MAX (CASE WHEN lower( channel )= 'email'   then additional_param_3  ELSE NULL  END) as EMAIL_CALL_TO_ACTION,
    MAX (CASE WHEN lower( channel )= 'email'   then additional_param_4 ELSE NULL  END) as EMAIL_MESSAGE_SUBJECT
    case when pkg_cde = (select bill_code from "prod"."public"."insights_customer_services_rates_lcpr" where as_of = date('2023-07-27'))-- se tiene que ajustar la fecha
    then TRUE else FALSE end as REC_SAME_PKG
    MAX (CASE WHEN lower( channel )= 'email' then link ELSE NULL END) as EMAIL_LINK,
    MAX (CASE WHEN lower( channel )= 'email' then link_exp_dt ELSE NULL END) as EMAIL_LINK_EXP_DTE,
    MAX (CASE WHEN lower( channel )= 'email' then link_dt ELSE NULL END) as EMAIL_LINK_DTE
    
from  "prod"."public"."lcpr_offers"  -- ruta de la vista de RS
),

TO_ATTRIBUTES_AWS AS (
    SELECT
        sub_acct_no_ooi AS cust_acct,
        ord_typ AS ord_typ,
        CASE WHEN lower(ord_typ) IN ('downgrade','upgrade','sidegrade') THEN ls_chg_dte_ocr ELSE null END AS migration_dte,
        order_no_ooi AS order_id,
        salesrep_area AS salesrep_area,
        oper_area AS oper_area
    FROM 
        "db-stage-dev"."transactions_orderactivity"
)
-- audienciencia LCPR_CROSS_CUST_ATTRIBUTES
LCPR_CROSS_CUST_ATTRIBUTES as (
select 
    distinct(cust_acct)
from csr_attributes
where
    -- Condiciones correspondientes a la tabla de CSR
    valid_pckg = true and joint_customer = false and welcome_offer = false and subsidize_flag = false and CUST_TYPE = 'RES'  -- condicion de mismo paquete dentro de los recomendados. 
    -- condiciones correpondientes la vista de Redshift
    num_accounts = 1
),

LCPR_CROSS_CUST_BEHAVIOR as (
select 
    distinct(cust_acct)
from csr_attributes
where 
    -- condiciones CSR
    DELINQUENCY_DAYS <=50  and
    -- Condiciones vista redshift

    --condiciones flagging
    open_order = false and
    trouble_call = false
),

LCPR_SINGLE_OFFER_EMAIL_ELIGIBLE as (
    select 
        distinct(cust_acct)
    from csr_attributes --left join offers_attributes on csr_attributes.cust_acct = offers_attributes.cust_acct
    where   
        EMAIL is not null and
        HSD = 1  
        -- condiciones offers
        channel = 'email'
        -- condiciones de la vista de "res_addr_2_hse"
        
),


LCPR_LINK_ELIGIBLE_CRITERIA as ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 
    where 
    -- condiciones offers
    link =! null and
    link_exp_dt is between date('2023-29-07') and date('2023-08-08') and -- correr los proximos 7 a 15 días
    link_dt 
),

LCPR_SINGLE_OFFER_REC_CUST as ( -- audiencia a probar en RS
    select
    distinct (account_id)      
    from 
    where 
        offer_type = 'single' and 
        "rank" = 1
),

LCPR_TARGET_CUST as (
    select 
        distinct(cust_acct)
    from offers_attributes
    where lower(regime) is like ('target')
),

LCPR_CONTROL_CUST as ( 
    select 
        distinct(cust_acct)
    from offers_attributes
    where lower(regime) like ('%control%')
),

LCPR_MULTI_OFFER_OUT_CALL_ELIGIBLE AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 
    -- offers conditions
    where "rank" = 1 and 
    lower(offer_type) = 'multiple' and 
    lower(channel) like('%call center%') and 
    -- flagging
    dnt_call_flag = false
),


LCPR_MULTI_OFFER_REC_CUST as ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from  csr_attributes --left join offers_attributes on csr_attributes.cust_acct = offers_attributes.cust_acct
    where valid_pckg true and
    -- condiciones offers_attributes
    lower(offer_type) = 'multiple' AND
    "rank" = 1
),

LCPR_CONVERTED_CUSTOMERS AS (  
    SELECT 
        account_id
    FROM 
        -- join entre csr y transaction order
    WHERE 
    migration_dte = DATE('2023-07-27') and -- la tabla de transaction order esta con un delay de 3 días por ende si hoy esn 2023-07-30 toda poner la fecha 2023-07-27
    lower(ord_typ) IN ('downgrade','upgrade','sidegrade')
        
)


LCPR_SINGLE_OFFER_EMAIL_CONTACTED_30_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 
        
    where lst_email_contacted_dte >= date('2023-07-23') -- despues de los últimos 30 días se comprende la fecha de los ultimos 30 días 
    
),

LCPR_SINGLE_OFFER_EMAIL_CONTACTED_15_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 
        
    where lst_email_contacted_dte >= date('2023-07-02') -- despues de los últimos 15 días se comprende la fecha de los ultimos 30 días ejemplo para el 2023-07-30
    
),

LCPR_MULTI_OFFER_OUT_CALL_CONTACTED_30_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 
        
    where lst_cc_contacted_dte >= date('2023-07-16') -- despues de los últimos 30 días se comprende la fecha de los ultimos 30 días ejemplo para el 2023-07-30
    
),

LCPR_MULTI_OFFER_OUT_CALL_CONTACTED_30_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 
        
    where lst_cc_contacted_dte >= date('2023-07-02') -- despues de los últimos 30 días se comprende la fecha de los ultimos 30 días ejemplo para el 2023-07-30
    
),

LCPR_MULTI_OFFER_OUT_CALL_CONTACTED_15_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 
        
    where lst_cc_contacted_dte >= date('2023-07-16') -- despues de los últimos 15 días se comprende la fecha de los ultimos 30 días ejemplo para el 2023-07-30
    
),

LCPR_SIDEGRADE_CONVERTED_90_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 

    where sidegrade_migration_dte >= date('2023-05-16')  -- se toman los ultimos 3 meses 
),

LCPR_SIDEGRADE_CONVERTED_180_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 

    where sidegrade_migration_dte >= date('2023-02-16')  -- se toman los ultimos 6 meses 
),

LCPR_UPGRADE_CONVERTED_90_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 

    where sidegrade_migration_dte >= date('2023-05-16')  -- se toman los ultimos 3 meses 
),

LCPR_UPGRADE_CONVERTED_180_DAYS AS ( -- audiencia a probar en RS
    select
        distinct(cust_acct)
    from 

    where sidegrade_migration_dte >= date('2023-02-16')  -- se toman los ultimos 6 meses 
),

LCPR_EMAIL_SENT_15_DAYS as (-- audiencia a probar en RS
    select
        distinct(cust_acct)
    from -- lcpr_last_comms

    where lst_email_sent >= date('2023-07-16')  -- se toman los ultimos 15 días  
),

LCPR_EMAIL_SENT_30_DAYS as (-- audiencia a probar en RS
    select
        distinct(cust_acct)
    from -- lcpr_last_comms

    where lst_email_sent >= date('2023-07-02')  -- se toman los ultimos 30 días  
),

LCPR_CALL_NOT_OFFER_15_DAYS as (
    select
        distinct(cust_acct)
    from -- outbound feedback
    
    where out_call_fbk_call_dte >= date('2023-07-16') and -- se toman los ultimos 15 días  
    out_call_fbk_call_not_offer_made = true
),

LCPR_CALL_NOT_OFFER_30_DAYS as (
    select
        distinct(cust_acct)
    from -- outbound feedback
    
    where out_call_fbk_call_dte >= date('2023-07-02') and -- se toman los ultimos 30 días  
    out_call_fbk_call_not_offer_made = true
),

LCPR_CALL_NOT_ATTEND_15_DAYS as (
    select
        distinct(cust_acct)
    from -- outbound feedback
    
    where out_call_fbk_call_dte >= date('2023-07-16') and -- se toman los ultimos 15 días  
    out_call_fbk_call_not_attend_call = true
),

LCPR_CALL_NOT_ATTEND_15_DAYS as (
    select
        distinct(cust_acct)
    from -- outbound feedback
    
    where out_call_fbk_call_dte >= date('2023-07-16') and -- se toman los ultimos 15 días  
    out_call_fbk_call_not_attend_call = true
),

LCPR_CALL_NOT_ATTEND_15_DAYS as (
    select
        distinct(cust_acct)
    from -- outbound feedback
    
    where out_call_fbk_call_dte >= date('2023-07-16') and -- se toman los ultimos 15 días  
    out_call_fbk_call_not_attend_call = true
),
