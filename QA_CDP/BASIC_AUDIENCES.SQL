-- Basic Audience LCPR_CROSS_CUST_ATTRIBUTES
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
            from "db-stage-prod-lf"."insights_customer_services_rates_lcpr" 
    where as_of = date('2023-07-23')
    -- limit 100
),

-- audienciencia LCPR_CROSS_CUST_ATTRIBUTES
LCPR_CROSS_CUST_ATTRIBUTES as (
select 
    distinct(cust_acct)
from csr_attributes
where
    -- Condiciones correspondientes a la tabla de CSR
    valid_pckg = true and joint_customer = false and welcome_offer = false and subsidize_flag = false and CUST_TYPE = 'RES'  
    -- condiciones correpondientes de Redshift
    
),

LCPR_CROSS_CUST_BEHAVIOR as (
select 
    distinct(cust_acct)
from csr_attributes
where 
    -- condiciones CSR
    DELINQUENCY_DAYS <=50  
    -- Condiciones vista redshift
    --condiciones flagging
),

LCPR_SINGLE_OFFER_EMAIL_ELIGIBLE as (
    select 
        distinct(cust_acct)
    from csr_attributes --left join offers_attributes on csr_attributes.cust_acct = offers_attributes.cust_acct
    where   
        EMAIL is not null and
        HSD = 1  
        -- condiciones offers
        
),

-- LCPR_SINGLE_OFFER_EMAIL_RETARGETING as (
--     select 
--         distinct(cust_acct)
--     from 
-- ),

-- LCPR_LINK_ELIGIBLE_CRITERIA as (
--     select
--         distinct(cust_acct)
--     from 
    -- where 
    --condiciones offers
-- ),

-- LCPR_SINGLE_OFFER_REC_CUST as (
--     select
        
--     from
--     where 
-- ),

-- LCPR_TARGET_CUST as (
--     select 
--         distinct(cust_acct)
--     from offers_attributes
--     where lower(regime) is 'target'
-- ),

-- LCPR_CONTROL_CUST as (
--     select 
--         distinct(cust_acct)
--     from offers_attributes
--     where lower(regime) like ('%control%')
-- ),

-- LCPR_MULTI_OFFER_OUT_CALL_ELIGIBLE AS (
--     select
--         distinct(cust_acct)
--     from 
--     where rank_order = 1 and lower(channel) like('%call center%')
-- ),

-- LCPR_MULTI_OFFER_OUT_CALL_RETARGETING as (
--     select
--     from 
--     where
-- ),

LCPR_MULTI_OFFER_REC_CUST as (
    select
        distinct(cust_acct)
    from  csr_attributes --left join offers_attributes on csr_attributes.cust_acct = offers_attributes.cust_acct
    where valid_pckg true and
    -- condiciones offers_attributes
),

LCPR_SINGLE_OFFER_EMAIL_CONTACTED_CST AS (
    select
        distinct(cust_acct)
    from 
)



select * from LCPR_CROSS_CUST_ATTRIBUTES
limit 100
