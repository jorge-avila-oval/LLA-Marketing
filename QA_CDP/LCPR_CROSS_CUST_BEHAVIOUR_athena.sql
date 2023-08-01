-- LCPR_CROSS_CUST_BEHAVIOUR CODIGO PARA REALIZAR EN Athena
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
            from "db-stage-prod"."insights_customer_services_rates_lcpr" 
    where as_of = date('2023-07-27') -- valido para los clientes de 30-07-2023
    -- limit 100
),

flagging_attributes as (
select 
    sub_acct_no_sbb as cust_acct,
    case when has_privacy_flag = 'X' then true else false end as privacy_flag ,
    case when has_open_order  = 'X' then true else false end as open_order ,
    case when pending_tc  = 'X' then true else false end as trouble_call ,
    case when is_in_ndnc  = 'X' then true else false end as dnt_call_flag 
from "db-stage-prod"."flagging"  
-- limit 100
)
select 
    count(distinct csr.cust_acct)
from csr_attributes csr left join flagging_attributes flag on csr.cust_acct = flag.cust_acct
where csr.DELINQUENCY_DAYS <= 50 and
    open_order = false and
    trouble_call = false
