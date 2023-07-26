csr_attributes (
    SELECT 
        sub_acct_no_sbb as cust_acct
        as_of as csr_dte,
        CASE WHEN welcome_offer = 'X' THEN true ELSE false END as welcome_offer,
        CASE WHEN acp = 'X' THEN true ELSE false END as subsidize_flag,
        CASE WHEN joint_customer = 'X' THEN true ELSE false END as join_customer,
        CASE 
            WHEN  
                substring(bill_code ,1,1) IN ('R','F') AND 
                substring(bill_code ,2,1) IN ('1','2','3','4','5','6','7') 
                THEN true ELSE false END    as valid_pck,
        CASE 
            WHEN substring(account_id  , length(account_id  )-1,2) in ('24', '99', '67', '41', '19') then 'Control A'
             WHEN substring(account_id  , length(account_id  )-1,2) in ('25', '47', '79', '22', '93') then 'Control X' 
            WHEN  substring(account_id  , length(account_id   )-1,2) in ('26', '48', '80', '23', '94') then 'Control J' ELSE 'Target'
        END as regime,
        addr1_hse as CUST_ADDR1,
        home_phone_sbb as PHONE_1,
        bus_phone_sbb as PHONE_2,
        cyc_cde_sbb as INVOICE_DAY,
        email as EMAIL,
        cust_typ_sbb AS CUST_TYPE,
        delinquency_days AS DELINQUENCY_DAYS
        hsd AS HSD,
        bill_code AS PCK_CODE,
        res_name_sbb AS CUST_NAME
        
    from "db-stage-prod-lf"."insights_customer_services_rates_lcpr" 
    where as_of = date('2023-07-23')
    -- limit 100
)
