WITH 
csr_attributes AS (
    SELECT 
        account_id as numero_cuenta,
        sub_acct_no_sbb as cust_acct
    FROM "prod"."public"."insights_customer_services_rates_lcpr"
    WHERE AS_OF = (select max(as_of) from "prod"."public"."insights_customer_services_rates_lcpr" )
),
offers as (
    SELECT 
        *
    FROM "prod"."public"."lcpr_offers"
),

LINK_ELIGIBLE_CRITERIA as (
    SELECT 
        b.account_id 
    FROM
    csr_attributes A
    LEFT JOIN offers B
    on  a.numero_cuenta = b.account_id 
    WHERE 
        link is not null and
        CAST(DATEADD(SECOND, link_dt/1000,'1970/1/1') AS DATE) = current_date and
        CAST(DATEADD(SECOND, link_exp_dt/1000,'1970/1/1') AS DATE) between current_date+7 and current_date + 15
),

TARGET_CUST AS (
    SELECT 
        numero_cuenta
    FROM csr_attributes left join  offers on csr_attributes.numero_cuenta = offers.account_id
    where regime = 'offerfit'

)

SELECT 
    COUNT(DISTINCT numero_cuenta)
FROM csr_attributes
    INNER JOIN LINK_ELIGIBLE_CRITERIA ON csr_attributes.numero_cuenta = LINK_ELIGIBLE_CRITERIA.account_id
