WITH csr_attributes as ( 
SELECT  
account_id as numero_cuenta, 
as_of as csr_dte 
from "prod"."public"."insights_customer_services_rates_lcpr" 
WHERE AS_OF = (select max(as_of) from "prod"."public"."insights_customer_services_rates_lcpr"))

, 

OUT_CALL_6_WEEKS AS ( 
        SELECT
        account_id,
        lst_cc_out_sent_dt, 
        CAST(DATEADD(SECOND, lst_cc_out_sent_dt/1000,'1970/1/1') AS DATE) AS date,
        CURRENT_DATE - 42 AS period,
        CASE WHEN date >= period then true else false end as condition
    FROM 
         "prod"."public"."lcpr_last_comms")


,
OUT_CALL_3_MONTH AS ( 
 SELECT
        account_id AS cuenta,
        lst_cc_out_contacted_dt,
        lst_cc_out_sent_dt,
        CAST(DATEADD(SECOND, lst_cc_out_contacted_dt/1000,'1970/1/1') AS DATE) AS date_contacted,
        CAST(DATEADD(SECOND, lst_cc_out_sent_dt/1000,'1970/1/1') AS DATE) AS date_sent,
        ADD_MONTHS(current_date,-3) AS min_date,
        CASE WHEN date_contacted >=min_date then true else false end as condition1,
        CASE WHEN date_sent >=min_date then true else false end as condition2
    FROM 
         "prod"."public"."lcpr_last_comms")

,


EMAIL_3_WEEKS AS ( 
    SELECT
        account_id AS cuenta2,
        lst_email_sent_dt, 
        CAST(DATEADD(SECOND, lst_email_sent_dt/1000,'1970/1/1') AS DATE) AS date,
        CURRENT_DATE - 21 AS period,
        CASE WHEN date >= period then true else false end as condition
    FROM 
         "prod"."public"."lcpr_last_comms"
),

CONVERTED_6_MONTH AS ( 
    SELECT
        account_id AS numero,
        CASE WHEN lower(ord_typ) IN ('downgrade', 'upgrade', 'sidegrade') THEN ls_chg_dte_ocr ELSE null END AS migration_dte,
        ord_typ,
        case when migration_dte >= DATEADD(month, -6, CURRENT_DATE) THEN true else false end AS condition
    FROM 
        "prod"."public"."transactions_orderactivity"
) 


SELECT
COUNT(DISTINCT a.numero_cuenta)
From csr_attributes a
Left JOIN OUT_CALL_6_WEEKS b ON a.numero_cuenta = b.account_id
Left JOIN OUT_CALL_3_MONTH c ON a.numero_cuenta = c.cuenta
Left JOIN EMAIL_3_WEEKS d ON a.numero_cuenta = d.cuenta2
Left JOIN CONVERTED_6_MONTH e ON a.numero_cuenta = e.numero
Where 
    -- out_call_sent_6_weeks
    b.condition = true  
    or
    -- out_Call_contacted_3_month
    (c.condition1 =true and c.condition2 =true ) 
    or  
    -- -- email_sent_3_weeks
    d.condition =true 
    or
    -- -- converted_6_month
    e.condition=true
