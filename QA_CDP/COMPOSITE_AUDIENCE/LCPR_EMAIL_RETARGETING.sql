WITH csr_attributes as ( 
SELECT  
account_id as numero_cuenta, 
as_of as csr_dte 
from "prod"."public"."insights_customer_services_rates_lcpr" 
WHERE AS_OF = (select max(as_of) from "prod"."public"."insights_customer_services_rates_lcpr"))

, 

LAST_COMMS_42 AS ( 
    SELECT  
        account_id,
        lst_cc_out_sent_dt, 
        CAST(DATEADD(SECOND, lst_cc_out_sent_dt/1000,'1970/1/1') AS DATE) AS date,
        CURRENT_DATE - 42 AS period,
        CASE WHEN date >= period then true else false end as condition
    FROM 
         "prod"."public"."lcpr_last_comms")


,
LAST_COMMS_1M AS ( 
SELECT 
account_id, 
lst_cc_out_contacted_dt, 
lst_cc_out_sent_dt, 
CAST(DATEADD(SECOND, lst_cc_out_contacted_dt/1000,'1970/1/1') AS DATE) AS date_contacted, 
CAST(DATEADD(SECOND, lst_cc_out_sent_dt/1000,'1970/1/1') AS DATE) AS date_sent, 
ADD_MONTHS(current_date,-1) AS min_date, 
CASE WHEN date_contacted >=min_date then true else false end as condition1, 
CASE WHEN date_sent >=min_date then true else false end as condition2 
FROM "prod"."public"."lcpr_last_comms") 

,


LAST_COMMS_21 AS ( 
    SELECT
        account_id,
        lst_email_sent_dt, 
        CAST(DATEADD(SECOND, lst_email_sent_dt/1000,'1970/1/1') AS DATE) AS date,
        CURRENT_DATE - 21 AS period,
        CASE WHEN date >= period then true else false end as condition
    FROM 
         "prod"."public"."lcpr_last_comms"
) 


SELECT
COUNT(DISTINCT a.numero_cuenta)
From csr_attributes a
Left JOIN LAST_COMMS_42 b ON a.numero_cuenta = b.account_id
Left JOIN LAST_COMMS_1M c ON a.numero_cuenta = c.account_id
Left JOIN LAST_COMMS_21 d ON a.numero_cuenta = d.account_id
Where 
    -- out_call_sent_6_weeks
    b.condition = true  
    or
    -- out_Call_contacted_1_mont
    (c.condition1 =true and c.condition2 =true ) 
    or  
    -- -- email sent v3 weeks
    d.condition =true
