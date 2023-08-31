with last_transaction as (
    select 
        CAST(DATEADD(SECOND, ls_chg_dte_ocr_ms/1000,'1970/1/1') AS DATE) as ord_date, 
        * 
    from "prod"."public"."lcpr_last_transaction_orderactivity" 
    where CAST(DATEADD(SECOND, ls_chg_dte_ocr_ms/1000,'1970/1/1') AS DATE) >= date('2023-08-29')
),

contacted_cust as (
    SELECT 
        CAST(DATEADD(SECOND, sent_dt_ms/1000,'1970/1/1') AS DATE) as sent_date ,
        *
    FROM "prod"."public"."lcpr_communications_hst" 
    WHERE sent_date >= date('2023-08-29') and channel = 'email'
)

select 
    *
from contacted_cust inner join last_transaction on contacted_cust.account_id = last_transaction.account_id
