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
        CAST(DATEADD(SECOND, contact_dt_ms/1000,'1970/1/1') AS DATE) as contact_date,
        *
    FROM "prod"."public"."lcpr_communications_hst" 
    WHERE sent_date >= date('2023-08-29') and channel = 'email'
)

select 
    contacted_cust.account_id,
    sent_date,
    sent_dt_ms,
    contact_date,
    contact_dt_ms,
    conv_type,
    order_no_ooi,
    ord_date,
    cable_up,
    internet_up,
    phone_up,
    channel,
    datediff(day,contact_date, ord_date)
    -- *
from contacted_cust inner join last_transaction on contacted_cust.account_id = last_transaction.account_id
where 
    ord_date >= contact_date AND
     contact_dt_ms is not null 
     and datediff(day,sent_date, ord_date) <= 15
