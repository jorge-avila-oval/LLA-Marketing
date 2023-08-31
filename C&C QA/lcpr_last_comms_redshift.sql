SELECT 
    -- count(distinct account_id)
    account_id,
    CAST(DATEADD(SECOND, lst_conv_email_dt/1000,'1970/1/1') AS DATE) as lst_conv_email_date,
    lst_conv_email_dt as lst_conv_email_dt_ms,
    CAST(DATEADD(SECOND, lst_email_sent_dt/1000,'1970/1/1') AS DATE) as lst_email_sent_date,
    lst_email_sent_dt as lst_email_sent_dt_ms,
    CAST(DATEADD(SECOND, lst_email_contacted_dt/1000,'1970/1/1') AS DATE) as lst_email_contacted_date,
    lst_email_contacted_dt as lst_email_contacted_dt_ms
FROM "prod"."public"."lcpr_last_comms"
WHERE 
  lst_email_sent_date = DATE('2023-08-30')
  -- lst_email_sent_date >= DATE('2023-08-29')
