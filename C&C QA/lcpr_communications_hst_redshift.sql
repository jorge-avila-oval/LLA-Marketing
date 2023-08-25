SELECT 
    account_id,
    channel, 
    campaign,
    use_case,
    CAST(DATEADD(SECOND, sent_dt_ms/1000,'1970/1/1') AS DATE) as sent_dt,
    sent_dt_ms,
    CAST(DATEADD(SECOND, contact_dt_ms/1000,'1970/1/1') AS DATE) as contact_dt,
    contact_dt_ms,
    CAST(DATEADD(SECOND, conv_dt_ms/1000,'1970/1/1') AS DATE) as conv_dt,
    conv_dt_ms,
    conv_type,
    CAST(DATEADD(SECOND, rec_dt_ms/1000,'1970/1/1') AS DATE) as rec_dt, 
    rec_dt_ms,
    order_id,
    conv_use_case_category,
    dt,
    dt_ms
FROM "prod"."public"."lcpr_communications_hst"
WHERE 
    channel = 'email' and sent_dt_ms is not null and 
  -- condición para incluir el nombre de los automation program que enviaron los correos
    campaign  in ('CBM_CDP_OFFERFIT_UPSELLING_TARGET', 'CBM_CDP_OFFERFIT_UPSELLING_CONTROL')
    and dt = '20230824'
