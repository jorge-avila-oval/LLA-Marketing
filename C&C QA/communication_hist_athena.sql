
-- communication history Athena
SELECT 
    account_id,
    sub_acct_no_sbb,
    use_case,
    campaign_name,
    contact_id,
    campaign_id,
    comm_dt,
    sent_dt	cancel_dt,
    cancel_reason,
    bounce_dt,
    dsn_reason,
    bounce_type,
    open_dt	click_dt,
    clicks_count,
    contact_dt,
    id_oferta_int,
    offer_type,
    rec_date,
    regime,
    link,
    link_generated_dt,
    link_expiration_dt,
    order_id,
    conv_use_case_cat,
    conversion_dt,
    conversion_type,
    program_id,
    conv_flg,
    month,
    day,
    channel

FROM "db_dev_cdp_project"."communications_hist" 
where 
    -- day='20230830'-- vista del día
    comm_dt >= date('2023-08-29') -- vista acumulada
    and campaign_name in 
        ('CBM_CDP_OFFERFIT_UPSELLING_TARGET','CBM_CDP_OFFERFIT_UPSELLING_CONTROL')
