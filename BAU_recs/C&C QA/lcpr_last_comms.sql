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
where 
    lst_email_sent_date = '2023-08-24' and 
    account_id in (
        'LCPR_FX_8211080560106239',
        'LCPR_FX_8211080610139503',
        'LCPR_FX_8211080650078637',
        'LCPR_FX_8211080700226434',
        'LCPR_FX_8211080700260557',
        'LCPR_FX_8211080730180452',
        'LCPR_FX_8211080730181492',
        'LCPR_FX_8211080760026559',
        'LCPR_FX_8211080850182460',
        'LCPR_FX_8211080870448545',
        'LCPR_FX_8211080880039417',
        'LCPR_FX_8211080900171034',
        'LCPR_FX_8211080910063015',
        'LCPR_FX_8211080920137387',
        'LCPR_FX_8211080920356573',
        'LCPR_FX_8211790170448256',
        'LCPR_FX_8211790190179204',
        'LCPR_FX_8211790230047783',
        'LCPR_FX_8211790260321413',
        'LCPR_FX_8211790280067855',
        'LCPR_FX_8211790360811156',
        'LCPR_FX_8211790370192175',
        'LCPR_FX_8211990013283635',
        'LCPR_FX_8211990014460786',
        'LCPR_FX_8211990014549349',
        'LCPR_FX_8211990014766711',
        'LCPR_FX_8211990021956560',
        'LCPR_FX_8211990022036602',
        'LCPR_FX_8211990030712608',
        'LCPR_FX_8211990031037427',
        'LCPR_FX_8211990040538050',
        'LCPR_FX_8211990051833481',
        'LCPR_FX_8211990051929545',
        'LCPR_FX_8211990052014073',
        'LCPR_FX_8211080670138254',
        'LCPR_FX_8211080700232333',
        'LCPR_FX_8211080700268089',
        'LCPR_FX_8211080750009854',
        'LCPR_FX_8211080750043127',
        'LCPR_FX_8211080870180254',
        'LCPR_FX_8211080870571163',
        'LCPR_FX_8211080880203328',
        'LCPR_FX_8211080880207055',
        'LCPR_FX_8211080900145582',
        'LCPR_FX_8211080910161892',
        'LCPR_FX_8211080920037165',
        'LCPR_FX_8211080920299062',
        'LCPR_FX_8211790200324543',
        'LCPR_FX_8211790250155987',
        'LCPR_FX_8211790330216064',
        'LCPR_FX_8211790360237337',
        'LCPR_FX_8211790410174266',
        'LCPR_FX_8211790430162291',
        'LCPR_FX_8211990014353676',
        'LCPR_FX_8211990014514830',
        'LCPR_FX_8211990030461370',
        'LCPR_FX_8211990031073497',
        'LCPR_FX_8211990031109614',
        'LCPR_FX_8211080520049248',
        'LCPR_FX_8211080920365848',
        'LCPR_FX_8211790360445047',
        'LCPR_FX_8211990014660948',
        'LCPR_FX_8211990030035422',
        'LCPR_FX_8211990051897080',
        'LCPR_FX_8211990051984425',
        'LCPR_FX_8211080840175848',
        'LCPR_FX_8211790170225209',
        'LCPR_FX_8211790360152726',
        'LCPR_FX_8211790360816023',
        'LCPR_FX_8211990021690425'
    )
