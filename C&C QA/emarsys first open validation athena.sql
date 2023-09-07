-- lcpr opens dates analisis
WITH 
opens as (
    SELECT
        lcpr_aiq_id as account_id,
        contact_id,
        email_sent_at,
        message_id,
        event_time,
        customer_id,
        partitiontime,
        loaded_at,
        month,
        day
    FROM "db_dev_cdp_project"."feedback_email_opens"  opens INNER JOIN "db_dev_cdp_project"."emarsys_id" emarsys_id
        ON cast(contact_id as bigint) = user_id
    WHERE date(email_sent_at) >= date ('2023-08-29')
),

first_opens as (
select 
    distinct account_id,
    min(email_sent_at) as email_sent_at,
    min(event_time) as event_time
from opens
group by 1 
),

communication_hist as (
SELECT 
    *
FROM "db_dev_cdp_project"."conv_communications_hist" 
where 
    -- day='20230830'-- vista del día
    comm_dt >= date('2023-08-29')
    -- and "open_dt" is not null 
)


select 
    communication_hist.account_id,
    sent_dt as sent_dt_comm_hist,
    email_sent_at as email_sent_at_contact_data,
    contact_dt as contact_dt_comm_hist,
    open_dt as open_dt_comm_hist,
    event_time as open_dt_contact_data
from communication_hist inner join first_opens 
    on communication_hist.account_id = first_opens.account_id
