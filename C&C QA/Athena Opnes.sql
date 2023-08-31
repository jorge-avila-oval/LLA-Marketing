SELECT 
contact_id,
launch_id,
domain,
email_sent_at,
campaign_type,
platform,
md5,
is_mobile,
is_anonymized,
uid,
ip,user_agent,
generated_from,
campaign_id,
message_id,
event_time,
customer_id,
partitiontime,
loaded_at,
month,
day
 FROM "db_dev_cdp_project"."feedback_email_opens" 
 where date(email_sent_at) >= date ('2023-08-29')
