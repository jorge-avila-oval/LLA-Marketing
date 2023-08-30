SELECT 
    * 
FROM "db_dev_cdp_project"."feedback_email_unsubscribes" 
WHERE  DATE(event_time) >= DATE('2023-08-29')
