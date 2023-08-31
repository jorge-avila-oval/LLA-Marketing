SELECT 
    *
FROM "db_dev_cdp_project"."communications_hist" 
where 
    -- day='20230830'-- vista del día
    comm_dt >= date('2023-08-29') -- vista acumulada
