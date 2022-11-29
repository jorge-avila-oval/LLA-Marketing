WITH 
ejercicio1 as (
select 
    DATE_TRUNC('MONTH', DATE(dt)) as mes,
    act_acct_cd 
from "db-analytics-prod"."fixed_cwp"
where 
    YEAR(DATE(dt)) = 2022
    and extract (day from cast(dt as timestamp)) = 1
    and act_cust_typ_nm = 'Residencial'
    AND pd_bb_prod_cd is not null 
),
interaccion as (
SELECT 
    account_id,
    -- interaction_purpose_descrip,
    DATE_TRUNC('MONTH', DATE(interaction_start_time)) AS interaction_date
    
FROM "db-stage-prod"."interactions_cwp"
WHERE interaction_purpose_descrip = 'TRUCKROLL'
)

SELECT 
    DISTINCT ejercicio1.mes,
    COUNT(DISTINCT ejercicio1.act_acct_cd) AS num_users_truckroll
FROM ejercicio1 INNER JOIN interaccion ON ejercicio1.act_acct_cd = interaccion.account_id and ejercicio1.mes = interaccion.interaction_date
GROUP BY 1 ORDER BY 1
