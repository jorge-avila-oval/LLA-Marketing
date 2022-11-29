-- Ejercicio 2 -----------------------------------------------------------
WITH comparativo as(
SELECT 
    dt,-- fecha estraida la info
    DAY(DATE(dt)) as dia,
    MONTH(DATE(dt)) as mes,
    act_acct_cd,-- identificador de cliente
    lag(DATE(dt),1) OVER (PARTITION BY act_acct_cd ORDER BY MONTH(DATE(dt)), DAY(DATE(dt))) AS RETRASO
FROM "db-analytics-prod"."fixed_cwp"
WHERE fi_outst_age < 90 -- clientes residenciales con menos de 90 días de mora del año y pertenezcan al mes de julio (07) y agosto (08)
    AND act_cust_typ_nm = 'Residencial' 
    AND year(date(dt)) = 2022
    AND (month(date(dt)) = 7 OR month(date(dt)) = 8)
),
dif_fecha as (
SELECT 
    act_acct_cd,
    dt,
    RETRASO,
    date_diff('DAY', RETRASO, date(dt)) AS DIFERENCIA
FROM comparativo

)

SELECT 
    dt,
    COUNT(DISTINCT act_acct_cd)
FROM dif_fecha
WHERE DIFERENCIA > 1
GROUP BY 1 ORDER BY 1
