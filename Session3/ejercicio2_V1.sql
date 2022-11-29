WITH usuarios_activos AS( -- Tabla con los usuarios activos
SELECT 
    dt,-- fecha estraida la info
    act_acct_cd -- identificador de cliente
FROM "db-analytics-prod"."fixed_cwp"
WHERE fi_outst_age < 90 -- clientes residenciales con menos de 90 días de mora
    AND act_cust_typ_nm = 'Residencial' -- solo se tienen en cuenta los clientes residenciales
    AND year(date(dt)) = 2022
    AND (MONTH(DATE(dt)) = 7 OR MONTH(DATE(dt)) = 8)
),
ultimo_registro as (
SELECT
    FIRST_VALUE(dt) OVER(PARTITION BY act_acct_cd ORDER BY dt DESC) AS dt_ultimo_registro,
    act_acct_cd
FROM usuarios_activos
)
SELECT 
    dt_ultimo_registro,
    COUNT(DISTINCT act_acct_cd)
FROM ultimo_registro
WHERE dt_ultimo_registro < '2022-08-31'
GROUP BY 1 order by 1
