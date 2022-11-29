WITH usuarios_activos AS( -- Tabla con los usuarios activos

SELECT 
    dt,-- fecha estraida la info
    act_acct_cd -- identificador de cliente
FROM "db-analytics-prod"."fixed_cwp"
WHERE fi_outst_age < 90 -- clientes residenciales con menos de 90 días de mora
    AND act_cust_typ_nm = 'Residencial' -- solo se tienen en cuenta los clientes residenciales
    AND year(date(dt)) = 2022
),

llamadas_reclamos AS(
SELECT 
    date(interaction_start_time) as fecha,
    account_id,
    interaction_purpose_descrip,
    interaction_id
FROM "db-stage-prod"."interactions_cwp"
WHERE interaction_purpose_descrip = 'CLAIM'
),
num_llamadas as (
SELECT 
    MONTH(DATE(dt)) AS mes,
    users.act_acct_cd AS usuario,
    COUNT(DISTINCT interaction_id) AS num_llamada
FROM usuarios_activos AS users INNER JOIN llamadas_reclamos AS claims ON claims.account_id = users.act_acct_cd AND month(date(dt)) = month(date(fecha))
GROUP BY 1,2
),
clasificacion as (
SELECT 
    *,
   CASE WHEN num_llamada = 1 THEN  'Single Callers' ELSE 'Repeated Callers' END AS category
FROM num_llamadas
)

SELECT
    mes,
    category,
    COUNT(DISTINCT usuario) as num_users
FROM clasificacion
GROUP BY 1,2 ORDER BY 1
