-- ejercicio 3 ----------------------------------------------------------------------------------------------------------------------------------------
WITH usuarios_activos AS( -- Tabla con los usuarios activos

SELECT 
    dt,-- fecha estraida la info
    act_acct_cd -- identificador de cliente
FROM "db-analytics-prod"."fixed_cwp"
WHERE fi_outst_age < 90 -- clientes residenciales con menos de 90 días de mora
    AND act_cust_typ_nm = 'Residencial' -- solo se tienen en cuenta los clientes residenciales
    AND year(date(dt)) = 2022
),

interaccion as ( -- seleccion de las interacciones 
SELECT 
    account_id, -- id del cliente
    date(interaction_start_time) as date_interaccion -- fecha de creación de la interaccion
    
FROM "db-stage-prod"."interactions_cwp" 

), 
ordenes_desconexion as ( -- seleccion de los usuarios que han tenido ordenes de desconexion
SELECT 
    account_id, -- id del cliente
    DATE(order_start_date) AS order_date -- fecha de creación de la orden de servicio
    
FROM "db-stage-dev"."so_hdr_cwp"
WHERE order_type = 'DEACTIVATION' -- solo se obtienen las ordenes de servicio que son de desconexion

),

union_interaccion as ( -- se hace una union entre los clientes activos y las interacciones
SELECT 
    u.act_acct_cd, -- del cliente 
    date_interaccion -- fecha de creacon de la interaccion
FROM usuarios_activos AS u
INNER JOIN interaccion as inter ON u.act_acct_cd = inter.account_id -- se realiza la inner join paraq obtener solo los clientes activos que hayan tenido interacciones
), 
diferencia_dias as ( -- se realiza una union entre los usuarios activos con interacciones 
SELECT 
    u.act_acct_cd, --id del cliente
    date_diff('DAY',date_interaccion, order_date) as dif_days --diferencia de dias entre lainteraccióny la orden de desconexión
FROM union_interaccion AS u
INNER JOIN ordenes_desconexion AS des ON cast(u.act_acct_cd as BIGINT) = des.account_id -- se obtiene solo os usuiarios que estan activo, han tenido interacciones y generan ordenes de desconexion
)

SELECT 
    COUNT(DISTINCT act_acct_cd) AS num_clientes -- se cuenta el número de clientes que tiene 
FROM diferencia_dias
WHERE dif_days <= 40 and dif_days >= 0 -- solo se tiene en cuenta los clientes con menos de días entre su interaccion y orden de desconexión
