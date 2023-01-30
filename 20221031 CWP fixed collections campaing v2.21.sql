--#############################################
---------> Campaña FIJO 2.21 <---------------
--#############################################

--------------------------------------------------------------
--00. Definir parametros de consulta
with parameters as (
SELECT
--------------------------------------------------------------
--############ Cambiar las fechas para consultar pagos #########
CURRENT_DATE as start_date, 
--############ Capacidad diaria de llamadas del callcenter #####
4000 as Daily_agent_cap,
4000 as Daily_candidates_cap,
--############ Consulta último DNA, desactivar en falla DNA ####
(SELECT MAX(CAST(dt as DATE)) FROM "db-analytics-prod"."dna_fixed_cwp" ) as DNA_dt,
--############ Contigencia fecha y mora, activar en falla DNA ##
-- DATE('2022-09-20') as DNA_dt,
--############ Ajuste del overdue age ##
(0) as adjust_age
),
--------------------------------------------------------------

-- ##############################################################

--------------------------------------------------------------
--01. Consulta DNA 
--------------------------------------------------------------
DNA AS (
SELECT *,
    --- payment terms customers --- 
    (fi_outst_age-fi_overdue_age) AS pmnt_terms,
    --- Tech flag customers --- 
    Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'Copper' end as Technology,
    --- identify new invoice ---
    CASE WHEN fi_bill_due_dt_m0 IS NULL  AND  fi_bill_due_dt_m1 < cast(dt as date) THEN fi_bill_due_dt_m1 ELSE fi_bill_due_dt_m0 
    END AS next_due_dt,
    CASE WHEN fi_bill_due_dt_m0 IS NULL  THEN fi_bill_amt_m1 ELSE fi_bill_amt_m0 
    END AS open_invoice
    --- Relevant customers --- 
    FROM "db-analytics-prod"."dna_fixed_cwp"
    WHERE CAST(dt as DATE) = (select DNA_dt from parameters limit 1)
    AND pd_mix_nm != '0P'
    AND act_cust_typ_nm = 'Residencial'
    AND (DATE_DIFF('DAY',CAST(dt as date),CURRENT_DATE)+fi_overdue_age) between -3 and 90 
    
),
DNA_adjst as (
Select
    load_dt, dt,
    act_acct_name as Nombre_Cliente,
    act_acct_cd as id_cliente_unico,
    act_cust_typ as Tipo_Cliente,
    REPLACE(act_acct_stat,'TFS','T') as Status_Cuenta,
    Technology as Tecnologia, 
    (CAST(DATE_DIFF('DAY',CAST(dt as date),CURRENT_DATE) as BIGINT)+(fi_overdue_age+(select adjust_age from parameters))) as Dias_Morosidad,
    --- identificar ultimo día de pago -- 
    CASE WHEN fi_bill_pmnt_dt_m0 Is not null THEN fi_bill_pmnt_dt_m0
        WHEN fi_bill_pmnt_dt_m0 is null THEN fi_bill_pmnt_dt_m1
        WHEN (fi_bill_pmnt_dt_m0 is null AND fi_bill_pmnt_dt_m1 is null) THEN fi_bill_pmnt_dt_m2
    ELSE fi_bill_pmnt_dt_m3
    END AS Fecha_Ult_Pago,
    act_contact_phone_1 as Contacto1,
    act_contact_phone_2 as Contacto2,
    act_contact_phone_3 as Contacto3,
    act_contact_mail_1 AS CorreoElectronico,
    --- Calcular monto vencido ---
    round(CASE WHEN  next_due_dt <= CAST(dt as DATE) THEN fi_outst_amt ELSE (fi_outst_amt-open_invoice) END,2) as Monto_Vencido,
    --total_overdue_amt as Monto_Vencido,
    fi_outst_amt as Monto_Total,
        act_blng_cycl as Ciclo,
    -- DATE(date_parse(substr(CAST(oldest_unpaid_bill_dt as VARCHAR),1,8), '%Y%m%d')) as factura_antigua_no_paga,
    -- DATE(date_parse(substr(CAST(oldest_unpaid_due_dt as VARCHAR),1,8), '%Y%m%d')) as vencimiento_factura_antigua,
    DATE_ADD('day',-fi_outst_age, DATE(dt)) as factura_antigua_no_paga,
    DATE_ADD('day',-fi_overdue_age, DATE(dt)) as vencimiento_factura_antigua,
    -- fi_overdue_age,
    -- fi_outst_age,
    -- pmnt_terms
    COALESCE (pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm) as Plan
from DNA
),
---------------------------------------
--02. Consulta del score modelo de Ptich
---------------------------------------
score_ptich AS (
SELECT decile,score_ptich, act_acct_cd as act_acct_cd_PTICH,week_dt,
CASE 
    WHEN decile <=2 THEN 'High' 
    WHEN CAST(decile as BIGINT) >=3 AND CAST(decile as BIGINT) < 9
    THEN 'Medium' ELSE 'Low' END AS Risk
FROM "db-stage-prod"."ptich_cwp_scores"
WHERE CAST(week_dt as DATE) = (SELECT MAX(CAST(week_dt as DATE)) FROM "db-stage-prod"."ptich_cwp_scores" )
),
----------------------------------------
--03. Consulta de pagos realizados
----------------------------------------
pagos AS (
SELECT account_id, SUM (CAST(payment_amt_usd as DOUBLE)) as Pagos
FROM "db-stage-prod"."payments_cwp"
WHERE CAST(date_parse(create_timestamp, '%Y-%m-%d %H:%i:%s') AS DATE) >= DATE_add('day',-2,CURRENT_DATE )
GROUP BY account_id
),
-----------------------------------------------------------------
--04. reporte de Acqueon sobre contactos realizados durante el mes
-----------------------------------------------------------------
Acqueon_month_contact as (
--4.1 Cruzar con las referencias y equivalentes
SELECT 
x.*,
CASE
    WHEN length(x.businessfield_1) =12 AND try_cast(x.businessfield_1 as BIGINT) > 0 THEN CAST(x.businessfield_1 as VARCHAR)
    WHEN length(x.businessfield_2) =12 AND try_cast(x.businessfield_2 as BIGINT) > 0 THEN CAST(x.businessfield_2 as VARCHAR)
    WHEN length(x.businessfield_3) =12 AND try_cast(x.businessfield_3 as BIGINT) > 0 THEN CAST(x.businessfield_3 as VARCHAR)
    WHEN length(x.businessfield_4) =12 AND try_cast(x.businessfield_4 as BIGINT) > 0 THEN CAST(x.businessfield_4 as VARCHAR)
    WHEN length(x.businessfield_5) =12 AND try_cast(x.businessfield_5 as BIGINT) > 0 THEN CAST(x.businessfield_5 as VARCHAR)
    WHEN length(x.businessfield_6) =12 AND try_cast(x.businessfield_6 as BIGINT) > 0 THEN CAST(x.businessfield_6 as VARCHAR)
    -- WHEN length(x.businessfield_7) =12 AND try_cast(x.businessfield_7 as BIGINT) > 0 THEN CAST(x.businessfield_7 as VARCHAR)
    WHEN length(x.businessfield_8) =12 AND try_cast(x.businessfield_8 as BIGINT) > 0 THEN CAST(x.businessfield_8 as VARCHAR)
    WHEN length(x.businessfield_9) =12 AND try_cast(x.businessfield_9 as BIGINT) > 0 THEN CAST(x.businessfield_9 as VARCHAR)
    WHEN length(x.businessfield_10) =12 AND try_cast(x.businessfield_10 as BIGINT) > 0 THEN CAST(x.businessfield_10 as VARCHAR)
    WHEN length(x.businessfield_11) =12 AND try_cast(x.businessfield_11 as BIGINT) > 0 THEN CAST(x.businessfield_11 as VARCHAR)
    WHEN length(x.businessfield_12) =12 AND try_cast(x.businessfield_12 as BIGINT) > 0 THEN CAST(x.businessfield_12 as VARCHAR)
    ELSE Null END  AS businessfield,
date_parse(Replace(x.call_start_datetime,'.',''), '%d/%m/%Y %h:%i:%s %p') as timestamp_contact
FROM "cwp-collections"."acqueon_campaign_month" AS x
LEFT JOIN "cwp-collections"."collections_reference" AS y ON x.outcome = y.outcome
),
Aqueon_final_list as (
--4.2 Tabla de clientes agrupada con historial de promesas de pago y resultado de ultimo intento
SELECT businessfield, lower(outcome) as outcome, timestamp_contact,
COUNT(businessfield) as attemtps
FROM
(
--4.2.1 Subtabla de ultimo contacto
SELECT
businessfield,
COALESCE(
    first_value(outcome) over(partition by businessfield order by timestamp_contact DESC),first_value(childoutcome) over(partition by businessfield order by timestamp_contact DESC)
    ) as outcome,
first_value(timestamp_contact) over(partition by businessfield order by timestamp_contact DESC) as timestamp_contact
FROM Acqueon_month_contact
)
WHERE DATE_TRUNC('month',timestamp_contact) = DATE_TRUNC('month',CURRENT_DATE)
GROUP BY businessfield, outcome,timestamp_contact
),
-----------------------------------------------------------------
--05. Calcular  contactabilidad para criterio del grupo de control
-----------------------------------------------------------------
control_group as (
SELECT  businessfield, 
ROUND(
(CAST(SUM(CASE WHEN childoutcome ='No contesta1' THEN 0 ELSE 1 END) as double)
/CAST(SUM(CASE WHEN contactid IS NOT null THEN 1 ELSE 0 END) as double)
)
,2) as contact_rate
FROM(
SELECT *,
CASE WHEN CAST(businessfield_1 as VARCHAR) is not null THEN CAST(businessfield_1 as VARCHAR)
    WHEN  CAST(businessfield_1 as VARCHAR) is null THEN CAST(businessfield_2 as VARCHAR)
    ELSE Null END  AS businessfield
FROM "cwp-collections"."feedback_historical_data"
)
GROUP BY businessfield
),
-------------------------------------------------------
--06. Identificar clientes con tickets técnicos abiertos 
-------------------------------------------------------
open_tickets as(
SELECT account_id, COUNT (interaction_id) as open_Tickets_Truckrolls
FROM(
SELECT account_id,interaction_id,interaction_purpose_descrip,
first_value(interaction_status) over(partition by interaction_id order by interaction_start_time DESC) last_status,
first_value(interaction_start_time) over(partition by interaction_id order by interaction_start_time DESC) last_status_date,
first_value(interaction_status) over(partition by interaction_id order by interaction_start_time ASC) first_status,
first_value(interaction_start_time) over(partition by interaction_id order by interaction_start_time ASC) first_status,
array_agg(interaction_status) as status
FROM "db-stage-prod"."interactions_cwp"
WHERE  interaction_purpose_descrip = 'TICKET' OR interaction_purpose_descrip = 'TRUCKROLL'
GROUP BY account_id,interaction_id,interaction_status,interaction_start_time,interaction_purpose_descrip
)
WHERE last_status  = 'OPEN'
AND DATE(last_status_date) >= DATE_add('day',-30,CURRENT_DATE )
GROUP BY account_id   
),
-------------------------------------------------------
--07. Identificar restriciones del cliente 
-------------------------------------------------------
restriction as (
SELECT
account_id, last_order_id,Last_order_dt,last_sales_rep_name,Last_channel_type
    FROM
    (
    SELECT 
    *,
    DATE(order_start_date) as order_dt,
    CONCAT(
    CASE WHEN lob_vo_count = 1 THEN CONCAT(network_type,'/','VO') ELSE '' END,
    CASE WHEN lob_bb_count = 1 THEN CONCAT(network_type,'/','BB') ELSE '' END,
    CASE WHEN lob_tv_count = 1 THEN CONCAT(network_type,'/','TV') ELSE '' END
    ) as pd_tech_mix,
    first_value(sales_rep_name) OVER (PARTITION BY account_id ORDER BY (order_start_date) DESC) as last_sales_rep_name,
    first_value(channel_type) OVER (PARTITION BY account_id ORDER BY (order_start_date) DESC) as Last_channel_type,
    first_value(channel_sub_type) OVER (PARTITION BY account_id ORDER BY (order_start_date) DESC) as last_channel_sub_type,
    first_value(DATE(order_start_date)) OVER (PARTITION BY account_id ORDER BY (order_start_date) DESC) as last_order_dt,
    first_value(order_id) OVER (PARTITION BY account_id ORDER BY (order_start_date) DESC) as last_order_id
    FROM "db-stage-dev"."so_hdr_cwp"
    WHERE order_type in ('TRANSFER')
    AND order_status in ('COMPLETED')
    AND DATE(order_start_date) >= DATE_ADD('month', -6, CURRENT_DATE)
    )
GROUP BY  Last_order_dt,account_id,source_system_name, last_sales_rep_name,last_channel_sub_type,Last_channel_type,last_order_id
ORDER BY Last_order_dt DESC
),
--------------------------------------------------------------
--08. Identificar clientes excluidos (phoneix, VIP, candidatos)
--------------------------------------------------------------
exclusion AS (
SELECT act_acct_cd as act_acct_cd_excl, array_agg(source) as source, lower(array_join(array_agg(category),',')) as category
FROM "cwp-collections"."cwp_collections_exclusion"
GROUP BY act_acct_cd
),
--------------------------------------------------------------
--09. Unir B2C Fijos del día/score PTICH/Pagos/Outcome Acqueon
--------------------------------------------------------------
final_table AS (
SELECT
a.*,
b.*,
c.Pagos,
d.*,
CASE
    WHEN h.category like ('%xcluir%') OR h.category like ('%cesada%') then 'Exclusion'
    WHEN h.category like ('%hoenix%')  then 'Phoenix'
    WHEN  h.category like ('%candidato%') 
    --OR month(DATE_ADD('day',60,vencimiento_factura_antigua)) = month(CURRENT_DATE) 
    THEN 'Candidate'
    WHEN(CAST(substr(a.ID_CLIENTE_UNICO, 7,2) as int) between 3 and 9)  THEN 'Control_group_1'
    ELSE 'Treatment_group'  END as Cust_group,
CASE
    WHEN outcome IS NULL  OR outcome not in  ('invalid','no dial tone') THEN 'valid_phone'
    ELSE 'invalid_phone'END as check_phone,
f.contact_rate,
g.open_Tickets_Truckrolls,
h.act_acct_cd_excl,
h.category,
i.last_order_id,
i.Last_order_dt,
CASE WHEN lower (i.last_sales_rep_name) like ('%r%b%')
        AND lower(i.Last_channel_type) like ('%ac19%')
        AND Last_order_dt >= DATE_ADD('day',-2,CURRENT_DATE )
    THEN 'Restablecido_auto_48hrs'
    WHEN lower (i.last_sales_rep_name) like ('%r%h%q%')
        AND lower(i.Last_channel_type) not like ('%ac19%')
        AND Last_order_dt >= DATE_ADD('day',-2,CURRENT_DATE )
    THEN 'Restablecido_manu_48hrs'
    WHEN lower (i.last_sales_rep_name) like ('%d%g%')
        AND lower(i.Last_channel_type) like ('%ac19%')
    THEN 'Restringido'
ELSE 'Servicio_regular' END as last_plan_change
FROM DNA_adjst AS a
LEFT JOIN score_ptich AS b ON CAST(a.ID_CLIENTE_UNICO AS BIGINT)= CAST(b.act_acct_cd_PTICH AS BIGINT)
LEFT JOIN Pagos AS c ON CAST(a.ID_CLIENTE_UNICO AS BIGINT) = CAST(c.account_id AS BIGINT)
LEFT JOIN Aqueon_final_list AS d ON CAST(a.ID_CLIENTE_UNICO AS VARCHAR) = CAST(d.businessfield AS VARCHAR)
LEFT JOIN control_group AS f ON CAST(a.ID_CLIENTE_UNICO AS VARCHAR) = CAST(f.businessfield AS VARCHAR)
LEFT JOIN open_tickets AS g ON CAST(a.ID_CLIENTE_UNICO AS VARCHAR) = CAST(g.account_id AS VARCHAR)
LEFT JOIN exclusion AS h ON CAST(a.ID_CLIENTE_UNICO AS VARCHAR) = CAST(h.act_acct_cd_excl AS VARCHAR)
LEFT JOIN restriction AS i ON CAST(a.ID_CLIENTE_UNICO AS VARCHAR) = CAST(i.account_id AS VARCHAR)
ORDER BY decile ASC
),
--------------------------------------------------------------
--10. Reglas de negocio, exclusión de clientes con bloqueos
--------------------------------------------------------------
depurated_list as (
SELECT 	
*
-- 10.1 Backlog no contactactado del día anterior
FROM (
SELECT *,
CASE WHEN Outcome IS NULL THEN (Dias_Morosidad-1) ELSE Dias_Morosidad END AS Overdue,
CASE WHEN ciclo like  ('%A%')
    THEN DATE_ADD('MONTH',1,DATE_ADD('DAY',60,vencimiento_factura_antigua))
    ELSE DATE_ADD('DAY',60,vencimiento_factura_antigua) 
    END as Churn_date
FROM final_table
)
 WHERE Pagos IS NULL
 AND Nombre_Cliente IS NOT NULL
 AND Monto_Total >0
 AND open_Tickets_Truckrolls IS NULL
AND (outcome IS NULL 
OR NOT (
outcome LIKE ('%contactado%due%cuenta%')
OR outcome LIKE ('%equivocado%')
OR outcome LIKE ('%cliente%con%da%o%')
OR outcome LIKE ('%cliente%al%d%a%')
OR outcome LIKE ('%cliente%con%reclamo%')
OR outcome LIKE ('%arreglo%de%pago%')
OR outcome LIKE ('%cambio%de%plan%')
OR outcome LIKE ('%success%')
OR outcome LIKE ('%cesada%')
OR outcome LIKE ('%encuesta%')
))
ORDER BY contact_rate ASC,  score_ptich DESC
),
--------------------------------------------------------
--11. definir tramos de la deuda y Tiers para call center
--------------------------------------------------------
business_rules AS (
Select
*,
CASE
    WHEN Dias_Morosidad Between -3 and 5
        THEN '1. Heads up contact'
    WHEN Dias_Morosidad Between 6 and 12 
        THEN '2. After soft dx'
    WHEN Dias_Morosidad Between 13 and 21 
        THEN '3. Chill week'
    WHEN Dias_Morosidad Between 22 and 41 
        THEN '4. Retention attempt 1'
    WHEN Dias_Morosidad Between 42 and 58 
        THEN '5. Chill week'
    WHEN Dias_Morosidad Between 59 and 70 
        THEN '6. Retention attempt 2'
    WHEN Dias_Morosidad Between 71 and 76 
        THEN '7. Close to churn'
    ELSE NULL
END AS TRANCHE
FROM depurated_list
),
--------------------------------------------------------------
--12. Establecer prioridades para el desbordamiento de llamadas
--------------------------------------------------------------
contact_channel as(
SELECT *,
CASE
--------------------------------------------------------------
---------------------CANDIDATOS-------------------------------
--------------------------------------------------------------
--- Prioridad 0: candidatos nunca contactados
    WHEN Cust_group = 'Candidate' 
    AND outcome IS NULL
        THEN 'Tier_00'
--- Prioridad 1: Candidatos desbordamiento
    WHEN Cust_group = 'Phoenix'
        AND outcome IS NULL
        THEN 'Tier_01'
--- Prioridad 2: Candidatos desbordamiento
    WHEN Cust_group = 'Candidate' OR Cust_group = 'Phoenix'
        AND attemtps >=2
        THEN 'Tier_02'   
--------------------------------------------------------------
---------------------TRADICIONAL------------------------------
--------------------------------------------------------------
--- Prioridad 3: Grupo de control
    WHEN Cust_group = 'Control_group_1'
        AND Dias_Morosidad > 0
        THEN 'Tier_03'
--- Prioridad 4: Clientes que nunca han sido contactados y van a hacer churn proximamente
    WHEN TRANCHE = '7. Close to churn'
        THEN 'Tier_04'
--- Prioridad 5: Clientes de alto riesgo de todos los tramos nunca contactados
    WHEN Risk = 'High'
        AND Dias_Morosidad > 0
        AND Overdue In (3,7,9,11,22,28,32,36,40,46,52,57,59,66,71,72,73,74,75)
        AND outcome is null
        THEN 'Tier_05'
--- Prioridad 6: Clientes de alto riesgo de todos los tramos y repetido
    WHEN Risk = 'High'
        AND Dias_Morosidad > 0
        AND Overdue In (3,7,9,11,22,28,32,36,40,46,52,57,59,66,71,72,73,74,75)
        THEN 'Tier_06'
--- Prioridad 7: Clientes de medio riesgo nunca contactados
    WHEN Risk = 'Medium'
        AND Dias_Morosidad > 0
        AND Overdue In (8,10,24,34,62,69,71,72,73,74,75)
        AND outcome is null
        THEN 'Tier_07'
--- Prioridad 8: Clientes de medio riesgo de todos los tramos
    WHEN Risk = 'Medium'
    AND Dias_Morosidad > 0
        AND Overdue In (8,10,24,34,62,69,71,72,73,74,75)
        THEN 'Tier_08'
--- Prioridad 9: desbordamiento nunca llamados ciclos ABC
    WHEN Dias_Morosidad > 0
        AND outcome is null
        AND Overdue not In (1,4,5,27,30)
        AND Ciclo In ('A','B','C')
        THEN 'Tier_09'  
--- Prioridad 10: desbordamiento nunca llamados otros ciclos
    WHEN Dias_Morosidad > 0
        AND outcome is null
        AND Overdue not In (1,4,5,14,17)
        AND Ciclo not In ('A','B','C')
        THEN 'Tier_10' 
--- Prioridad 11: desbordamiento llamados anteriormente
    ELSE 'Tier_11'

END AS TIER 
FROM business_rules
),
-------------------------------------------------------
--13. Definir canales de contacto de acuerdo al journey
-------------------------------------------------------
channels as (
SELECT *, 
CASE

-------------#### MASSIVE EMAIL-SMS: Preoverdue reminder ##### ----------------------
    WHEN  Dias_Morosidad In (-3)
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Prevoverdue'

-------------#### MASSIVE EMAIL-SMS: Restriction warning -4 days ##### ----------------------
    WHEN  Dias_Morosidad In (1)
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction_warning_-4_days'
-------------#### MASSIVE EMAIL-SMS: Restriction warning -24 hrs ##### ----------------------
    WHEN  Dias_Morosidad In (4)
        AND act_acct_cd_excl IS  NULL
       
        ----!!!TEST!!!------
        OR ( 
        Dias_Morosidad between 6 AND 22
        AND plan not like ('%postmora%')
        AND status_cuenta in ('W','D')
        )
        --------------
       
        THEN 'SMS-EMAIL:Restriction_warning_-24_hrs'        
-------------#### MASSIVE EMAIL-SMS: Restriction notification ##### ----------------------
    WHEN  Dias_Morosidad In (5)
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction'
-------------#### MASSIVE EMAIL-SMS: suspension warning cycle ABC ##### ----------------------
    WHEN  Dias_Morosidad In (27)
        AND Ciclo In ('A','B','C')
        --AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_warning_-3_days'
-------------#### MASSIVE EMAIL-SMS: suspension warning cycle OTHER ##### ----------------------
    WHEN  Dias_Morosidad In (14)
    AND Ciclo NOT In ('A','B','C')
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_warning_-3_days'
-------------#### MASSIVE EMAIL-SMS: suspension notificacion cycle ABC ##### ----------------------
    WHEN  Dias_Morosidad In (30)
        AND Ciclo In ('A','B','C')
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension'
-------------#### MASSIVE EMAIL-SMS: suspension notificacion cycle OTHER ##### ----------------------
    WHEN  Dias_Morosidad In (17)
    AND Ciclo NOT In ('A','B','C')
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension'   

-------------#### MASSIVE EMAIL-SMS: Restriction reminder ##### ----------------------
    WHEN  Risk = 'High'
        AND Dias_Morosidad In (8,10,14,17,21)
        AND Ciclo In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction_reminder'
    WHEN  Risk = 'Medium'
        AND Dias_Morosidad In (7,9,11,14,17,21)
        AND Ciclo In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction_reminder'
    WHEN  Risk = 'Low'
        AND Dias_Morosidad In (7,9,11,14,17,21,22)
        AND Ciclo In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction_reminder'
    WHEN  Risk = 'High'
        AND Dias_Morosidad In (8,10)
        AND Ciclo NOT  In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction_reminder'
    WHEN  Risk = 'Medium'
        AND Dias_Morosidad In (7,9,11)
        AND Ciclo NOT In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction_reminder'
    WHEN  Risk = 'Low'
        AND Dias_Morosidad In (7,9,11)
        AND Ciclo NOT In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Restriction_reminder'        

-------------#### MASSIVE EMAIL-SMS: suspension notificacion cycle ABC reminder ##### ----------------------
    WHEN  Risk = 'High'
        AND Dias_Morosidad In (39,47,50,54,57)
        AND Ciclo In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_reminder'
    WHEN  Risk = 'Medium'
        AND Dias_Morosidad In (39,47,50,54,57)
        AND Ciclo In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_reminder'
    WHEN  Risk = 'Low'
        AND Dias_Morosidad In (32,36,39,42,44,47,54,59,66)
        AND Ciclo In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_reminder'

-------------#### MASSIVE EMAIL-SMS: suspension notificacion cycle OTHER reminder ##### ----------------------   
    WHEN  Risk = 'High'
        AND Dias_Morosidad In (21,27,30,39,47,50,54,57)
        AND Ciclo NOT In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_reminder'
    WHEN  Risk = 'Medium'
        AND Dias_Morosidad In (21,27,30,39,47,50,54,57)
        AND Ciclo NOT In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_reminder'
    WHEN  Risk = 'Low'
        AND Dias_Morosidad In (21,22,27,30,32,36,39,42,44,47,54,59,66)
        AND Ciclo NOT In ('A','B','C')
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        THEN 'SMS-EMAIL:Suspension_reminder'
        
-------------#### MASSIVE EMAIL-SMS: Control Group ##### ----------------------        
    WHEN Cust_group = 'Control_group_1'
        AND act_acct_cd_excl IS  NULL
        AND Dias_Morosidad > 0
        THEN 'SMS-EMAIL:Generic'        
ELSE 'NA'
END Massive_notification,
        
CASE

-------------#### Agent_Calls current month candidate ##### ----------------------
    WHEN Cust_group = 'Candidate'
        AND ranking_tier <= (select (Daily_candidates_cap) from parameters limit 1)
        AND (outcome IS NULL  OR outcome not in  ('invalid','no dial tone'))
    THEN 'AGENT CALL Candidate'    
-------------#### Agent_Calls by tiers ##### ----------------------          
    WHEN TIER = 'Tier_03'
        AND ranking_tier <= (select (Daily_agent_cap*0.1) from parameters limit 1)
        AND (outcome IS NULL  OR outcome not in  ('invalid','no dial tone'))
    THEN 'AGENT CALL'
    
    WHEN ranking_tier <= (select (Daily_agent_cap*0.90) from parameters limit 1)
        AND Cust_group = 'Treatment_group'
        AND (outcome IS NULL  OR outcome not in  ('invalid','no dial tone'))
    THEN 'AGENT CALL'

-------------#### whatsapp spill over in payroll week ##### ----------------------          
     WHEN ((CAST(day(CURRENT_DATE) as BIGINT) between 10 AND 17) OR (CAST(day(CURRENT_DATE) as BIGINT)  between  26 AND 31))
        AND ranking_tier <= (select (Daily_agent_cap) from parameters limit 1)
        AND Cust_group = 'Treatment_group'
        AND (outcome IS NULL  OR outcome not in  ('invalid','no dial tone') OR outcome not like 'n%mero%errado%')
        --AND Dias_Morosidad not in (1,4,5,27,30,14,17)
    THEN 'Whatsapp'   

-------------#### MASSIVE IVR-ROBOCALL ##### ----------------------
     WHEN Risk = 'High'
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        AND Dias_Morosidad In (19,24,34,42,44,62,69)
    THEN 'IVR-Robocall'
    WHEN Risk = 'Medium'
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        AND Dias_Morosidad In (3,19,22,28,32,36,42,44,59,66)  
    THEN 'IVR-Robocall'   
    WHEN Risk = 'Low'
        AND Cust_group = 'Treatment_group'
        AND act_acct_cd_excl IS  NULL
        AND Dias_Morosidad In (8,10,19,24,34,62,69)  
    THEN 'IVR-Robocall'      

ELSE 'Silence'
END as CONTACT_CHANNEL
FROM(
SELECT *,
CASE 

------
WHEN Cust_group = 'Candidate' OR Cust_group = 'Phoenix'
    THEN ROW_NUMBER () OVER (Partition by Cust_group,check_phone ORDER by TIER ASC, decile ASC)
-------
WHEN Cust_group = 'Control_group_1'
    THEN ROW_NUMBER () OVER (Partition by Cust_group,check_phone ORDER by contact_rate DESC) 
-------   
WHEN Cust_group = 'Treatment_group'
    THEN ROW_NUMBER () OVER (Partition by Cust_group,check_phone ORDER by TIER ASC, decile ASC,contact_rate DESC)
-------

ELSE null
END as ranking_tier
FROM contact_channel
)
)
SELECT *, DATE(CURRENT_DATE) as campaign_run_date
FROM channels
--WHERE id_cliente_unico = '312029450000'