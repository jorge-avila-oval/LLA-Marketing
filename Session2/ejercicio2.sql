with
tabla_dias_mora as(
select
    account_id, 
    date_diff('DAY',  cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date), cast(dt as date)) as dias_mora
    
from "db-analytics-prod"."tbl_postpaid_cwc" 

where account_type = 'Residential'
    and org_id = '338'
AND date_trunc('day', date(dt)) = date('2022-10-01')
),
tabla_categorizados as (
select
    account_id,
    dias_mora,
    case when dias_mora >= 90 then 'Inactivos' when dias_mora <90 then 'Activos' else 'Activos' end as clasificacion
    
from tabla_dias_mora   
)

select 
    count(distinct tabla_categorizados.account_id) as num_userss_desactivation
from tabla_categorizados     
left join "db-stage-dev"."so_hdr_cwc" as service on cast(tabla_categorizados.account_id as bigint) = service.account_id
where clasificacion = 'Activos'
    and service.order_type = 'DEACTIVATION'
    and date_trunc('month', date(service.order_start_date)) = date('2022-10-01') 
group by 1,2 order by 2
