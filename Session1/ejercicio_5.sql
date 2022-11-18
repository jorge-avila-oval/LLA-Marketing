with
tabla_dias_mora as(
select
    oldest_unpaid_bill_dt,
    dt,
    date_diff('DAY',  cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date), cast(dt as date)) as dias_mora
    
from "db-analytics-prod"."tbl_postpaid_cwc" 

where account_type = 'Residential'
    and org_id = '338'
    and extract(day from cast(dt as date)) = 1
    and extract(month from cast(dt as date)) = 10 
    and extract(year from cast(dt as date)) = 2022
)

select 
    dias_mora,
    case when dias_mora >= 90 then 'Inactivos' 
    when dias_mora <90 then 'Activos' else 'Activo' end as clasificacion
from tabla_dias_mora    
