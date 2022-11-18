with resultado as (
select 
    extract (month from cast(dt as timestamp)) as mes,
    extract (day from cast(dt as timestamp)) as dia,
    case when date_diff('month', cast(act_cust_strt_dt as date) , cast(dt as date)) < 6 then 'early tenure' 
        when date_diff('month', cast(act_cust_strt_dt as date) , cast(dt as date)) <= 12 then 'mid tenure' else 'late tenure' end as clasificacion,
    count(distinct act_acct_cd) as num_usuarios
from "db-analytics-prod"."fixed_cwp"
where 
    extract (year from cast(dt as timestamp)) = 2022
    and extract (day from cast(dt as timestamp)) = 1
    and act_cust_typ_nm = 'Residencial'
group by 1,2,3
order by mes)

-- Validación con los resultados del ejercicio 1
select 
    mes,
    dia,
    sum(num_usuarios)
from resultado
group by 1,2
order by mes
