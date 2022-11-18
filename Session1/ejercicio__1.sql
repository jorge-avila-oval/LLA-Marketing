select 
     extract (month from cast(dt as timestamp)) as mes,
    min(extract (day from cast(dt as timestamp))) as dia,
    count(distinct act_acct_cd) as num_usuarios
from "db-analytics-prod"."fixed_cwp"
where 
    extract (year from cast(dt as timestamp)) = 2022
    and act_cust_typ_nm = 'Residencial'
group by 1
order by mes 
