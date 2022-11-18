select 
  
    extract(month from order_start_date) as mes,
    count(distinct order_id) as num_usuarios
from "db-stage-dev"."so_hdr_cwc"
where 
    org_cntry = 'Jamaica'
    and account_type = 'Residential'
    and extract(year from order_start_date) = 2022
group by 1
order by mes
