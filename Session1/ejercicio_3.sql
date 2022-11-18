select 
    interaction_purpose_descrip,
    count(distinct interaction_id) as num_interacciones
    
from "db-stage-prod"."interactions_cwp" 
where 
    extract(month from interaction_start_time) = 8
    and extract(year from interaction_start_time) = 2022
group by interaction_purpose_descrip
