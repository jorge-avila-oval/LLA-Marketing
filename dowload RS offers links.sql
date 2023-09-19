UNLOAD (
'
select 
*   
from "prod"."public"."lcpr_offers"
where CAST(DATEADD(SECOND, link_dt/1000,''1970/1/1'') AS DATE)  = date(''2023-09-19'') and link is not null 
' )
TO 's3://aiq-exchange-lcpr/exploration/Redshift Exports/20230919_offers_links'
IAM_ROLE   'arn:aws:iam::283229902738:role/redshift_s3_access'
PARALLEL OFF
HEADER
ALLOWOVERWRITE
CSV
