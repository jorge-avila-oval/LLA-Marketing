UNLOAD (
'select 
*   
from "prod"."public"."lcpr_offers"
where link_dt = 1694044800000 and link is not null ' )
TO 's3://aiq-exchange-lcpr/exploration/Redshift Exports/20230907_offers_links'
IAM_ROLE   'arn:aws:iam::283229902738:role/redshift_s3_access'
PARALLEL OFF
HEADER
ALLOWOVERWRITE
CSV

