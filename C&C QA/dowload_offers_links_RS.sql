UNLOAD (
'
SELECT 
    *
FROM "prod"."public"."lcpr_offers" 
WHERE 
    CAST(DATEADD(SECOND, link_dt/1000,''1970/1/1'') AS DATE) = date(''2023-09-15'')
    ' )
TO 's3://aiq-exchange-lcpr/exploration/Redshift Exports/20230915_offer_table_link_2023-09-15'
IAM_ROLE   'arn:aws:iam::283229902738:role/redshift_s3_access'
PARALLEL OFF
HEADER
ALLOWOVERWRITE
CSV
