UNLOAD (
'SELECT
    *
FROM "prod"."public"."lcpr_offers"
where
    LINK IS NOT NULL' )
TO 's3://aiq-exchange-lcpr/exploration/Redshift Exports/20230905_offers'
IAM_ROLE   'arn:aws:iam::283229902738:role/redshift_s3_access'
PARALLEL OFF
HEADER
ALLOWOVERWRITE
CSV
