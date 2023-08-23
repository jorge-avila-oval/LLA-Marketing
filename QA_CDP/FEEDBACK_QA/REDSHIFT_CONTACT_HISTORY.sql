SELECT 
    *
FROM "prod"."public"."lcpr_communications_hst"
WHERE channel = 'email' and sent_dt_ms is not null and 
    campaign  in ('TEST_20230823_CBM_CDP_OFFERFIT_CONTROL')
