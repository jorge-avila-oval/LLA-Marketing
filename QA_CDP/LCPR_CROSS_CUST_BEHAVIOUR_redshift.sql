-- LCPR_CROSS_CUST_BEHAVIOUR Redshift
with
csr_attributes as (
    SELECT 
    *
            from "prod"."public"."insights_customer_services_rates_lcpr"
   WHERE AS_OF = (select max(as_of) from "prod"."public"."insights_customer_services_rates_lcpr" )
    -- limit 100
),

flagging_attributes as (
select 
    *,
    -- sub_acct_no_sbb as cust_acct,
    case when has_privacy_flag = 'X' then true else false end as privacy_flag ,
    case when has_open_order  = 'X' then true else false end as open_order ,
    case when pending_tc  = 'X' then true else false end as trouble_call ,
    case when is_in_ndnc  = 'X' then true else false end as dnt_call_flag
from "prod"."public"."flagging"
-- limit 100
)
select 
    count(distinct csr.account_id)
from csr_attributes csr left join flagging_attributes flag on csr.account_id = flag.account_id
where 
    csr.delinquency_days <= 50   and
    open_order = false   and
    trouble_call = false
