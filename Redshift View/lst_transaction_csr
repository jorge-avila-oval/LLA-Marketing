with lst_order as
(
select 
    sub_acct_no_ooi,
    max(date(ls_chg_dte_ocr)) as lst_chg_dte_ocr
from "db-stage-dev"."transactions_orderactivity"
where ord_typ in ('SIDEGRADE','UPGRADE','DOWNGRADE')
group by sub_acct_no_ooi
order by sub_acct_no_ooi
),

lst_order_bundle as
(
select 
    trans_ord.sub_acct_no_ooi,
    lst_chg_dte_ocr as max_dt,
    date(ls_chg_dte_ocr) as dt,
    order_no_ooi,
    ord_typ,
    phone_up,
    SUBSTRING(phone_up,1,5) as bill_code_phone,
    cable_up,
    SUBSTRING(cable_up,1,5) as bill_code_cable,
    internet_up,
    SUBSTRING(internet_up,1,5) as bill_code_internet
from lst_order left join "db-stage-dev"."transactions_orderactivity" trans_ord
    on trans_ord.sub_acct_no_ooi = lst_order.sub_acct_no_ooi and date(trans_ord.ls_chg_dte_ocr) = lst_order.lst_chg_dte_ocr
),

order_csr as (
select 
    sub_acct_no_sbb,
    max_dt as order_dt,
    date(csr.as_of) as csr_dt,
    ord_typ,
    order_no_ooi
from lst_order_bundle left join "db-stage-prod"."insights_customer_services_rates_lcpr" csr on 
    lst_order_bundle.sub_acct_no_ooi = csr.sub_acct_no_sbb and (bill_code_internet = csr.bill_code or bill_code_cable = csr.bill_code or bill_code_cable = csr.bill_code)
where max_dt <= date(csr.as_of)
order by sub_acct_no_sbb )

select 
    sub_acct_no_sbb,
    order_dt,
    ord_typ,
    order_no_ooi,
    min(csr_dt)
from order_csr
group by 
sub_acct_no_sbb,
    order_dt,
    ord_typ,
    order_no_ooi
order by sub_acct_no_sbb, order_dt
