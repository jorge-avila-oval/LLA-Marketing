with csr_id as (
    SELECT 
        account_id as numero_cuenta
    FROM  "prod"."public"."insights_customer_services_rates_lcpr"
    WHERE AS_OF = (select max(as_of) from "prod"."public"."insights_customer_services_rates_lcpr" )

),

offers as (
    SELECT
        account_id,
        MAX ( CASE WHEN lower(channel ) = 'email' then hsd_service  ELSE NULL END ) as FROM_HSD_SPEED
    from "prod"."public"."lcpr_offers"
    group by 1
)
SELECT 
    count(distinct account_id)
FROM csr_id LEFT JOIN offers offers ON csr_id.numero_cuenta = offers.account_id
WHERE FROM_HSD_SPEED is not null  -- poner la condición que se quiere probar en cada uno de los atributos. 
;
