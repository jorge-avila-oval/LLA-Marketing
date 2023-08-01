CREATE
OR REPLACE VIEW "public"."lcpr_customer_service_features" AS
SELECT
    'LCPR_FX_':: text || a.sub_acct_no_sbb:: text AS account_id,
    a.home_phone_sbb,
    b.num_accounts,
    "date_part"('epoch':: text, a.lst_bill_dt) AS lst_bill_dt,
    d.change_hsd_speed,
    "date_part"('epoch':: text, a.dt:: date) AS dt
FROM
    (
        SELECT
            data_service_rates.sub_acct_no_sbb,
            data_service_rates.home_phone_sbb,
            CASE
            WHEN data_service_rates.cyc_cde_sbb > data_service_rates.current_day THEN to_date(
                (
                    data_service_rates."year":: text || to_char(data_service_rates."month" - 1, '00':: text)
                ) || to_char(data_service_rates.cyc_cde_sbb, '00':: text),
                'YYYYMMDD':: text
            )
            ELSE to_date(
                (
                    data_service_rates."year":: text || to_char(data_service_rates."month", '00':: text)
                ) || to_char(data_service_rates.cyc_cde_sbb, '00':: text),
                'YYYYMMDD':: text
            ) END AS lst_bill_dt,
            data_service_rates.dt
        FROM
            (
                SELECT
                    insights_customer_services_rates_lcpr.sub_acct_no_sbb,
                    insights_customer_services_rates_lcpr.home_phone_sbb,
                    insights_customer_services_rates_lcpr.cyc_cde_sbb,
                    pgdate_part(
                        'day':: text,
                        'now':: text:: date:: timestamp without time zone
                    ):: integer AS current_day,
                    pgdate_part(
                        'month':: text,
                        'now':: text:: date:: timestamp without time zone
                    ):: integer AS "month",
                    pgdate_part(
                        'year':: text,
                        'now':: text:: date:: timestamp without time zone
                    ) AS "year",
                    insights_customer_services_rates_lcpr.dt
                FROM
                    insights_customer_services_rates_lcpr
                WHERE
                    insights_customer_services_rates_lcpr.dt:: date:: text = (
                        (
                            SELECT
                                "max"(insights_customer_services_rates_lcpr.dt:: text) AS max_dt
                            FROM
                                insights_customer_services_rates_lcpr
                        )
                    )
            ) data_service_rates
    ) a
    LEFT JOIN (
        SELECT
            data_service_rates.sub_acct_no_sbb,
            regexp_count(
                (
                    pg_catalog.listagg(
                        DISTINCT data_service_rates.sub_acct_no_sbb:: text,
                        ',':: text
                    ) OVER(PARTITION BY data_service_rates.home_phone_sbb)
                ):: text,
                ',':: text
            ) + 1 AS num_accounts
        FROM
            (
                SELECT
                    insights_customer_services_rates_lcpr.sub_acct_no_sbb,
                    insights_customer_services_rates_lcpr.home_phone_sbb,
                    insights_customer_services_rates_lcpr.cyc_cde_sbb,
                    pgdate_part(
                        'day':: text,
                        'now':: text:: date:: timestamp without time zone
                    ):: integer AS current_day,
                    pgdate_part(
                        'month':: text,
                        'now':: text:: date:: timestamp without time zone
                    ):: integer AS "month",
                    pgdate_part(
                        'year':: text,
                        'now':: text:: date:: timestamp without time zone
                    ) AS "year",
                    insights_customer_services_rates_lcpr.dt
                FROM
                    insights_customer_services_rates_lcpr
                WHERE
                    insights_customer_services_rates_lcpr.dt:: date:: text = (
                        (
                            SELECT
                                "max"(insights_customer_services_rates_lcpr.dt:: text) AS max_dt
                            FROM
                                insights_customer_services_rates_lcpr
                        )
                    )
            ) data_service_rates
    ) b ON a.sub_acct_no_sbb = b.sub_acct_no_sbb
    LEFT JOIN (
        SELECT
            insights_customer_services_rates_lcpr.sub_acct_no_sbb,
            CASE
            WHEN count(
                DISTINCT insights_customer_services_rates_lcpr.hsd_speed
            ) > 1 THEN true
            ELSE false END AS change_hsd_speed
        FROM
            insights_customer_services_rates_lcpr
        GROUP BY
            insights_customer_services_rates_lcpr.sub_acct_no_sbb
    ) d ON a.sub_acct_no_sbb = d.sub_acct_no_sbb;
