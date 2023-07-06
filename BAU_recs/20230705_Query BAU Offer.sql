SELECT *
FROM (
        SELECT 
          C.sub_acct_no_sbb,
          C.FLAG_DROP_TYPE,
          C.package_cod_from
          C.package_price_from,
          C.bb_speed_from,
          C.package_code_to,
          C.package_price_to,
          C.bb_speed_to,
          C.package_price_to - c.package_price_from as DELTA_PRICE,
            ROW_NUMBER() OVER (
                PARTITION by C.sub_acct_no_sbb
                ORDER BY C.package_price_to
            ) as ranking
        FROM (
                (
                    SELECT sub_acct_no_sbb,
                        drop_type,
                        SUBSTRING(cast (PLAY_TYPE as VARCHAR), 1, 2) as play_type_2,
                        stb_sc,
                        primary_video_services,
                        total_charge as package_price_from,
                        package_code as package_code_from,
                        down_speed as bb_speed_from,
                        CASE
                            WHEN lower(stb_sc) like '%dvr%' THEN 1 ELSE 0
                        end as new_dvr_flag,
                        CASE
                            WHEN primary_video_services = 'BASIC' THEN 'Basic'
                            WHEN primary_video_services = 'BROADCAST' THEN 'Broadcast'
                            WHEN primary_video_services = 'CHOICE PACK' THEN 'Choice Pack'
                            WHEN primary_video_services = 'ESP PRIMERA' THEN 'EDP'
                            WHEN primary_video_services = 'EXP BASIC' THEN 'EXP Basic'
                            WHEN primary_video_services = 'LOCAL CHOICE' THEN 'Local Choice'
                            WHEN primary_video_services = 'TU MUNDO' THEN 'Tu Mundo'
                            WHEN primary_video_services = 'ULTIMATE' THEN 'Ultimate'
                            WHEN primary_video_services = 'UPICK' THEN 'UPICK' ELSE NULL
                        END AS FLAG_PRIMARY_VIDEO_SERVICES,
                        CASE
                            WHEN drop_type = 'COAX' THEN 'Coax Trad' ELSE 'FTTH'
                        END AS FLAG_DROP_TYPE,
            CASE
                WHEN (play_type = '3P Video HSD Voice' OR
                  play_type = '2P HSD Voice' OR
                  play_type = '2P Video HSD ' OR
                  play_type = '1P HSD ') THEN 1 ELSE 0
               END AS FLAG_HSD_DNA,
            CASE
                WHEN (play_type = '3P Video HSD Voice' OR
                  play_type = '2P Video Voice' OR
                  play_type = '2P Video HSD ' OR
                  play_type = '1P Video ') THEN 1 ELSE 0
               END AS FLAG_VIDEO_DNA,
            CASE
                WHEN (play_type = '3P Video HSD Voice' OR
                  play_type = '2P HSD Voice' OR
                  play_type = '2P Video Voice' OR
                  play_type = '1P Voice') THEN 1 ELSE 0
               END AS FLAG_VOICE_DNA

                    from "lcpr.sandbox.dev"."customer_profile_dna"
                    where as_of = '2023-06-30'
                ) a
                left join (
                    select connection_type,
                        play,
                        tv,
                        regular as package_price_to,
                        csg_codes as package_code_to,
                        download_speed as bb_speed_to,
            CASE
                WHEN phone IS NOT NULL THEN 1 ELSE 0
                  END AS FLAG_VOICE_FRONTBOOK,
            CASE
                WHEN tv IS NOT NULL THEN 1 ELSE 0
                  END AS FLAG_VIDEO_FRONTBOOK,
            CASE
                WHEN modem IS NOT NULL THEN 1 ELSE 0
                  END AS FLAG_HSD_FRONTBOOK
                    from "lcpr.sandbox.dev"."2023_lcpr_frontbook"
                ) b ON
        a.FLAG_DROP_TYPE = b.connection_type
                AND a.play_type_2 = b.play
        AND a.FLAG_PRIMARY_VIDEO_SERVICES = b.tv
            AND b.bb_speed_to >= a.bb_speed_from
        AND a.FLAG_HSD_DNA = b.FLAG_HSD_FRONTBOOK
        AND a.FLAG_VOICE_DNA = b.FLAG_VOICE_FRONTBOOK
        AND a.FLAG_VOICE_DNA = b.FLAG_VOICE_FRONTBOOK
            AND CASE
                    WHEN lower(stb_sc) like '%dvr%' then b.package_price_to + 5 > a.package_price_from else b.package_price_to > a.package_price_from
            end
            ) C
    )
WHERE ranking = 1
    AND package_price_to IS NOT NULL
