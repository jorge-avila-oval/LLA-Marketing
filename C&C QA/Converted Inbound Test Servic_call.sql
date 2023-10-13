SELECT 
    serv_call.account_id
FROM db_dev_cdp_project.feedback_serv_calls_in serv_call
    INNER JOIN db_dev_cdp_project.lcpr_offers_in offers
    ON serv_call.account_id = offers.sub_acct_no

WHERE service_c IN ('Tier2','PR_TechnicalAssistance_Spa','CSR_SPA','Business_Sales','Business_SPA','Sales_SPA','Business_Retention','Business_ENG','Sales_ENG','FMC')
    AND callactiondesc IN (Answered','Warm transfer to service by agent','Transferred To External','Transferred To Service','Call Complete')
    AND agent_area IN (
    'C2MAX TECH SUPPORT','C2MAX TECH SUPPORT FIBER','TELEPERFORMANCE TECH SUPPORT','TELEPERFORMANCE CSR','CALLCENTER AGUADILLA','C2MAX CSR','NPS CLOSED LOOP','TELEPERFORMANCE LIVEPERSON','BUSINESS CSSR','SALES CSSR','C2MAX SALES','BUSINESS INSIDE SALES','C2MAX OA','ORDER ASSURANCE','LivePerson','BUSINESS BAE')
    and date_diff('day',date(date),current_date) <= 30
