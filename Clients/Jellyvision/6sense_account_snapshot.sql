INSERT INTO `jellyvision.db_6sense_6qa_account_snapshot`
WITH _6qa_account AS (
    SELECT 
        account6qa6sense__c AS  _is_6qa,
        accountprofilescore6sense__c AS _accountprofilescore6sense, 
        account6qaageindays6sense__c AS _account6qaageindays6sense, 
        accountreachscore6sense__c AS _accountreachscore6sense, 
        accountintentscore6sense__c AS _accountintentscore6sense, 
        accountbuyingstage6sense__c AS _accountbuyingstage6sense, 
        accountprofilefit6sense__c AS _accountprofilefit6sense, 
        account6qaenddate6sense__c AS _account6qaenddate6sense,
        accountupdatedate6sense__c AS accountupdatedate6sense__c,
        accountnumericalreachscore6sense__c AS _accountnumericalreachscore6sense, 
        account6qastartdate6sense__c AS _account6qastartdate6sense,
        name AS _account_name,
        web_domain_name__c  AS _domain,
        jv_revenue_estimate__c AS _revenue,
        industry_variable__c AS _industry,
        isdeleted AS _isdeleted,
        id AS _id,
        CURRENT_DATE('America/New_York') AS _extract_date,
        TIMESTAMP(CURRENT_DATETIME('Asia/Kuala_Lumpur')) AS _run_date,
        CURRENT_TIMESTAMP() AS _extract_timestampt
    FROM `x-marketing.jellyvision_salesforce.Account`

    WHERE DATE(TIMESTAMP(accountupdatedate6sense__c), 'Asia/Kuala_Lumpur') = CURRENT_DATE('America/New_York')
), 
_all_date AS (
    SELECT DISTINCT
        _run_date AS _run_date
    FROM `jellyvision.db_6sense_6qa_account_snapshot`
)
SELECT * 
FROM _6qa_account
WHERE _run_date NOT IN (SELECT _run_date FROM _all_date);