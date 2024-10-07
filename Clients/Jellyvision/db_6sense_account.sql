-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

---- 6Sense Account ----
TRUNCATE TABLE `jellyvision.db_6sense_account`;

INSERT INTO `jellyvision.db_6sense_account` (
  _is_6qa,
  _account_profile_score_6sense,
  _account_6qa_age_in_days_6sense,
  _account_reach_score_6sense,
  _account_intent_score_6sense,
  _account_buying_stage_6sense, 
  _account_profile_fit_6sense, 
  _account_6qa_end_date_6sense,
  _account_update_date_6sense,
  _account_numerical_reach_score_6sense, 
  _account_6qa_start_date_6sense,
  _account_name,
  _domain,
  _revenue,
  _industry
)

SELECT 
    account6qa6sense__c AS  _is_6qa,
    accountprofilescore6sense__c AS _account_profile_score_6sense, 
    account6qaageindays6sense__c AS _account_6qa_age_in_days_6sense, 
    accountreachscore6sense__c AS _account_reach_score_6sense, 
    accountintentscore6sense__c AS _account_intent_score_6sense, 
    accountbuyingstage6sense__c AS _account_buying_stage_6sense, 
    accountprofilefit6sense__c AS _account_profile_fit_6sense, 
    account6qaenddate6sense__c AS _account_6qa_end_date_6sense,
    accountupdatedate6sense__c AS account_update_date_6sense,
    accountnumericalreachscore6sense__c AS _account_numerical_reach_score_6sense, 
    account6qastartdate6sense__c AS _account_6qa_start_date_6sense,
    name AS _account_name,
    web_domain_name__c  AS _domain,
    jv_revenue_estimate__c AS _revenue,
    industry_variable__c AS _industry
FROM `x-marketing.jellyvision_salesforce.Account`
WHERE isdeleted IS FALSE;