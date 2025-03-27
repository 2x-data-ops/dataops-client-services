-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

----6Sense account

CREATE OR REPLACE TABLE`jellyvision.6qa_account` AS
WITH contact AS (
  SELECT
    name AS _name,
    firstname AS _firstname,
    lastname AS _lastname,
    id AS _contactid,
    masterrecordid AS _masterrecordid,
    recordtypeid AS _recprdtypeid,
    ownerid AS _ownerid,
    days_since_last_touch__c AS _day_since_last_touch,
    last_touch__c AS _last_touch,
    days_since_last_call__c AS _dayt_since_last_call,
    of_outreach_email_attempts_last_90_day__c AS _of_outreach_email_attempts_last_90_day,
    of_marketo_emails_sent_last_90_days__c AS _of_marketo_emails_sent_last_90_days,
    case_sensitive_contact_id__c AS _case_sensitive_contact_id,
    accountid AS _accountid,
    DATE_DIFF(TIMESTAMP(CURRENT_DATETIME('America/New_York')), lastactivitydate,DAY) AS _lasttouch,
    last_activity__c AS _last_activity,
    lastactivitydate AS _contact_lastactivitydate
  FROM `x-marketing.jellyvision_salesforce.Contact`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY accountid ORDER BY lastactivitydate DESC) = 1 
), event AS (
  SELECT
    accountid,
    MAX(activitydate) AS _last_event_date
  FROM `x-marketing.jellyvision_salesforce.Event`
  GROUP BY 1 
), account AS (
  SELECT
    id AS _id,
    name AS _account_name,
    web_domain_name__c AS _domain,
    ownerid,
    account_owner_name__c,
    type,
    account6qa6sense__c AS _is_6qa,
    accountbuyingstage6sense__c AS _accountbuyingstage6sense,
    account6qastartdate6sense__c AS _account6qastartdate6sense,
    account6qaenddate6sense__c AS _account6qaenddate6sense,
    account6qaageindays6sense__c AS _account6qaageindays6sense,
    contacts_active_sequence__c,
    _last_event_date,
    lastactivitydate,
    accountprofilescore6sense__c AS _accountprofilescore6sense,
    accountreachscore6sense__c AS _accountreachscore6sense,
    accountintentscore6sense__c AS _accountintentscore6sense,
    accountprofilefit6sense__c AS _accountprofilefit6sense,
    accountupdatedate6sense__c AS accountupdatedate6sense__c,
    accountnumericalreachscore6sense__c AS _accountnumericalreachscore6sense,
    jv_revenue_estimate__c AS _revenue,
    industry_variable__c AS _industry,
    isdeleted AS _isdeleted,
    case_sensitive_id__c
  FROM `x-marketing.jellyvision_salesforce.Account` acc
  LEFT JOIN event
    ON acc.id = event.accountid
  WHERE DATE(account6qastartdate6sense__c) > '2024-12-31'
    AND isdeleted IS FALSE
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY lastactivitydate DESC) = 1
  ORDER BY
    account6qastartdate6sense__c ASC 
)
SELECT
  account.*,
  contact.* EXCEPT(_accountid)
FROM
  account
LEFT JOIN contact
  ON account._id = contact._accountid