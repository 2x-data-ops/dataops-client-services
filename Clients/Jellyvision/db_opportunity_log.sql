CREATE OR REPLACE TABLE `x-marketing.jellyvision.db_contacts_accounts_log` AS
WITH
  account AS (
  SELECT
    id AS accountid,
    name
  FROM
    `x-marketing.jellyvision_salesforce.Account` 
),account_leads AS (
  SELECT
    id AS accountid,
    name
  FROM
    `x-marketing.jellyvision_salesforce.Account` 
  QUALIFY ROW_NUMBER() OVER (PARTITION by name ORDER BY createddate DESC) = 1
),
  contact AS (
 SELECT
    case_sensitive_contact_id__c,
    id,
    firstname,
    name,
    lastname,
    title,
    accountid,
    email,
    mkto71_Lead_Score__c,
    lifecycle_date_collected__c,
    lifecycle_Date_MQL__c,
    MQL_Reason__c,
    lifecycle_Date_SAL__c,
    lifecycle_Date_SQL__c,
    Contact_Status__c,
    createddate,
    Segment_Tag__c,
    Lifecycle_Stage__c,
    lifecycle_Date_Recycled__c,
    lifecycle_Last_MQL_Date__c,
    Contact_Status_Update__c,
    lastmodifieddate,
    account6qa6sense__c,
    CAST(NULL AS STRING) AS convertedcontactid,
    ownerid,
    createdbyid,
    masterrecordid,
    'Contacts' AS _contact_leads_segments
  FROM
    `x-marketing.jellyvision_salesforce.Contact`
    WHERE isdeleted IS FALSE
  -- WHERE
  --   case_sensitive_contact_id__c = '0030z00002aE0pKAAS' 
), leads AS (
  SELECT
    case_sensitive_lead_id__c,
    id,
    firstname,
    leads.name,
    lastname,
    title,
    CASE WHEN convertedaccountid IS NOT NULL THEN convertedaccountid ELSE accountid END,
    CASE WHEN  account_leads.name IS NOT NULL THEN account_leads.name 
    WHEN convertedaccountid IS NOT NULL THEN account_leads.name ELSE company END AS company,
    email,
    CAST(NULL AS FLOAT64) AS mkto71_Lead_Score__c,
    lifecycle_date_collected__c,
    lifecycle_Date_MQL__c,
    MQL_Reason__c,
    lifecycle_Date_SAL__c,
    lifecycle_Date_SQL__c,
    Status,
    createddate,
    CAST(NULL AS STRING) AS Segment_Tag__c,
    Lifecycle_Stage__c,
    lifecycle_Date_Recycled__c,
    lifecycle_Last_MQL_Date__c,
    statushistory__last_status_updated__c AS Contact_Status_Update__c,
    lastmodifieddate,
    account6qa6sense__c,
    convertedcontactid,
    ownerid,
    createdbyid,
    masterrecordid,
    'Leads' AS _contact_leads_segments,
   
  FROM
    `x-marketing.jellyvision_salesforce.Lead` leads
  LEFT JOIN account_leads ON company = account_leads.name 
   WHERE isdeleted IS FALSE
), contacts AS (
SELECT
  case_sensitive_contact_id__c AS _case_sensitive_contact_id,
  id AS _contactid,
  firstname AS _firstname,
  contact.name AS _contactname,
  lastname AS _lastname,
  title AS _title,
  contact.accountid AS _accountid,
  account.name AS _accountname,
  email AS _email,
  mkto71_Lead_Score__c AS _lead_score,
  lifecycle_date_collected__c AS _date_collected,
  lifecycle_Date_MQL__c AS _date_mql,
  MQL_Reason__c AS _mql_reason,
  lifecycle_Date_SAL__c AS _date_sal, 
  lifecycle_Date_SQL__c AS _date_sql,
  Contact_Status__c AS _contact_status,
  createddate AS _createddate,
  Segment_Tag__c AS _segment_tag,
  Lifecycle_Stage__c AS _lifecycle_stage,
  lifecycle_Date_Recycled__c AS _date_recycled,
  lifecycle_Last_MQL_Date__c AS _last_mql,
  Contact_Status_Update__c AS _contact_status_update,
  lastmodifieddate AS _lastmodifieddate,
  account6qa6sense__c AS _account6qa6sense,
  CAST(NULL AS STRING) AS _converted_contactid,
  ownerid,
  createdbyid,
  masterrecordid,
  'Contacts' AS _contact_leads_segments
FROM
  contact
LEFT JOIN
  account
ON
  contact.accountid = account.accountid
) , user AS (
  SELECT
    id,
    name AS _owner_name
  FROM
    `x-marketing.jellyvision_salesforce.User` 
 ), combine AS (
SELECT * FROM contacts 
UNION ALL 
SELECT * FROM leads
 ) SELECT combine.*,
 user._owner_name,
 --created._owner_name
  FROM combine 
 LEFT JOIN user ON combine.ownerid = user.id;
 --WHERE  _contactname LIKE '%Kyle Plett%';


-- TRUNCATE TABLE `jellyvision.db_opportunity_log`;
-- INSERT INTO `jellyvision.db_opportunity_log`
CREATE OR REPLACE TABLE `jellyvision.db_opportunity_log` AS

WITH

base AS (
  SELECT
    opp.accountid,
    opp.id AS opp_id,
    opp.casesafeid__c AS casesafeid,
    type__c AS opp_record_type,
    opp.opp_owner_role__c AS opp_owner_role,
    opp.ownerid AS opp_owner_id,
    user.name AS opp_owner_name,
    account.name AS account_name,
    opp.account_type__c AS account_type,
    opp.account_domain_name__c AS account_domain_name,
    account.web_domain_name__c AS web_domain_name,
    opp.account_status__c AS account_status,
    opp.name AS opp_name,
    opp.stagename AS opp_stage,
    'Q' || CAST(opp.fiscalquarter AS STRING) || '-' || CAST(opp.fiscalyear AS STRING) AS fiscal_period,
    opp.amount,
    SAFE_DIVIDE(opp.probability, 100) AS probability,
    DATE(opp.createddate) AS created_date,
    opp.createddate AS _createddate,
    DATE(opp.closedate) AS closed_date,
    DATE(account.last_churn_date__c) AS last_churn_date,
    DATE(account.active_date_fw__c) AS max_product_date,
    contactid

  FROM `x-marketing.jellyvision_salesforce.Opportunity` as opp
  LEFT JOIN `x-marketing.jellyvision_salesforce.Account` AS account
    ON opp.accountid = account.id
  LEFT JOIN `x-marketing.jellyvision_salesforce.User` AS user
    ON opp.ownerid = user.id

  WHERE NOT opp.isdeleted
)

, toc AS (
  SELECT
    base.*,
    CASE
        WHEN last_churn_date IS NULL THEN
            CASE
                WHEN opp_record_type IN ("New Business", "Essentials") AND opp_stage IN ("Closed Won", "Closed Won Pending") THEN "Net-new Customer"
                WHEN max_product_date > CURRENT_DATE THEN "Existing Customer"
                WHEN opp_record_type IN ("New Business", "Essentials") AND opp_stage NOT IN ("Closed Won", "Closed Won Pending") THEN "Prospect"
                WHEN max_product_date < CURRENT_DATE THEN "Inactive Customer"
            END
        WHEN last_churn_date IS NOT NULL THEN
            CASE
                WHEN last_churn_date  <  max_product_date AND opp_record_type IN ("New Business", "Essentials") AND opp_stage IN ("Closed Won", "Closed Won Pending") AND max_product_date > CURRENT_DATE THEN "Net-new Customer"
                WHEN max_product_date > CURRENT_DATE THEN "Existing Customer"
                WHEN opp_record_type IN ("New Business", "Essentials") AND opp_stage NOT IN ("Closed Won", "Closed Won Pending") AND last_churn_date < CURRENT_DATE AND max_product_date < last_churn_date THEN "Prospect"
                WHEN last_churn_date < CURRENT_DATE THEN "Inactive Customer"
            END
    END AS type_of_customers

  FROM base

  QUALIFY ROW_NUMBER() OVER (PARTITION by accountid ORDER BY created_date DESC, closed_date DESC) = 1
) , opportunity AS (


SELECT
  base.*,
  toc.type_of_customers,


FROM base
LEFT JOIN toc
  ON base.accountid = toc.accountid
) ,
  contact AS (
  SELECT
    _contactid,
    _firstname,
    _contactname AS name,
    _lastname,
    _title,
    _accountname,
    _accountid,
    _email,
    _date_mql,
   _last_mql,
    _date_sal,
  _account6QA6sense,
  _contact_leads_segments
  FROM
    `x-marketing.jellyvision.db_contacts_accounts_log`
) , opportunity_contact AS (
  SELECT opportunity.*,
contact.* EXCEPT (_accountid,_contactid) 
FROM opportunity
LEFT JOIN contact ON opportunity.accountid = contact._accountid
) SELECT * EXCEPT(_createddate) ,
  CASE 
    WHEN _date_mql <= _createddate 
         AND _date_mql >= DATE_SUB(_createddate, INTERVAL 90 DAY) 
    THEN "Influenced" 
    ELSE "Not Influenced" 
  END AS Influenced
FROM opportunity_contact

QUALIFY ROW_NUMBER() OVER(PARTITION BY opp_id,accountid ORDER BY _date_mql DESC) = 1