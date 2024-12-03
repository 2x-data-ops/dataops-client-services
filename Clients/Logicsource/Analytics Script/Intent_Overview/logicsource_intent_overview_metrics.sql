-- CREATE OR REPLACE TABLE `x-marketing.logicsource.engagement_opportunity` AS
TRUNCATE TABLE `x-marketing.logicsource.engagement_opportunity`;

INSERT INTO `x-marketing.logicsource.engagement_opportunity` (
  _opportunityID,
  _accountid,
  _accountname,
  _opportunityName,
  _currentStage,
  _createTS,
  _createTS_date,
  _closeTS,
  _amount,
  _acv,
  _type,
  _reason,
  _oppLastChangeinStage,
  campaignid,
  campaign_name,
  domain__c,
  annualrevenue,
  industry,
  arr_status__c,
  account_status__c,
  customer_status__c,
  contactname,
  firstname,
  lastname
)
WITH campaign AS (
  SELECT
    id,
    name
  FROM `x-marketing.logicsource_salesforce.Campaign`
),
fcrm__fcr_last_campaign_touch AS (
  SELECT
    id,
    name
  FROM `x-marketing.logicsource_salesforce.Campaign`
),
fcrm__fcr_first_campaign_touch AS (
  SELECT
    id,
    name
  FROM `x-marketing.logicsource_salesforce.Campaign`
),
account_info AS (
  SELECT DISTINCT
    main.id AS accountid,
    COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value) AS domain__c,
    annualrevenue,
    industry,
    0 AS arr_status__c,
    "" AS account_status__c,
    "" AS customer_status__c,
    --RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
    main.name AS accountname
  FROM `x-marketing.logicsource_salesforce.Account` main
  LEFT JOIN `x-marketing.logicsource_hubspot.contacts` k
    ON main.id = k.property_salesforceaccountid.value 
  --JOIN `x-marketing.logicsource_salesforce.Contact` supp ON main.id = supp.accountid
),
contact AS (
  SELECT
    id AS contactid,
    accountid,
    ownerid,
    name AS contactname,
    firstname,
    lastname,
    "" AS status_reason__c,
    "" AS client_status__c
  FROM `x-marketing.logicsource_salesforce.Contact`
) --,opps_created AS (
SELECT DISTINCT
  main.id AS _opportunityID,
  main.accountid AS _accountid,
  accountname AS _accountname,
  main.name AS _opportunityName,
  stagename AS _currentStage,
  main.createddate AS _createTS,
  DATE(main.createddate) AS _createTS_date,
  closedate AS _closeTS,
  amount AS _amount,
  CAST(NULL AS INTEGER) AS _acv,
  --_domain,
  main.type AS _type,
  "" AS _reason,
  laststagechangedate AS _oppLastChangeinStage,
  campaignid,
  campaign.name AS campaign_name,
  domain__c,
  annualrevenue,
  industry,
  arr_status__c,
  account_status__c,
  customer_status__c,
  contactname,
  firstname,
  lastname,
  /* previous_stage__c AS _previousStage,
  days_in_stage__c AS _daysCurrentStage */
FROM `x-marketing.logicsource_salesforce.Opportunity` main
LEFT JOIN campaign
  ON main.campaignid = campaign.id
LEFT JOIN account_info
  ON main.accountid = account_info.accountid
LEFT JOIN contact
  ON main.contactid = contact.contactid
WHERE main.isdeleted = FALSE;

--AND main.name = 'Shoprite South Africa'
--AND main.type !='Renewal'
--AND LOWER(accountname) NOT LIKE '%logicsource%'
--AND EXTRACT(YEAR FROM main.createddate ) IN (2022, 2023);
/* 
Script to run the pipeline related data for the intent-driven marketing dashboard
 */
--CREATE OR REPLACE TABLE `x-marketing.logicsource.dashboard_engagement_opportunity` AS
TRUNCATE TABLE `x-marketing.logicsource.dashboard_engagement_opportunity`;

INSERT INTO `x-marketing.logicsource.dashboard_engagement_opportunity` (
  _opportunityid,
  _accountid,
  rownum,
  _domain,
  _opportunityName,
  _engagementID,
  _opportunityCreated,
  _opportunityWon,
  _opportunityLost,
  _opportunityValue,
  _currentstage,
  _previousStage,
  _stageMovement,
  _oppLastChangeinStage,
  _engagementDate,
  _email,
  _jobtitle,
  _accountname,
  _engagement,
  _engagement_activities,
  _description,
  _t90_days_score,
  _type,
  _isInfluence,
  _isAccelerate,
  _uniqueID
)
WITH account_info AS (
  SELECT DISTINCT
    main.id AS accountid,
    RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain,
    main.name AS accountname
  FROM `x-marketing.logicsource_salesforce.Account` main
  JOIN `x-marketing.logicsource_salesforce.Contact` supp
    ON main.id = supp.accountid
),
account_score AS (
  SELECT DISTINCT
    _domain,
    EXTRACT(WEEK FROM scores._extract_date) AS _week,
    EXTRACT(YEAR FROM scores._extract_date) AS _year,
    (
      COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score, 0) +
      COALESCE(_quarterly_organic_social_score, 0) + COALESCE(_quarterly_form_fill_score, 0) +
      COALESCE(_quarterly_paid_ads_score, 0) + COALESCE(_quarterly_web_score, 0) +
      COALESCE(_quarterly_organic_social_score, 0)
    ) AS _t90_days_score,
  FROM `x-marketing.logicsource.account_90days_score` scores
),
engagements AS (
  SELECT DISTINCT
    *
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement NOT IN ('Opportunity Created', 'Opportunity Stage Change')
    AND _engagement IS NOT NULL
),
account_engagement AS (
  SELECT DISTINCT
    eng._domain,
    _sfdcaccountid AS accountid,
    _company,
    _engagement,
    _email,
    _date AS _timestamp,
    _jobtitle,
    _seniority,
    _contentTitle,
    _description,
    -- _weekly_first_party_score,
    -- _ytd_first_party_score,
    -- Updated to use the new T90 Day window scoring.
    MD5(CONCAT(eng._domain, _engagement, _date, _contentTitle, _email)) AS _engagementID
  FROM engagements AS eng
),
opps_created AS (
  SELECT DISTINCT
    main.id AS _opportunityID,
    accountid AS _accountid,
    accountname AS _accountname,
    main.name AS _opportunityName,
    stagename AS _currentStage,
    main.createddate AS _createTS,
    closedate AS _closeTS,
    amount AS _amount,
    CAST(NULL AS INTEGER) AS _acv,
    _domain,
    main.type AS _type,
    loss_reason__c AS _reason,
    laststagechangedate AS _oppLastChangeinStage,
    /* previous_stage__c AS _previousStage,
    days_in_stage__c AS _daysCurrentStage */
  FROM `x-marketing.logicsource_salesforce.Opportunity` main
  JOIN account_info
    USING (accountid)
  WHERE main.isdeleted = FALSE
    AND main.type != 'Renewal'
    AND LOWER(accountname) NOT LIKE '%logicsource%'
    AND EXTRACT(YEAR FROM main.createddate) IN (2022, 2023)
),
opp_hist_source AS (
  SELECT DISTINCT
    opportunityid AS _opportunityid,
    -- createddate AS _oppLastChangeinStage,
    oldvalue AS _previousstage,
    -- newvalue AS _currentstage,
    ROW_NUMBER() OVER (PARTITION BY opportunityid ORDER BY createddate DESC) AS _order
  FROM `x-marketing.logicsource_salesforce.OpportunityFieldHistory`
  WHERE field = 'StageName'
    /*  ORDER BY _oppLastChangeinStage DESC */
),
opp_hist AS (
  SELECT
    *
  FROM opp_hist_source
  RIGHT JOIN opps_created
    USING (_opportunityid)
  WHERE _order = 1
),
combined_data AS (
  SELECT DISTINCT
    opp_hist._opportunityID,
    opp_hist._accountid,
    account_engagement._domain,
    opp_hist._opportunityName,
    account_engagement._engagementID,
    DATE(opp_hist._createTS) AS _opportunityCreated,
    IF(_currentstage = 'Closed Won', DATE(opp_hist._closeTS), NULL) AS _opportunityWon,
    IF(_currentstage = 'Closed Lost', DATE(opp_hist._closeTS), NULL) AS _opportunityLost,
    COALESCE(_acv, _amount) AS _opportunityValue,
    _currentstage,
    _previousStage,
    -- _daysCurrentStage,
    CASE
      WHEN SPLIT(_previousstage, '.') [OFFSET(0)] > SPLIT(_currentstage, '.') [OFFSET(0)] THEN 'Downward'
      ELSE 'Upward'
    END AS _stageMovement,
    _oppLastChangeinStage AS _oppLastChangeinStage,
    DATE(account_engagement._timestamp) AS _engagementDate,
    _email,
    _jobtitle,
    _accountname,
    _engagement,
    _contentTitle AS _engagement_activities,
    _description,
    _t90_days_score,
    -- _ytd_first_party_score,
    opp_hist._type,
    -- Logic to determine the influenced opp. 
    (IF(DATE(account_engagement._timestamp) <= DATE(opp_hist._createTS), 1, 0)) AS _isInfluence,
  FROM opp_hist
  LEFT JOIN account_engagement 
    USING (_domain)
  LEFT JOIN account_score
    ON (
      EXTRACT(WEEK FROM opp_hist._createTS) = account_score._week
      AND EXTRACT(YEAR FROM opp_hist._createTS) = account_score._year
    )
    AND opp_hist._domain = account_score._domain
  WHERE LOWER(_accountname) NOT LIKE '%logicsource%'
),
get_accelerated AS (
  SELECT
    *,
    (
      CASE
        WHEN _isInfluence = 0
        AND _currentStage NOT LIKE '%Nurture%'
        AND (
          _engagementDate >= _opportunityCreated
          AND TIMESTAMP(_engagementDate) <= _oppLastChangeinStage
        )
        AND _opportunityLost IS NULL
        AND _opportunityWon IS NULL THEN 1
        ELSE 0
      END
    ) AS _isAccelerate,
    MD5(CONCAT(CAST(_opportunityID AS BYTES), _engagementID)) AS _uniqueID,
    ROW_NUMBER() OVER (PARTITION BY _opportunityID ORDER BY _engagementDate DESC) AS rownum
  FROM combined_data
),
get_accelerated_is_influenced AS (
  SELECT DISTINCT
    _opportunityID,
    _accountid,
    MIN(rownum) AS rownum
  FROM get_accelerated
  WHERE (_isInfluence = 1)
  GROUP BY 1, 2
),
opp_influenced AS (
  SELECT
    *
  FROM get_accelerated
  JOIN get_accelerated_is_influenced
    USING (_opportunityid, _accountid, rownum)
  WHERE (_isInfluence = 1)
),
get_accelerated_is_accelerrated AS (
  SELECT DISTINCT
    _opportunityID,
    _accountid,
    MIN(rownum) AS rownum
  FROM get_accelerated
  WHERE (_isAccelerate = 1)
  GROUP BY 1, 2
),
opp_accelerated AS (
  SELECT
    *
  FROM get_accelerated
  JOIN get_accelerated_is_accelerrated
    USING (_opportunityid, _accountid, rownum)
  WHERE (_isAccelerate = 1)
    AND _opportunityID NOT IN (
      SELECT DISTINCT
        _opportunityID
      FROM opp_influenced
    )
)
SELECT
  *
FROM opp_influenced
UNION DISTINCT
SELECT
  *
FROM opp_accelerated;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------- Account Influence Script ------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--CREATE OR REPLACE TABLE `x-marketing.logicsource.dashboard_account_opportunity` AS
TRUNCATE TABLE `x-marketing.logicsource.dashboard_account_opportunity`;

INSERT INTO `x-marketing.logicsource.dashboard_account_opportunity` (
  _domain,
  _distinctOpen,
  _distinctClick,
  _distinctContactUsForm,
  _distinctWebinarForm,
  _distinctWebinarattended,
  _distinctGatedContent,
  _distinctcontentsync,
  _distinctpaidadsshare,
  _distinctpaidadscomment,
  _distinctpaidadsfollow,
  _distinctpaidadsvisit,
  _distinctpaidadsclick_like,
  _distinctorganicadscomment,
  _distinctorganicadsfollow,
  _distinctorganicadsvisit,
  _distinctorganicadsclick_like,
  _email_score,
  _content_synd_score,
  _organic_social_score,
  _contact_us_form_score,
  _other_form_fill_score,
  _paid_ads_score,
  _quarterly_email_score,
  _quarterly_content_synd_score,
  _quarterly_paid_ads_score,
  _quarterly_form_fill_score,
  _quarterly_organic_social_score,
  _website_time_spent,
  _website_page_view,
  _website_visitor_count,
  _career_page_count,
  _visited_website,
  _website_time_spent_score,
  _website_page_view_score,
  _website_visitor_count_score,
  _career_page_score,
  _visited_website_score,
  _quarterly_web_score,
  _extract_date,
  _Tminus90_date,
  _engagement_score,
  _quarterly_engagement_cluster,
  _target_account,
  _accountid,
  _opps_created,
  _total_opps_value,
  _total_active_opps,
  _total_active_influenced_opps,
  _total_closed_opps,
  _total_closed_influenced_opps
)
WITH account_info AS (
  SELECT DISTINCT
    main.id AS accountid,
    RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain,
    main.name AS accountname,
    CAST(NULL AS BOOLEAN) target_account__c
  FROM `x-marketing.logicsource_salesforce.Account` main
  JOIN `x-marketing.logicsource_salesforce.Contact` supp
    ON main.id = supp.accountid
),
score AS (
  SELECT DISTINCT
    *,
    (
      COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score, 0) +
      COALESCE(_quarterly_organic_social_score, 0) + COALESCE(_quarterly_form_fill_score, 0) +
      COALESCE(_quarterly_paid_ads_score, 0) + COALESCE(_quarterly_web_score, 0) +
      COALESCE(_quarterly_organic_social_score, 0)
    ) AS _engagement_score
  FROM `x-marketing.logicsource.account_90days_score`
),
pivot_engagement AS (
  SELECT DISTINCT
    score.*,
    CASE 
      WHEN _engagement_score > 14 THEN 'High'
      WHEN _engagement_score BETWEEN 1 AND 14 THEN 'Low'
      ELSE 'No'
    END AS _quarterly_engagement_cluster,
    IF(target_account__c = TRUE, 1, 0) AS _target_account,
    ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY _extract_date DESC) AS _order
  FROM score
  LEFT JOIN account_info
    USING (_domain)
),
influenced_opps AS (
  SELECT DISTINCT
    _opportunityid AS id,
    _opportunityCreated,
    _isInfluence
  FROM `x-marketing.logicsource.dashboard_engagement_opportunity`
  WHERE _isInfluence = 1
),
opps_created AS (
  SELECT DISTINCT
    main.id AS _opportunityID,
    accountid AS _accountid,
    accountname AS _accountname,
    main.name AS _opportunityName,
    stagename AS _currentStage,
    main.createddate AS _createTS,
    closedate AS _closeTS,
    amount AS _amount,
    CAST(NULL AS INTEGER) AS _acv,
    _domain,
    main.type AS _type,
    loss_reason__c AS _reason,
    laststagechangedate AS _oppLastChangeinStage,
    /* previous_stage__c AS _previousStage,
    days_in_stage__c AS _daysCurrentStage */
    _isinfluence
  FROM `x-marketing.logicsource_salesforce.Opportunity` main
  JOIN account_info
    USING (accountid)
  LEFT JOIN influenced_opps
    USING (id)
  WHERE main.isdeleted = FALSE
    AND main.type != 'Renewal'
    AND LOWER(accountname) NOT LIKE '%logicsource%'
    AND EXTRACT(YEAR FROM main.createddate) IN (2022, 2023)
),
pivoted_opps AS (
  SELECT DISTINCT
    _domain,
    _accountid,
    COUNT(DISTINCT _opportunityID) AS _opps_created,
    SUM(_amount) AS _total_opps_value,
    SUM(IF(_currentStage NOT LIKE '%Closed%', _amount, 0)) AS _total_active_opps,
    SUM(IF(_currentStage NOT LIKE '%Closed%' AND _isInfluence = 1, _amount, 0)) AS _total_active_influenced_opps,
    SUM(IF(_currentStage LIKE '%Closed%', _amount, 0)) AS _total_closed_opps,
    SUM(IF(_currentStage LIKE '%Closed%' AND _isInfluence = 1, _amount, 0)) AS _total_closed_influenced_opps,
  FROM opps_created
  GROUP BY 1, 2
),
combined_data AS (
  SELECT
    *
  FROM pivot_engagement
  LEFT JOIN pivoted_opps
    USING (_domain)
  WHERE LOWER(_domain) NOT LIKE '%logicsource%'
    AND _order = 1
)
SELECT DISTINCT
  * EXCEPT (_order)
FROM combined_data
ORDER BY _extract_date DESC;