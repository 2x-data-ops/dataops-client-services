CREATE OR REPLACE TABLE `x-marketing.pcs.googleanalytic_contact` AS 

-- Step 1: Optimize the Contact CTE
WITH contact AS (
   SELECT
    acc.id,
    acc.accountid AS _sfdcAccountID,
    CONCAT(acc.firstname, ' ', acc.lastname) AS _name,
    acc.title AS _title,
    acc.email AS _email,
    acc.phone AS _phone,
    email_domain__c AS _domain,
    account_name__c AS _companyname, 
    territory__c, 
    mailingcity AS _city,
    mailingcountry AS _country,
    mailingstate AS _state,
   fin.name, 
   total_market_value_amt__c,
   contributing__c,
   roth_cont_rate_amt__c, 
   roth_percentage__c, 
   roth_rate_eff_date__c, 
   roth_auto_enroll_cd__c, 
   defered_cont_rate_amt__c, 
   deferred_pct__c, 
   deferred_rate_eff_date__c,
   fin.program__c,
   fin.id AS financial_account_id,
   fin.ownerid,
   j.name AS owner_name,
   birthdate,
   discovery_date_of_birth_full__c,
   DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) AS age,
   CASE WHEN DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) <= 39 THEN "Early"
   WHEN DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) >= 40 AND DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) <= 59 THEN "Mid"
   WHEN DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) >= 60 THEN "End" END AS _age_segment,
  CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link,
  CAST(acc._sdc_sequence AS STRING) AS _sdc_sequence
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN (SELECT * EXCEPT (rownum)
FROM (
  
  SELECT 
  fin.program__c,fin.name,fin.id,
   fin.id AS financial_account_id,fin.ownerid,total_market_value_amt__c,
   contributing__c,
   roth_cont_rate_amt__c, 
   roth_percentage__c, 
   roth_rate_eff_date__c, 
   roth_auto_enroll_cd__c, 
   defered_cont_rate_amt__c, 
   deferred_pct__c, 
   deferred_rate_eff_date__c,account_holder__c,
    ROW_NUMBER() OVER(
            PARTITION BY account_holder__c
            ORDER BY lastmodifieddate DESC
        ) AS rownum
      FROM `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin
      WHERE isdeleted IS FALSE
    ) WHERE rownum = 1 ) fin ON acc.id =  fin.account_holder__c
  LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = fin.ownerid 
  WHERE acc.isdeleted IS FALSE 
),

-- Step 2: Optimize the Prep CTE by reducing the use of UNNEST
prep AS (
  SELECT
    event_date, 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    user_id, 
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ids') AS ids,
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    (MAX(event_timestamp) - MIN(event_timestamp))/1000000 AS session_length_in_seconds
  FROM `x-marketing.analytics_411351491.events_*`
  GROUP BY event_date, page_title, user_id, ids, session_id
),

-- Step 3: Optimize the Avg CTE by using the previously optimized Prep CTE
avg AS (
  SELECT
    event_date, 
    page_title,
    user_id,
    ids,
    SUM(session_length_in_seconds) / COUNT(DISTINCT session_id) AS average_session_duration_seconds
  FROM prep
  GROUP BY event_date, page_title, user_id, ids
),

-- Step 4: Utilize the GA CTE without modifications (already optimized)
ga AS ( 
  SELECT *
  FROM avg
),

-- Step 5: Optimize the Email Campaign CTE by reducing unnecessary columns and filters
email_campaign AS (
  SELECT *
  FROM (
    SELECT 
      _notes, 
      _status, 
      _trimcode, 
      _screenshot, 
      _assettitle, 
      _subject, 
      _whatwedo, 
      _campaignid AS airtable_id, 
      _utm_campaign, 
      _preview, 
      _code, 
      _journeyname,
      _campaignname, 
      _formsubmission, 
      _id, 
      _livedate, 
      _utm_source, 
      _emailname, 
      _assignee, 
      _utm_medium, 
      _landingpage,
      _emailsequence AS _segment,
      _link,
      _rootcampaign,
      _emailsegment,
      ROW_NUMBER() OVER(
          PARTITION BY _campaignid, _code 
          ORDER BY _livedate DESC
      ) AS rownum
    FROM `x-marketing.pcs_mysql.db_airtable_email_participant_engagement`
    WHERE _rootcampaign = 'Participant Education Series' 
      AND _campaignid IS NOT NULL 
      AND _campaignid != ''
  )
  WHERE rownum = 1
),

-- Step 6: Optimize the Get Campaign ID CTE by reducing UNNEST usage
get_campaignid AS (
  SELECT 
    CONCAT(event_date, event_timestamp, event_name) AS _id,
    REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'[\?&]j=([^&]*)') AS campaign_id,
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS ga_id
  FROM `x-marketing.analytics_411351491.events_*`
  WHERE (SELECT COUNT(*) FROM UNNEST(event_params) WHERE key = 'page_location') > 0
),

-- Step 7: Optimize the Campaign IDs CTE
campaign_ids AS (
  SELECT DISTINCT 
    _id,
    campaign_id,
    ga_id 
  FROM get_campaignid
  WHERE campaign_id IS NOT NULL
),

-- Step 8: Optimize the Google Analytic Activity CTE
google_analytic_activity AS (
  SELECT 
    activity.*,
    SPLIT(SUBSTR(traffic_source.name, STRPOS(traffic_source.name, '?j=') + 3), '&')[ORDINAL(1)] AS _campaignids,
    PARSE_TIMESTAMP('%Y%m%d', activity.event_date) AS _timestamp,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'event_value'),
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'event_value') AS STRING),
      CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'event_value') AS STRING),
      CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'event_value') AS STRING),
      CAST(TIMESTAMP_MICROS(event_previous_timestamp) AS STRING)
    ) AS events,
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    user.value.string_value AS user_value -- Include this to capture the `user` data
  FROM `x-marketing.analytics_411351491.events_*` activity
  LEFT JOIN UNNEST(user_properties) AS user -- Unnest user properties to access `user.value.string_value`
  WHERE EXISTS (SELECT 1 FROM UNNEST(event_params) WHERE key = 'ga_session_id')
),

-- Step 9: Optimize the All Data CTE by using the previously optimized CTEs
all_data AS (
  SELECT 
    activity.*,
    l.*,
    c.campaign_id
  FROM google_analytic_activity activity
  LEFT JOIN contact l ON activity.user_value = l.id -- Use `activity.user_value` to join with `contact`
  LEFT JOIN ga ON activity.user_value = ga.ids AND ga.event_date = activity.event_date
  LEFT JOIN campaign_ids c ON activity.session_id = c.ga_id
)

-- Final Select with Optimized Joins and Filters
SELECT 
  datas.*,
  email_campaign.*
FROM all_data AS datas
LEFT JOIN email_campaign ON datas.campaign_id = email_campaign.airtable_id;
