--------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------- Accounth Health Script ------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------


--CREATE OR REPLACE TABLE `logicsource.db_consolidated_engagements_log` AS 
TRUNCATE TABLE `logicsource.db_consolidated_engagements_log`;
INSERT INTO `logicsource.db_consolidated_engagements_log`
--CREATE OR REPLACE TABLE `logicsource.db_consolidated_engagements_log` AS 
 WITH 
#Query to pull all the contacts in the leads table from Hubspot
contacts AS (
  SELECT * EXCEPT( _rownum) 
  FROM (
     SELECT 
        CAST(vid AS STRING) AS _id,
        property_email.value AS _email,
        CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
        COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain, 
        properties.jobtitle.value AS _jobtitle,
       CASE WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value ELSE property_management_level.value END AS _seniority,
       CASE WHEN property_job_role__organic_.value IS NOT NULL THEN property_job_role__organic_.value ELSE property_job_role.value END AS _jobrole,
        property_lead_segment.value AS _lead_segment, 
         properties.hs_analytics_source.value AS _source,
        properties.job_function.value AS _function,
        property_phone.value AS _phone,
        associated_company.properties.name.value AS _company,
        property_leadstatus.value AS _leadstatus, 
        CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
          CASE WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
      WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
      WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
      WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
      WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
      WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet' ELSE associated_company.properties.industry.value
      END AS _industry,
        property_city.value AS _city, 
        property_state.value AS _state,
        property_country.value AS _country,
        CAST(associated_company.company_id AS STRING) AS _persona,
        property_lifecyclestage.value AS _lifecycleStage,
        form_submissions,
        property_salesforceaccountid.value AS _sfdcaccountid,
        property_salesforcecontactid.value AS _sfdccontactid,
        ROW_NUMBER() OVER( PARTITION BY property_email.value, CONCAT(property_firstname.value, ' ', property_lastname.value) ORDER BY vid DESC) AS _rownum
      FROM 
        `x-marketing.logicsource_hubspot.contacts` k
        LEFT JOIN `x-marketing.logicsource_salesforce.Lead`  lead ON k.properties.salesforceleadid.value = lead.id
      WHERE --vid = 12346251
        property_email.value IS NOT NULL 
        AND property_email.value NOT LIKE '%2x.marketing%'
        AND property_email.value NOT LIKE '%logicsourceworkplace.com%'
      ) 
  WHERE _rownum = 1
), accounts  AS (
SELECT * EXCEPT (_order)
FROM (
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _id DESC) AS _order
FROM (
SELECT *EXCEPT(_order) 
 FROM 
  (
    SELECT 
      DISTINCT properties.domain.value AS _domain, 
      CAST( sfdc.companyid AS STRING) AS _id,
      CAST(NULL AS STRING) AS _name, 
      -- CAST(NULL AS STRING) AS _lastname,
      CAST(NULL AS STRING) AS _title,
      CAST(NULL AS STRING) AS _seniority,
            CAST(NULL AS STRING) AS _jobrole,
      CAST(NULL AS STRING) AS _lead_segment, 
       CAST(NULL AS STRING) AS _source,
     CAST(NULL AS STRING) AS _function,
      _phone,
      _company, 
      CAST(NULL AS STRING) AS _leadstatus, 
      CAST(_revenue AS STRING) AS _revenue,
      _industry AS _industry, 
      _city AS _city,
      _state AS _state,
      _country AS _country,
      CAST(NULL AS STRING) AS _persona,
      CAST(NULL AS STRING) AS _lifecycleStage,
      _sfdcaccountid,
      CAST(NULL AS STRING) _sfdccontactid,
       ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY property_hs_lastmodifieddate.value DESC) AS _order
    FROM 
      contacts
    RIGHT JOIN
      `logicsource_hubspot.companies` sfdc ON contacts._persona = CAST( sfdc.companyid AS STRING)
  )
  WHERE
    _order = 1
     UNION ALL 
      SELECT DISTINCT _domain AS _domain, 
 CAST(NULL AS STRING)  AS _id,
      CAST(NULL AS STRING) AS _name, 
      -- CAST(NULL AS STRING) AS _lastname,
      CAST(NULL AS STRING) AS _title,
      CAST(NULL AS STRING) AS _seniority,
            CAST(NULL AS STRING) AS _jobrole,
      CAST(NULL AS STRING) AS _lead_segment, 
       CAST(NULL AS STRING) AS _source,
     CAST(NULL AS STRING) AS _function,
      _phone,
      _domain, 
      CAST(NULL AS STRING) AS _leadstatus, 
      CAST(_revenue AS STRING) AS _revenue,
      _industry AS _industry, 
      _city AS _city,
      _city AS _state,
      _country AS _country,
      CAST(NULL AS STRING) AS _persona,
      CAST(NULL AS STRING) AS _lifecycleStage,
      CAST(NULL AS STRING) AS _sfdcaccountid,
      CAST(NULL AS STRING) _sfdccontactid, 
 FROM `logicsource.dashboard_mouseflow_kickfire`
 WHERE 
 (_domain IS NOT NULL AND _domain != '')
 UNION ALL 
  SELECT DISTINCT _accountdomain AS _accountdomain, 
 CAST(NULL AS STRING)  AS _id,
      CAST(NULL AS STRING) AS _name, 
      -- CAST(NULL AS STRING) AS _lastname,
      CAST(NULL AS STRING) AS _title,
      CAST(NULL AS STRING) AS _seniority,
            CAST(NULL AS STRING) AS _jobrole,
      CAST(NULL AS STRING) AS _lead_segment, 
       CAST(NULL AS STRING) AS _source,
     CAST(NULL AS STRING) AS _function,
       CAST(NULL AS STRING) AS _phone,
      _accountdomain, 
      CAST(NULL AS STRING) AS _leadstatus, 
      CAST(NULL AS STRING)  AS _revenue,
      _industry AS _industry, 
      CAST(NULL AS STRING)  AS _city,
      CAST(NULL AS STRING)  AS _state,
      CAST(NULL AS STRING)  AS _country,
      CAST(NULL AS STRING) AS _persona,
      CAST(NULL AS STRING) AS _lifecycleStage,
      CAST(NULL AS STRING) AS _sfdcaccountid,
      CAST(NULL AS STRING) _sfdccontactid, 
 FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement` 
 WHERE 
 (_accountdomain IS NOT NULL AND _accountdomain != '')
)
) WHERE _order = 1
) ,
#Query to pull the email engagement 
email_engagement AS (
    SELECT 
      *
    FROM ( 
      SELECT DISTINCT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _date,
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle AS _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description,
      0 AS frequency,
      FROM 
        (SELECT * FROM `logicsource.db_email_engagements_log`)
      WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ LOWER(_engagement) NOT IN ('sent','delivered', 'downloaded', 'bounced', 'unsubscribed', 'processed', 'deffered', 'spam', 'suppressed', 'dropped','mql')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|logicsource') 
      AND _domain IS NOT NULL 
    ORDER BY 
      1, 3 DESC, 2 DESC
),
web_engagements AS (
  SELECT
    DISTINCT _visitorid AS _email, 
    _domain, 
    _timestamp, 
    EXTRACT(WEEK FROM _timestamp) AS _week,  
    EXTRACT(YEAR FROM _timestamp) AS  _year, 
    _page AS _webActivity, 
    "Web Visit" AS _engagement, 
    CONCAT(
      "Engagement Time: ", _engagementtime, "\n",
      "utm_source: ", _utmsource, "\n",
      "utm_campaign: ", _utmcampaign, "\n",
      "utm_medium: ", _utmmedium, "\n",
      "utm_content: ", _utmcontent, "\n") AS _description,
      0 AS frequency,
  FROM
    `x-marketing.logicsource.db_web_engagements_log`
  --WHERE 
    --NOT REGEXP_CONTAINS(LOWER(_fullurl), 'unsubscribe')
    --AND NOT REGEXP_CONTAINS(LOWER(_fullurl), '=linkedin|=google|=6sense')
  ORDER BY 
    _domain, _timestamp DESC
),
ad_clicks AS (
  /*SELECT 
    DISTINCT _visitorid AS _email, 
    _domain, 
    _timestamp, 
    EXTRACT(WEEK FROM _timestamp) AS _week,  
    EXTRACT(YEAR FROM _timestamp) AS  _year, 
    _page AS _webActivity, 
    "Ad Clicks" AS _engagement, 
    CONCAT(
      "utm_source: ", _utmsource, "\n",
      "utm_campaign: ", _utmcampaign, "\n",
      "utm_medium: ", _utmmedium, "\n",
      "utm_content: ", _utmcontent, "\n"
      ) AS _description, 
  FROM 
    `x-marketing.logicsource.db_web_engagements_log`
  WHERE 
    NOT REGEXP_CONTAINS(LOWER(_fullurl), 'unsubscribe')
    AND REGEXP_CONTAINS(LOWER(_fullurl), '=linkedin|=google|=6sense')*/
    SELECT 
    _contactemail, 
    _accountdomain,  
    CAST(_date AS TIMESTAMP) AS _timestamp,
    EXTRACT(WEEK FROM CAST(_date AS TIMESTAMP)) AS _week,
    EXTRACT(YEAR FROM CAST(_date AS TIMESTAMP)) AS _year,
   '' AS _webActivity, 
    _medium AS _engagement, 
    CONCAT ( _engagementtype, "-", _frequency ) AS _description,
    SAFE_CAST(_frequency AS INT64) AS frequency,

FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement` 
WHERE _medium = 'Paid Ads'
),
content_engagement AS (
  SELECT 
    DISTINCT _visitorID AS _email, 
    _domain, 
    TIMESTAMP(_visitDate) AS _date, 
    EXTRACT(WEEK FROM _visitDate) AS _week,  
    EXTRACT(YEAR FROM _visitDate) AS _year, 
    _title,
    "Content Engagement" AS _engagement, 
    CONCAT("Total Page Views: ", _pageviews) AS _description,
    0 AS frequency,
  FROM 
    `x-marketing.logicsource.db_content_engagements_log`
  /* WHERE 
    REGEXP_CONTAINS(LOWER(_page), '/blog/') */
),
/* web_activities_6sense AS (
    SELECT
      DISTINCT CAST(NULL AS STRING) AS _email,
      _6sensedomain AS _domain,
      TIMESTAMP(main._activities_on) AS _activities_on,
      EXTRACT(WEEK FROM main._activities_on) AS _week,
      EXTRACT(YEAR FROM main._activities_on) AS _year, 
      urls,
      "Web Visits" AS _engagement,  
      CONCAT("Visit Count: ", _webvisitcount) AS _webvisitcount
    FROM
      `logicsource.6sense_surging_web_keywords` main,
      UNNEST(SPLIT(_weburls, ", ")) AS urls
    WHERE
      LENGTH(_weburls) > 1
  ), */
form_fills AS (
  SELECT * EXCEPT (rownum) 
  FROM (
    SELECT 
      activity.email AS _email,
      _domain AS _domain,
      activity.timestamp AS _date,
      EXTRACT(WEEK FROM activity.timestamp) AS _week,  
      EXTRACT(YEAR FROM activity.timestamp) AS _year,
      form_title,
      'Form Filled' AS _engagement,
      activity.description AS _description,
      0 AS frequency,
     ROW_NUMBER() OVER(PARTITION BY email, description ORDER BY timestamp DESC) AS rownum
    FROM (
       SELECT 
          CAST(NULL AS STRING) AS devicetype,
          SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_hsmi=') + 9), '&')[ORDINAL(1)] AS _campaignID, #utm_content
          REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%20', ' '), '%3A',':') AS _campaign,
          SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_source=') + 8), '&')[ORDINAL(1)] AS _utm_source,
            form.value.title AS form_title,
          properties.email.value AS email, 
          form.value.timestamp AS timestamp, 
          'Downloaded' AS engagement,
          form.value.page_url AS description,
          campaignguid,
         COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain,
        FROM  
           `x-marketing.logicsource_hubspot.contacts` contacts, UNNEST(form_submissions) AS form
        LEFT JOIN 
          `x-marketing.logicsource_hubspot.forms` forms ON form.value.form_id = forms.guid
         -- WHERE    properties.email.value = 'michelle.fuentesfina@roquette.com'
        ) activity
    --LEFT JOIN 
     -- `x-marketing.logicsource_hubspot.campaigns` campaign ON activity._campaignID = CAST(campaign.id AS STRING)
  )
  WHERE 
    rownum = 1
    --AND 
   --AND _domain  = 'key-notion.com'
   UNION ALL 
    SELECT 
    _contactemail, 
    _accountdomain,  
    CAST(_date AS TIMESTAMP) AS _timestamp,
    EXTRACT(WEEK FROM CAST(_date AS TIMESTAMP)) AS _week,
    EXTRACT(YEAR FROM CAST(_date AS TIMESTAMP)) AS _year,
   _engagementtype AS _webActivity, 
    'Form Filled' AS _engagement, 
      _medium AS _description,
      0 AS frequency,
FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement` 
WHERE _medium = 'content syndication'
UNION ALL 
SELECT DISTINCT property_email.value,
COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain, 
property_lastmodifieddate.value,
EXTRACT( WEEK FROM property_lastmodifieddate.value ) AS _week ,
EXTRACT( YEAR FROM property_lastmodifieddate.value ) AS _year,
"Webinar Engagement" AS form_title,
'Form Filled' AS _engagement,
property_event_activity.value AS _description,
0 AS frequency,
 FROM `x-marketing.logicsource_hubspot.contacts` 
 WHERE property_event_activity.value IN ("Visited booth","Registered","Attended event")
),
 dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
),
/*intent_score AS (
  SELECT DISTINCT *,  
    FROM (
        SELECT 
          CAST(NULL AS STRING) AS _email, 
          _domain,
          EXTRACT(DATETIME FROM report._date) AS _date,
          EXTRACT(WEEK FROM _date)-1 AS _week, 
          EXTRACT(YEAR FROM _date) AS _year,
          "Bombora",
          "Bombora" AS _engagements,
          STRING_AGG(CONCAT(_topicname), ", ") OVER(PARTITION BY _domain)AS _topics,
          MAX(CAST(_averagecompositescore AS INT64)) OVER(PARTITION BY _domain) AS _weekly_avgCompositeScore,
        FROM 
          `logicsource.bombora_surge_report` report
    )
),  */
first_party_score AS (
  SELECT 
    DISTINCT _domain, _extract_date AS _extract_date,
    EXTRACT(WEEK FROM _extract_date)  AS _week,  -- Minus 1 as the score is referring to the week before.
    EXTRACT(YEAR FROM _extract_date) AS _year,
    (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score , 0)+ COALESCE(_quarterly_organic_social_score , 0)+ COALESCE(_quarterly_form_fill_score , 0)+ COALESCE(_quarterly_paid_ads_score , 0)+ COALESCE(_quarterly_web_score, 0)+ COALESCE(_quarterly_organic_social_score, 0)) AS _t90_days_score
  FROM
   `logicsource.account_90days_score`
  ORDER BY
    _extract_date  DESC
/* ), 
engagement_grade AS (
  SELECT 
    DISTINCT _week, 
    _year, 
    _email, 
    _weekly_contact_score,
    _ytd_contact_score,
    (
      CASE 
      WHEN _ytd_contact_score < 59 THEN 'C'
      WHEN _ytd_contact_score BETWEEN 60 AND 79 THEN 'B'
      WHEN _ytd_contact_score >= 80 THEN 'A'
      END
    ) AS _ytd_grade 
  FROM 
    `logicsource.contact_engagement_scoring` 
  ORDER BY 
    _week DESC */
),
# Combining the engagements - Contact based and account based engagements
engagements AS (
# Contact based engagement query
  SELECT DISTINCT
     contacts._domain, 
    contacts._email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    CAST(NULL AS INTEGER) AS _avg_bombora_score,
    contacts.*EXCEPT(_domain, _email, form_submissions),
    engagements._date ,
    CAST(engagements._date AS DATE) AS _extract_date
  FROM 
    dummy_dates
  JOIN (
    SELECT * FROM email_engagement 
    UNION ALL
    SELECT * FROM form_fills
  ) engagements USING(_week, _year)
  RIGHT JOIN
    contacts  ON engagements._email = contacts._email
  UNION ALL
# Account based engagement query
  SELECT 
    DISTINCT accounts._domain, 
    CAST(NULL AS STRING) AS _email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_timestamp, _week, _year, _domain, _email),
    accounts.*EXCEPT(_domain),
    engagements._timestamp,
    CAST(engagements._timestamp AS DATE) AS _extract_date
  FROM 
    dummy_dates
  CROSS JOIN
    accounts
  JOIN (
    -- SELECT * FROM intent_score UNION ALL
    SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM web_engagements UNION ALL
    SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM ad_clicks 
    --UNION ALL
    --SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM content_engagement
  ) engagements USING(_domain, _week, _year)
 ),
sfdc AS (
  SELECT 
    DISTINCT accountid, 
    acc.name AS _accountname, 
    annualrevenue AS _annualrevenue, 
    acc.industry AS _industry, 
    cnt.id AS contactid, 
    RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain,
    cnt.mailingcity AS _city,
    cnt.mailingstate AS _state,
    cnt.mailingcountry AS _country
  FROM 
    `logicsource_salesforce.Contact` cnt
  JOIN
    `logicsource_salesforce.Account` acc ON cnt.accountid = acc.id
),
opps_created AS (
  SELECT
    DISTINCT main.id AS _opportunityID, 
    main.accountid AS _accountid,
    sfdc.contactid AS _contactid,
    _accountname,
    main.name AS _opportunityName, 
    stagename AS _currentStage,
    main.createddate AS _createTS,
    -- closedate AS _closeTS,
    amount AS _amount,
    -- acv__c AS _acv,
    _domain,
    _industry,
    CAST(NULL AS INTEGER) AS _tier,
    _annualrevenue,
    main.type AS _type,
    -- reason__c AS _reason,
    laststagechangedate AS _oppLastChangeinStage,
    _city,
    _state,
    _country,
    CAST(NULL AS INT64) AS _t90_days_score
  FROM
    `logicsource_salesforce.Opportunity` main
  JOIN
    sfdc USING(accountid)
  /* LEFT JOIN
    first_party_score USING(_domain) */
  WHERE
    main.isdeleted = False
    AND main.type !='Renewal'
    AND LOWER(_accountname) NOT LIKE '%logicsource%'
    AND EXTRACT(YEAR FROM main.createddate ) IN (2022, 2023,2024)
),
opp_hist AS(
  SELECT
    *
  FROM
  (
    SELECT
      DISTINCT opportunityid AS _opportunityid,
      createddate AS _oppLastChangeinStage,
      oldvalue AS _previousstage,
      newvalue AS _currentstage,
      ROW_NUMBER() OVER(PARTITION BY opportunityid ORDER BY createddate DESC) AS _order
    FROM
      `logicsource_salesforce.OpportunityFieldHistory`
    WHERE
      field = 'StageName'
    ORDER BY
      _oppLastChangeinStage DESC
  )
  WHERE
    _order = 1
),
opps_stage_change AS (
  SELECT
    _domain,
    _accountid,
    _accountname, 
    _opportunityname,
    _amount,
    opps_created._currentStage,
    _oppLastChangeinStage,
    _previousStage,
    _industry,
    _tier,
    _annualrevenue,
    _city,
    _state,
    _country,
    _t90_days_score,
    _contactid,
  FROM
    opps_created
  JOIN
    opp_hist USING(_opportunityid, _oppLastChangeinStage)
)
SELECT 
  DISTINCT engagements.*, 
  COALESCE(_t90_days_score, 0) AS _t90_days_score, 
  -- COALESCE(_ytd_first_party_score, 0) AS _ytd_first_party_score, 
  -- engagement_grade._weekly_contact_score, 
  -- engagement_grade._ytd_contact_score,
  -- engagement_grade._ytd_grade
FROM 
  engagements
LEFT JOIN 
  first_party_score USING(_domain, _week, _year,_extract_date)
WHERE
  LENGTH(_domain) > 1
/*UNION DISTINCT
SELECT
  DISTINCT _domain,
  CAST(NULL AS STRING) AS _email ,
  EXTRACT(WEEK FROM _createTS) AS _week,
  EXTRACT(YEAR FROM _createTS) AS _year,
  _opportunityname AS _contentTitle,
  "Opportunity Created" AS _engagement,
  CONCAT('Amount: $', FORMAT("%'.2f", _amount), "\n", "Current Stage: ", _currentStage) AS _description,
  NULL AS _avg_bombora_score,
  CAST(NULL AS STRING) AS  _id,
  CAST(NULL AS STRING) AS  _name,
  CAST(NULL AS STRING) AS  _title,
  CAST(NULL AS STRING) AS  _2xseniority,
  CAST(NULL AS STRING) AS  _phone,
  _accountname,
  CAST(_annualrevenue AS STRING) AS _annualrevenue,
  _industry,
  _city,
  _state,
  _country,
  CAST(NULL AS STRING) AS _persona,
  CAST(NULL AS STRING) AS  _lifecyclestage,
  _accountid, 
  _contactid,
  _createTS AS _date,
  _t90_days_score,
FROM
  opps_created
UNION DISTINCT
SELECT
  DISTINCT _domain,
  CAST(NULL AS STRING) AS _email ,
  EXTRACT(WEEK FROM _oppLastChangeinStage) AS _week,
  EXTRACT(YEAR FROM _oppLastChangeinStage) AS _year,
  _opportunityname AS _contentTitle,
  "Opportunity Stage Change" AS _engagement,
  CONCAT('Amount: $', FORMAT("%'.2f", _amount), "\n", "Current Stage: ", _currentStage, "\n", "Previous Stage: ", _previousStage) AS _description,
  NULL AS _avg_bombora_score,
  CAST(NULL AS STRING) AS  _id,
  CAST(NULL AS STRING) AS  _name,
  CAST(NULL AS STRING) AS  _title,
  CAST(NULL AS STRING) AS  _2xseniority,
  CAST(NULL AS STRING) AS  _phone,
  _accountname,
  CAST(_annualrevenue AS STRING) AS _annualrevenue,
  _industry,
  _city,
  _state,
  _country,
  CAST(NULL AS STRING) AS _persona,
  CAST(NULL AS STRING) AS  _lifecyclestage,
  _accountid, 
  _contactid,
  _oppLastChangeinStage AS _date,
  _t90_days_score,
FROM
  opps_stage_change*/
;


CREATE OR REPLACE TABLE `logicsource.account_engagement_scoring` AS 
WITH account AS (
SELECT * EXCEPT (_order), "Target" AS _source 
FROM (
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _company DESC) AS _order
FROM (
SELECT *EXCEPT(_order) 
 FROM 
  (
   SELECT
      associated_company.properties.domain.value AS _domain,
     
      associated_company.properties.name.value AS _company,
      CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
      CASE WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
      WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
      WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
      WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
      WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
      WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet' ELSE associated_company.properties.industry.value
      END AS _industry,

      
      associated_company.properties.segment__c.value AS _company_segment,
     
      associated_company.properties.employee_range.value AS _employee_range, 
      associated_company.properties.employee_range_c.value AS _employee_range_c, 
      CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees, 
      CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue, 
      associated_company.properties.annual_revenue_range.value AS _annual_revenue_range, 
      associated_company.properties.annual_revenue_range_c.value AS _annual_revenue_range_c,
      ROW_NUMBER() OVER( PARTITION BY associated_company.properties.domain.value,associated_company.company_id ORDER BY properties.createdate.value DESC) AS _order
   FROM
      `x-marketing.logicsource_hubspot.contacts` k
      LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l ON LOWER(l.email) = LOWER(property_email.value)
  )
  WHERE
    _order = 1
     UNION ALL 
      SELECT DISTINCT _domain AS _domain, 
      CAST(NULL AS STRING) AS _company,
      -- CAST(NULL AS STRING) AS _lastname,
      CAST(NULL AS STRING) AS _revenue,
      CAST(NULL AS STRING) AS _industry,
            CAST(NULL AS STRING) AS  _company_segment,
      CAST(NULL AS STRING) AS _employee_range, 
       CAST(NULL AS STRING) AS _employee_range_c,
     CAST(NULL AS NUMERIC) AS  _numberofemployees, 
     CAST(NULL AS NUMERIC) AS _annualrevenue,
       CAST(NULL AS STRING) AS  _annual_revenue_range,  
        CAST(NULL AS STRING) AS  _annual_revenue_range_c,

 FROM `logicsource.dashboard_mouseflow_kickfire`
 WHERE 
 (_domain IS NOT NULL AND _domain != '')
  UNION ALL 
 SELECT DISTINCT  CASE WHEN _accountdomain = 'optum.com/' THEN 'optum.com' ELSE _accountdomain END   AS _domain, 
      CAST(NULL AS STRING) AS _company,
      -- CAST(NULL AS STRING) AS _lastname,
      CAST(NULL AS STRING) AS _revenue,
    _industry AS _industry, 
            CAST(NULL AS STRING) AS  _company_segment,
      CAST(NULL AS STRING) AS _employee_range, 
       CAST(NULL AS STRING) AS _employee_range_c,
     CAST(NULL AS NUMERIC) AS  _numberofemployees, 
     CAST(NULL AS NUMERIC) AS _annualrevenue,
       CAST(NULL AS STRING) AS  _annual_revenue_range,  
        CAST(NULL AS STRING) AS  _annual_revenue_range_c,
 FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement` 
 WHERE 
 (_accountdomain IS NOT NULL AND _accountdomain != '')
)
) WHERE _order = 1
), zoominfo AS (
   SELECT DISTINCT
    _domain, 
    _company,
    CAST(NULL AS STRING) AS _revenue,
    CAST(NULL AS STRING) AS _industry,
    CAST(NULL AS STRING) AS  _company_segment,
    CAST(NULL AS STRING) AS _employee_range, 
    CAST(NULL AS STRING) AS _employee_range_c,
    CAST(NULL AS NUMERIC) AS  _numberofemployees, 
    CAST(NULL AS NUMERIC) AS _annualrevenue,
    CAST(NULL AS STRING) AS  _annual_revenue_range,  
    CAST(NULL AS STRING) AS  _annual_revenue_range_c
    FROM `x-marketing.logicsource_mysql.db_zoominfo_intent`
), contacts AS ( 
SELECT CASE WHEN mainAcc._domain IS NULL THEN zoominfo._domain ELSE mainAcc._domain END AS _domain,
CASE WHEN mainAcc._domain IS NULL THEN zoominfo._company ELSE mainAcc._company END AS _company,
mainAcc.* EXCEPT (_source,_domain,_company),
 CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Bombora" END AS _source,
FROM account   mainAcc
LEFT JOIN  zoominfo  USING (_domain)
)
,
dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    -- _date,
    DATE_TRUNC(_date, WEEK(MONDAY)) AS _extract_date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 WEEK)) AS _date 
  ORDER BY 
    1 DESC
)
,email_engagements AS (
  SELECT * EXCEPT (_email_score), CASE WHEN _email_score >= 20 THEN 20 ELSE _email_score END AS _email_score  FROM (
    SELECT _domain,_emailOpentotal,_emailClickedtotal,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)) AS _emailopenscore_more,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 5 THEN  1 * 10 ELSE 0 END)) AS _emailopenscore,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END)) AS _emailclickscore_more,
  
  ((CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 5 THEN  1 * 10 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END) ) AS _email_score
    FROM
    (
      SELECT  
     _domain,
     SUM(_emailOpened) AS _emailOpentotal, 
     SUM(_emailClicked) AS _emailClickedtotal, 
     FROM (
      SELECT
      _domain,
      SUM(CASE WHEN _engagement = 'Email Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
      SUM( CASE WHEN _engagement = 'Email Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    ) a
    --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3
  ORDER BY _emailOpentotal DESC
  )
)
, email_last_engagementdate AS(
 SELECT email_engagements.*,
 CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_email,
 EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _email_week,
 EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _email_year
 FROM (
  SELECT * EXCEPT (rownum)
  FROM (
    SELECT 
    _domain,
    _email,
    _id,
    MAX(_date) OVER(PARTITION BY _domain)  AS _last_engagement_TS,
    ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _date DESC) AS rownum 
    FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN email_engagements ON  email_engagements._domain = _last_engagement._domain
)
,formfilled_engagements AS (
  SELECT *,CASE WHEN   _GatedContentscore + _distinctWebinarFormscore +  _distinctWebinarattendedscore >= 30 THEN 30 ELSE _GatedContentscore + _distinctWebinarFormscore +  _distinctWebinarattendedscore  END AS _GatedContentscore_total,
  CASE WHEN  _formFilled_score  >= 50 THEN 50 ELSE _formFilled_score END AS _formFilled_webinarscore_total,
  CASE WHEN  _GatedContentscore + _formFilled_score + _distinctWebinarFormscore +  _distinctWebinarattendedscore >= 80 THEN 80 ELSE _GatedContentscore + _formFilled_score + _distinctWebinarFormscore +  _distinctWebinarattendedscore END AS _form_fill_score_total 
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
  SELECT 
  _domain,
  
  _formFilled_total,
  _distinctGatedContenttotal, 
  _distinctWebinarFormtotal,
  _distinctWebinarattendedtotal,
    SUM(DISTINCT(CASE WHEN _formFilled_total >= 1 THEN 1 * 50 ELSE 0 END)) AS _formFilled_score,
    SUM(DISTINCT(CASE WHEN _distinctGatedContenttotal >= 1 THEN  1 * 20 ELSE 0 END)) AS _GatedContentscore,
    SUM(DISTINCT(CASE WHEN _distinctWebinarFormtotal >= 1 THEN  1 * 5 ELSE 0 END)) AS _distinctWebinarFormscore,
    SUM(DISTINCT(CASE WHEN _distinctWebinarattendedtotal >= 1 THEN  1 * 15 ELSE 0 END)) AS _distinctWebinarattendedscore,
    
    FROM
    (
      SELECT  
     _domain,
 
     SUM(_formFilled_contact_form)  AS _formFilled_total, 
     SUM(_distinctGatedContent) AS _distinctGatedContenttotal, 
     SUM(_distinctWebinarForm) AS _distinctWebinarFormtotal, 
     SUM(_distinctWebinarattended) AS _distinctWebinarattendedtotal,
     FROM (
      SELECT 
      _domain,
      SUM( CASE WHEN (_engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo'))  THEN 1 ELSE 0 END ) AS _formFilled_contact_form, 
     SUM( CASE WHEN (_engagement = 'Form Filled' AND  NOT  REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo|webinar')) OR  (_engagement = 'Form Filled' AND _contentTitle = "Other Content Engagement") OR (_engagement = 'Form Filled' AND _description = "Visited booth") THEN 1 ELSE 0 END) AS _distinctGatedContent,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Registered" THEN 1 ELSE 0 END) AS _distinctWebinarForm,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Attended event" THEN 1 ELSE 0 END)  AS _distinctWebinarattended,
     -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
     -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5
  ORDER BY _formFilled_total DESC
) 
)
, formfill_last_engagementdate AS(
  SELECT formfilled_engagements.*,
  CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_formfilled,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _formfilled_week,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _formfilled_year
  FROM (
    SELECT * EXCEPT (rownum)
    FROM (
      SELECT _domain,
      MAX(_date) OVER(PARTITION BY _domain)  AS _last_engagement_TS,
      ROW_NUMBER() OVER(PARTITION BY _domain  ORDER BY _date DESC) AS rownum 
      FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
      ) WHERE rownum = 1
      ) _last_engagement 
      RIGHT JOIN formfilled_engagements ON  formfilled_engagements._domain = _last_engagement._domain
) 
,paid_sosial_engagements AS (
  SELECT *,
  CASE WHEN  _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore >= 35 THEN 35 ELSE _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore  END AS _paid_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
SELECT _domain,_paidadssharetotal,_paidadscommenttotal,_paidadsfollowtotal,_paidadsvisittotal,_paidadsclick_liketotal,
    SUM(DISTINCT(CASE WHEN _paidadssharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _paidadssharescore,
    SUM(DISTINCT(CASE WHEN _paidadscommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _paidadscommentscore,
    SUM(DISTINCT(CASE WHEN _paidadsfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _paidadsfollowscore,
    SUM(DISTINCT(CASE WHEN _paidadsvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsvisitscore,
    SUM(DISTINCT(CASE WHEN _paidadsclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsclick_likescore,
    
    FROM
    (
      SELECT  
     _domain,
     SUM(_paidadsshare) AS _paidadssharetotal, 
     SUM(_paidadscomment) AS _paidadscommenttotal,
     SUM(_paidadsfollow) AS _paidadsfollowtotal,
     SUM(_paidadsvisit) AS _paidadsvisittotal,
     SUM(_paidadsclick_like) AS _paidadsclick_liketotal,
  FROM (
    SELECT  _domain,
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _paidadsshare,  
     SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _paidadscomment,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _paidadsfollow,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _paidadsvisit,  
    SUM( CASE WHEN  _engagement = 'Paid Ads'  AND SUBSTR(_description, 1,7) LIKE  '%Clicks%' OR SUBSTR(_description, 1,4) LIKE  '%Like%'  THEN 1 ELSE 0 END ) AS  _paidadsclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT *  FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Paid Ads') 
    )
  --WHERE 
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5,6
  ORDER BY _paidadssharescore  DESC
)
)
, paid_social_last_engagement AS(
 SELECT paid_sosial_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_paid_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _paid_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _paid_social_year
            FROM (
  SELECT * EXCEPT (rownum)
  FROM (
  SELECT _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END AS _id,
  MAX(_date) OVER(PARTITION BY _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END )  AS _last_engagement_TS,
  ROW_NUMBER() OVER(PARTITION BY 
    _domain,_email,_id  ORDER BY _date DESC) AS rownum 
   FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ("Paid Ads")
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN paid_sosial_engagements ON  paid_sosial_engagements._domain = _last_engagement ._domain
)
,organic_sosial_engagements AS (
  SELECT *,
  CASE WHEN  _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore >= 35 THEN 35 ELSE _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore  END AS _organic_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
SELECT _domain,_organicsharetotal,_organiccommenttotal,_organicfollowtotal,_organicvisittotal,_organicclick_liketotal,
    SUM(DISTINCT(CASE WHEN _organicsharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _organischarescore,
    SUM(DISTINCT(CASE WHEN _organiccommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _organiccommentscore,
    SUM(DISTINCT(CASE WHEN _organicfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _organicfollowscore,
    SUM(DISTINCT(CASE WHEN _organicvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicvisitscore,
    SUM(DISTINCT(CASE WHEN _organicclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicclick_likescore,
    
    FROM
    (
      SELECT  
     _domain,
     SUM(_organicsshare) AS _organicsharetotal, 
    SUM(_organiccomment) AS _organiccommenttotal,
    SUM(_organicfollow) AS _organicfollowtotal,
    SUM(_organicvisit) AS _organicvisittotal,
    SUM(_organicclick_like) AS _organicclick_liketotal,


  FROM (
    SELECT  _domain,
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _organicsshare,  
     SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _organiccomment,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _organicfollow,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _organicvisit,  
    SUM( CASE WHEN  _engagement = 'Organic Social'  AND SUBSTR(_description, 1,7) LIKE  '%Clicks%' OR SUBSTR(_description, 1,4) LIKE  '%Like%' THEN 1 ELSE 0 END ) AS  _organicclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Organic Social')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5,6
  ORDER BY _organicsharetotal  DESC
)
)
, organic_social_last_engagement AS(
 SELECT organic_sosial_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_organc_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _organc_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _organc_social_year
            FROM (
  SELECT * EXCEPT (rownum)
  FROM (
  SELECT _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END AS _id,
  MAX(_date) OVER(PARTITION BY _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END )  AS _last_engagement_TS,
  ROW_NUMBER() OVER(PARTITION BY 
    _domain,_email,_id  ORDER BY _date DESC) AS rownum 
   FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ("Paid Ads")
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN organic_sosial_engagements ON  organic_sosial_engagements._domain = _last_engagement ._domain
),weekly_web_data  AS (
  SELECT
        _domain,
        -- _week,
        -- _year,
        -- COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
        COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
        COALESCE(SUM(CASE WHEN _pageName IS NOT NULL THEN 1 END), 0) AS _website_page_view,
        COALESCE(COUNT(DISTINCT _visitorid), 0) AS _website_visitor_count,
        COALESCE(COUNT(DISTINCT CASE WHEN _pageName LIKE "%careers%" THEN _visitorid END), 0) AS _career_page,
        TRUE AS _visited_website,
        -- MAX(_timestamp) AS last_engaged_date
      FROM (
        /* SELECT
          DATE(_starttime) AS _timestamp,
          company._domain,
          SUM(CAST(_engagementtime AS INT64)) AS _website_time_spent,
          COUNT(DISTINCT(_page)) AS _website_page_view,
          COUNT(DISTINCT msflow._visitorid) AS _website_visitor_count,
          -- newsletter_subscription in the future,
        FROM
          `logicsource_mysql.mouseflow_pageviews` msflow
        LEFT JOIN (
          SELECT
            DISTINCT _ipaddr,
            _website AS _domain
          FROM
            `webtrack_ipcompany.webtrack_ipcompany_6sense`) company
          USING
            (_ipaddr)
        GROUP BY
          1, 2  */
          SELECT 
            _domain, 
            _visitorid,
            DATETIME(_timestamp) AS _timestamp, 
            EXTRACT(WEEK FROM _timestamp) AS _week,  
            EXTRACT(YEAR FROM _timestamp) AS _year, 
            _entrypage AS _pageName, 
            -- "Web Visit" AS _engagement, 
            CAST(_engagementtime AS INT64) AS _website_time_spent,
            _totalPages AS _website_page_view
          FROM 
            `logicsource.dashboard_mouseflow_kickfire` web 
          --WHERE 
            --NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email') 
            --AND _webactivity IS NOT NULL
          ORDER BY
            _timestamp DESC
          )
        WHERE
          --(_timestamp BETWEEN date_start AND date_end)
        --AND  
          LENGTH(_domain) > 2
        GROUP BY
          1 
     )
     -- Get scores for web visits activity
   , weekly_web_score AS (
        SELECT
          * EXCEPT(website_time_spent_score,
            website_page_view_score,
            website_visitor_count_score,
            visited_website_score),
            website_time_spent_score AS _website_time_spent_score,
            website_page_view_score AS _website_page_view_score,
            website_visitor_count_score AS _website_visitor_count_score,
            visited_website_score AS _visited_website_score,
            CASE
              WHEN (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + career_page_score) > 40 THEN 40
              ELSE (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + career_page_score)
            END AS _web_score_total
        FROM (
          SELECT
            *,
            COALESCE((_website_time_spent), 0)
              AS website_time_spent_score,
           ( CASE 
              WHEN _website_page_view >= 5 THEN 15
              -- WHEN _website_page_view < 5 THEN 10
              ELSE 0
            END
            +
            CASE 
              WHEN _website_page_view <= 5 THEN 10
              -- WHEN _website_page_view < 5 THEN 10
              ELSE 0
            END
            )
              AS website_page_view_score,
            (CASE WHEN _website_visitor_count >= 3 THEN 10
              -- WHEN _website_visitor_count < 3 THEN 5
              ELSE 0
            END
            + 
            CASE WHEN _website_visitor_count < 3 THEN 5
              -- WHEN _website_visitor_count < 3 THEN 5
              ELSE 0
            END)
              AS website_visitor_count_score,
              CASE WHEN _career_page > 1 THEN -5 ELSE 0 END AS career_page_score,
            5 AS visited_website_score
          FROM
            weekly_web_data ) 
  ), web_last_engagement AS (           
  SELECT 
 web_data._domain,web_data.* EXCEPT (_domain),
  _last_engagement.last_engaged_date AS _last_engagement_web,
  EXTRACT(WEEK FROM _last_engagement.last_engaged_date) AS week_web,
  EXTRACT(YEAR FROM _last_engagement.last_engaged_date) AS year_web
  FROM (
    SELECT 
        /* _website, */  _domain,
        MAX(_timestamp) AS last_engaged_date
        FROM (
         SELECT 
                   _domain AS _domain, 
                    _visitorid,
                    DATETIME(_timestamp) AS _timestamp, 
                    _engagementtime AS _website_time_spent,
                    _totalPages AS _website_page_view
                FROM `logicsource.dashboard_mouseflow_kickfire`
                --WHERE 
                --NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email') 
               -- AND _webactivity IS NOT NULL
                --AND (_domain IS NOT NULL AND _domain != '')
                ORDER BY _timestamp DESC
                )
    -- WHERE REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') = 'opcw.org'
    GROUP BY 1
    
    
    ) _last_engagement
    RIGHT JOIN weekly_web_score web_data ON   web_data._domain = _last_engagement._domain 
), combine_all AS ( #combine all channel data and calculate into the max data. 
   SELECT *,(COALESCE(_GatedContentscore_total ,0) + COALESCE(_formFilled_webinarscore_total,0)  + COALESCE(_email_score,0) 
  + COALESCE(_paid_ads_score_total,0) + COALESCE(_organic_ads_score_total,0) +  COALESCE(_web_score_total,0)
   ) AS _total_score,
   CASE 
   WHEN (
     _last_engagement_email_date >= formfilled_last_engaged_date
     AND
  
     _last_engagement_email_date >= organic_social_last_engagement
     AND 
     _last_engagement_email_date >= paid_social_engaged_date
     AND 
     _last_engagement_email_date >=  engagement_web_date
     ) THEN _last_engagement_email_date
     WHEN (
      formfilled_last_engaged_date >= _last_engagement_email_date
      AND 
      formfilled_last_engaged_date >= organic_social_last_engagement
      AND 
      formfilled_last_engaged_date >= paid_social_engaged_date
       AND 
      formfilled_last_engaged_date >= engagement_web_date
    
    ) THEN formfilled_last_engaged_date
    WHEN (
     paid_social_engaged_date >= _last_engagement_email_date
      AND 
      paid_social_engaged_date >= formfilled_last_engaged_date
      AND 
      paid_social_engaged_date >= organic_social_last_engagement
       AND 
      paid_social_engaged_date >= engagement_web_date
    
    ) THEN paid_social_engaged_date
     WHEN (
     organic_social_last_engagement >= _last_engagement_email_date
      AND 
      organic_social_last_engagement>= formfilled_last_engaged_date
      AND 
       organic_social_last_engagement >= paid_social_engaged_date
         AND 
       organic_social_last_engagement >= engagement_web_date
    
    ) THEN organic_social_last_engagement
         WHEN (
      engagement_web_date >= _last_engagement_email_date
      AND 
       engagement_web_date >= formfilled_last_engaged_date
      AND 
        engagement_web_date >= paid_social_engaged_date
         AND 
        engagement_web_date >= organic_social_last_engagement
    
    ) THEN  engagement_web_date
  
    END AS _last_engagement_date
    FROM (
      SELECT main.*,
      email_engagement.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS  _last_engagement_email_date,
      --paid_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
     -- organic_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
      formfill.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_formfilled AS DATE), DATE('2000-01-01')) AS  formfilled_last_engaged_date,
     
      cs.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organc_social AS DATE), DATE('2000-01-01')) AS  organic_social_last_engagement,
      search_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  paid_social_engaged_date,
      web_data.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_web AS DATE), DATE('2000-01-01')) AS  engagement_web_date,
      FROM contacts AS main
    LEFT JOIN email_last_engagementdate  AS email_engagement ON (main._domain = email_engagement._domain )
    LEFT JOIN formfill_last_engagementdate AS formfill ON (main._domain = formfill._domain)
    LEFT JOIN organic_social_last_engagement AS cs ON (main._domain = cs._domain)
    LEFT JOIN paid_social_last_engagement AS search_ads ON (main._domain = search_ads._domain) 
    LEFT JOIN web_last_engagement AS web_data ON (main._domain = web_data._domain)

)
), icp_score AS (
   SELECT 
 _domain AS _domain, 
 total_employee, total_score_divide_2, total_score, max_score
  FROM `x-marketing.logicsource.account_icp_score`
),all_data AS (
SELECT *,  EXTRACT(YEAR FROM _last_engagement_date ) AS _last_engagemtn_year,
  EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,          
  DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
  CASE 
  WHEN DATE_DIFF(CURRENT_DATE(),_last_engagement_date, DAY) > 180  THEN (_total_score - 50)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90  THEN (_total_score - 25)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 20)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30  THEN (_total_score - 15)
  ELSE _total_score
  END AS _score_new
FROM combine_all
--WHERE _domain =  'pepsico.com'
ORDER BY _last_engagement_date DESC,_total_score DESC
) 
SELECT all_data.*,
icp_score.* EXCEPT(_domain),
COALESCE(_total_score,0)   + COALESCE(max_score,0)  AS _total_score_icp_intent,
CASE WHEN COALESCE(_total_score,0)   + COALESCE(max_score,0)  >= 10 AND COALESCE(_total_score,0)   + COALESCE(max_score,0)  <= 20 THEN 'Low'
WHEN  COALESCE(_total_score,0)   + COALESCE(max_score,0)  >= 21 AND COALESCE(_total_score,0)   + COALESCE(max_score,0)  <= 49 THEN 'Medium'
WHEN COALESCE(_total_score,0)   + COALESCE(max_score,0)  >= 50  THEN 'High' ELSE "Low" END AS legend
FROM all_data
LEFT JOIN icp_score on all_data._domain = icp_score._domain  
--WHERE all_data._domain = "hcahealthcare.com"
 ;

 
CREATE OR REPLACE TABLE `logicsource.contact_engagement_scoring` AS 
WITH contacts AS (
SELECT * EXCEPT (rownum) FROM (
    SELECT
      CAST(vid AS STRING) AS _id,
      property_email.value AS _email,
      COALESCE(CONCAT(property_firstname.value, ' ', property_lastname.value),property_firstname.value) AS _name,
      COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain, 
      properties.jobtitle.value AS jobtitle,
      properties.job_function.value AS _function,
      CASE WHEN property_job_role__organic_.value IS NOT NULL THEN property_job_role__organic_.value ELSE property_job_role.value END AS _jobrole,
      properties.hs_lifecyclestage_marketingqualifiedlead_date.value AS _mqldate,
      properties.hs_analytics_source.value AS _source,
      properties.hs_latest_source.value AS _latest_source,
      CASE WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value ELSE property_management_level.value END AS _seniority,
      property_phone.value AS _phone,
      associated_company.properties.name.value AS _company,
      CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
      CASE WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
      WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
      WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
      WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
      WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
      WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet' ELSE associated_company.properties.industry.value
      END AS _industry,
      property_city.value AS _city,
      property_state.value AS _state,
      property_country.value AS _country,
      '' AS _persona,
      property_lifecyclestage.value AS _lifecycleStage,
      CAST(l.lead_score__c AS INT64) AS leadscore,
      properties.hs_lead_status.value AS _leadstatus,
      properties.ipqc_check.value AS _ipqc_check,
      property_hubspotscore.value AS _hubspotscore,
      associated_company.company_id,
      associated_company.properties.segment__c.value AS _company_segment,
      property_lead_segment.value AS _lead_segment, 
      property_segment__c.value AS _segment, 
      property_leadstatus.value AS _property_leadstatus, 
      associated_company.properties.linkedinbio.value AS _companylinkedinbio, 
      associated_company.properties.linkedin_company_page.value AS _company_linkedin, 
      associated_company.properties.employee_range.value AS _employee_range, 
      associated_company.properties.employee_range_c.value AS _employee_range_c, 
      CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees, 
      CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue, 
      associated_company.properties.annual_revenue_range.value AS _annual_revenue_range, 
      associated_company.properties.annual_revenue_range_c.value AS _annual_revenue_range_c,
      associated_company.properties.salesforceaccountid.value AS salesforceaccountid, 
      properties.salesforceleadid.value AS salesforceleadid,
      properties.salesforcecontactid.value AS salesforcecontactid,
      ROW_NUMBER() OVER( PARTITION BY property_email.value,CAST(vid AS STRING) ORDER BY properties.createdate.value DESC) AS rownum
    FROM
      `x-marketing.logicsource_hubspot.contacts` k
      LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l ON LOWER(l.email) = LOWER(property_email.value)
    WHERE
     property_email.value IS NOT NULL
      AND property_email.value NOT LIKE '%2x.marketing%'
      AND property_email.value NOT LIKE '%logicsource%' 
)
WHERE rownum = 1
)
,
dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    -- _date,
    DATE_TRUNC(_date, WEEK(MONDAY)) AS _extract_date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 WEEK)) AS _date 
  ORDER BY 
    1 DESC
)
,email_engagements AS (
  SELECT * EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score  FROM (
    SELECT _domain,_email,_id,_emailOpentotal,_emailClickedtotal,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)) AS _emailopenscore_more,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 5 THEN  1 * 5 ELSE 0 END)) AS _emailopenscore,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END)) AS _emailclickscore_more,
  
  ((CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)+(CASE WHEN _emailOpentotal >= 5 THEN  1 * 5 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END) ) AS _email_score
    FROM
    (
      SELECT  
     _domain,
     _email,
     _id,
     SUM(_emailOpened) AS _emailOpentotal, 
     SUM(_emailClicked) AS _emailClickedtotal, 
     FROM (
      SELECT
      _domain,
      _email,
      _id,
      SUM(CASE WHEN _engagement = 'Email Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
      SUM( CASE WHEN _engagement = 'Email Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
    ) a
    --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5
  ORDER BY _emailOpentotal DESC
  )
)
, email_last_engagementdate AS(
 SELECT email_engagements.*,
 CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_email,
 EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _email_week,
 EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _email_year
 FROM (
  SELECT * EXCEPT (rownum)
  FROM (
    SELECT 
    _domain,
    _email,
    _id,
    MAX(_date) OVER(PARTITION BY _domain,_email,_id)  AS _last_engagement_TS,
    ROW_NUMBER() OVER(PARTITION BY _domain,_email,_id  ORDER BY _date DESC) AS rownum 
    FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN email_engagements ON  email_engagements._id = _last_engagement._id
)
,formfilled_engagements AS (
  SELECT *,CASE WHEN  _GatedContentscore  >= 20 THEN 20 ELSE _GatedContentscore END AS _GatedContentscore_total,
  CASE WHEN  _formFilled_webinarscore  >= 50 THEN 50 ELSE _formFilled_webinarscore END AS _formFilled_webinarscore_total,
  CASE WHEN  _GatedContentscore + _formFilled_webinarscore >= 80 THEN 80 ELSE _GatedContentscore + _formFilled_webinarscore END AS _form_fill_score_total 
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
  SELECT 
  _domain,
  _email,_id,
  _formFilled_webinartotal,
  _distinctGatedContenttotal, 
    SUM(DISTINCT(CASE WHEN _formFilled_webinartotal >= 1 THEN 1 * 50 ELSE 0 END)) AS _formFilled_webinarscore,
    SUM(DISTINCT(CASE WHEN _distinctGatedContenttotal >= 1 THEN  1 * 20 ELSE 0 END)) AS _GatedContentscore,
    
    FROM
    (
      SELECT  
     _domain,
     _email,
     _id,
     SUM(_formFilled_webinar) AS _formFilled_webinartotal, 
     SUM(_distinctGatedContent) AS _distinctGatedContenttotal, 
     FROM (
      SELECT 
      _domain,
      _email,
      _id,
       SUM( CASE WHEN (_engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo|webinar'))  THEN 1 ELSE 0 END ) AS _formFilled_webinar,  
     SUM( CASE WHEN (_engagement = 'Form Filled' AND  NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn')) OR (_engagement = 'Form Filled' AND _contentTitle = "Other Content Engagement") OR (_engagement = 'Form Filled' AND _description = "Visited booth")THEN 1 ELSE 0 END) AS _distinctGatedContent,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Registered" THEN 1 ELSE 0 END) AS _distinctWebinarForm,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Attended event" THEN 1 ELSE 0 END)  AS _distinctWebinarattended,
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5
  ORDER BY _formFilled_webinartotal DESC
) 
)
, formfill_last_engagementdate AS(
  SELECT formfilled_engagements.*,
  CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_formfilled,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _formfilled_week,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _formfilled_year
  FROM (
    SELECT * EXCEPT (rownum)
    FROM (
      SELECT _domain,_email,_id,
      MAX(_date) OVER(PARTITION BY _domain,_email,_id)  AS _last_engagement_TS,
      ROW_NUMBER() OVER(PARTITION BY _domain,_email,_id  ORDER BY _date DESC) AS rownum 
      FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
      ) WHERE rownum = 1
      ) _last_engagement 
      RIGHT JOIN formfilled_engagements ON  formfilled_engagements._id = _last_engagement._id
) 
,paid_sosial_engagements AS (
  SELECT *,
  CASE WHEN  _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore >= 35 THEN 35 ELSE _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore  END AS _paid_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
SELECT _domain,_email,_id,_paidadssharetotal,_paidadscommenttotal,_paidadsfollowtotal,_paidadsvisittotal,_paidadsclick_liketotal,
    SUM(DISTINCT(CASE WHEN _paidadssharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _paidadssharescore,
    SUM(DISTINCT(CASE WHEN _paidadscommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _paidadscommentscore,
    SUM(DISTINCT(CASE WHEN _paidadsfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _paidadsfollowscore,
    SUM(DISTINCT(CASE WHEN _paidadsvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsvisitscore,
    SUM(DISTINCT(CASE WHEN _paidadsclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsclick_likescore,
    
    FROM
    (
      SELECT  
     _domain,_email,_id,
     SUM(_paidadsshare) AS _paidadssharetotal, 
    SUM(_paidadscomment) AS _paidadscommenttotal,
    SUM(_paidadsfollow) AS _paidadsfollowtotal,
    SUM(_paidadsvisit) AS _paidadsvisittotal,
    SUM(_paidadsclick_like) AS _paidadsclick_liketotal,


  FROM (
    SELECT  _domain,_email,_id,
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _paidadsshare,  
     SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _paidadscomment,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _paidadsfollow,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _paidadsvisit,  
    SUM( CASE WHEN  _engagement = 'Paid Ads'  AND REGEXP_CONTAINS(LOWER(_contentTitle), 'click|like') THEN 1 ELSE 0 END ) AS  _paidadsclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Paid Ads')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER BY _paidadssharescore  DESC
)
)
, paid_social_last_engagement AS(
 SELECT paid_sosial_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_paid_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _paid_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _paid_social_year
            FROM (
  SELECT * EXCEPT (rownum)
  FROM (
  SELECT _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END AS _id,
  MAX(_date) OVER(PARTITION BY _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END )  AS _last_engagement_TS,
  ROW_NUMBER() OVER(PARTITION BY 
    _domain,_email,_id  ORDER BY _date DESC) AS rownum 
   FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ("Paid Ads")
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN paid_sosial_engagements ON  paid_sosial_engagements._id = _last_engagement ._id
)
,organic_sosial_engagements AS (
  SELECT *,
  CASE WHEN  _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore >= 35 THEN 35 ELSE _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore  END AS _organic_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
SELECT _domain,_email,_id,_organicsharetotal,_organiccommenttotal,_organicfollowtotal,_organicvisittotal,_organicclick_liketotal,
    SUM(DISTINCT(CASE WHEN _organicsharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _organischarescore,
    SUM(DISTINCT(CASE WHEN _organiccommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _organiccommentscore,
    SUM(DISTINCT(CASE WHEN _organicfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _organicfollowscore,
    SUM(DISTINCT(CASE WHEN _organicvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicvisitscore,
    SUM(DISTINCT(CASE WHEN _organicclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicclick_likescore,
    
    FROM
    (
      SELECT  
     _domain,_email,_id,
     SUM(_organicsshare) AS _organicsharetotal, 
    SUM(_organiccomment) AS _organiccommenttotal,
    SUM(_organicfollow) AS _organicfollowtotal,
    SUM(_organicvisit) AS _organicvisittotal,
    SUM(_organicclick_like) AS _organicclick_liketotal,


  FROM (
    SELECT  _domain,_email,_id,
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _organicsshare,  
     SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _organiccomment,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _organicfollow,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _organicvisit,  
    SUM( CASE WHEN  _engagement = 'Organic Social'  AND REGEXP_CONTAINS(LOWER(_contentTitle), 'click|like') THEN 1 ELSE 0 END ) AS  _organicclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Organic Social')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER BY _organicsharetotal  DESC
)
)
, organic_social_last_engagement AS(
 SELECT organic_sosial_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_organc_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _organc_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _organc_social_year
            FROM (
  SELECT * EXCEPT (rownum)
  FROM (
  SELECT _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END AS _id,
  MAX(_date) OVER(PARTITION BY _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END )  AS _last_engagement_TS,
  ROW_NUMBER() OVER(PARTITION BY 
    _domain,_email,_id  ORDER BY _date DESC) AS rownum 
   FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ("Paid Ads")
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN organic_sosial_engagements ON  organic_sosial_engagements._id = _last_engagement ._id
), combine_all AS ( #combine all channel data and calculate into the max data. 
   SELECT *,(COALESCE(_GatedContentscore_total ,0) + COALESCE(_formFilled_webinarscore_total,0)  + COALESCE(_email_score,0) 
  + COALESCE(_paid_ads_score_total,0) + COALESCE(_organic_ads_score_total,0) 
   ) AS _total_score,
   CASE 
   WHEN (
     _last_engagement_email_date >= formfilled_last_engaged_date
     AND
  
     _last_engagement_email_date >= organic_social_last_engagement
     AND 
     _last_engagement_email_date >= paid_social_engaged_date
     ) THEN _last_engagement_email_date
     WHEN (
      formfilled_last_engaged_date >= _last_engagement_email_date
      AND 
      formfilled_last_engaged_date >= organic_social_last_engagement
      AND 
      formfilled_last_engaged_date >= paid_social_engaged_date
    
    ) THEN formfilled_last_engaged_date
    WHEN (
     paid_social_engaged_date >= _last_engagement_email_date
      AND 
      paid_social_engaged_date >= formfilled_last_engaged_date
      AND 
      paid_social_engaged_date >= organic_social_last_engagement
    
    ) THEN paid_social_engaged_date
     WHEN (
     organic_social_last_engagement >= _last_engagement_email_date
      AND 
      organic_social_last_engagement>= formfilled_last_engaged_date
      AND 
       organic_social_last_engagement >= paid_social_engaged_date
    
    ) THEN organic_social_last_engagement
  
    END AS _last_engagement_date
    FROM (
      SELECT main.*,
      email_engagement.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS  _last_engagement_email_date,
      --paid_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
     -- organic_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
      formfill.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_formfilled AS DATE), DATE('2000-01-01')) AS  formfilled_last_engaged_date,
     
      cs.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_organc_social AS DATE), DATE('2000-01-01')) AS  organic_social_last_engagement,
      search_ads.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  paid_social_engaged_date,
      FROM contacts AS main
    LEFT JOIN email_last_engagementdate  AS email_engagement ON (main._id = email_engagement._id )
    LEFT JOIN formfill_last_engagementdate AS formfill ON (main._id = formfill._id)
    LEFT JOIN organic_social_last_engagement AS cs ON (main._id = cs._id)
    LEFT JOIN paid_social_last_engagement AS search_ads ON (main._id = search_ads._id) 

)
), icp_score AS (
  SELECT 
  _prospectid AS _id, 
  hubspot_score, 
  total_score_ICP 
  FROM `x-marketing.logicsource.contact_icp_score` 
),all_data AS (
SELECT *,  EXTRACT(YEAR FROM _last_engagement_date ) AS _last_engagemtn_year,
  EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,          
  DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
  CASE 
  WHEN DATE_DIFF(CURRENT_DATE(),_last_engagement_date, DAY) > 180  THEN (_total_score - 50)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90  THEN (_total_score - 25)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 20)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30  THEN (_total_score - 15)
  ELSE _total_score
  END AS _score_new
FROM combine_all
--WHERE _domain =  'pepsico.com'
ORDER BY _last_engagement_date DESC,_total_score DESC
) 
SELECT all_data.*,
icp_score.* EXCEPT(_id),
COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)   AS _total_score_icp_intent,
CASE WHEN COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  >= 10 AND COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  <= 20 THEN 'Low'
WHEN  COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  >= 21 AND COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  <= 49 THEN 'Medium'
WHEN COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  >= 50  THEN 'High' ELSE "Low" END AS legend
FROM all_data
LEFT JOIN icp_score on all_data._id = icp_score._id ; 

CREATE OR REPLACE TABLE `logicsource.zoominfo_account_engagement_scoring` AS 
WITH account AS (
SELECT * EXCEPT (_order), "Target" AS _source,
"Hubspot" AS source_zi_intent,
FROM (
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _company DESC) AS _order
FROM (
SELECT *EXCEPT(_order) 
 FROM 
  (
   SELECT
      associated_company.properties.domain.value AS _domain,
     
      associated_company.properties.name.value AS _company,
      CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
      CASE WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
      WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
      WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
      WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
      WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
      WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet' ELSE associated_company.properties.industry.value
      END AS _industry,

      
      associated_company.properties.segment__c.value AS _company_segment,
     
      associated_company.properties.employee_range.value AS _employee_range, 
      associated_company.properties.employee_range_c.value AS _employee_range_c, 
      CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees, 
      CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue, 
      associated_company.properties.annual_revenue_range.value AS _annual_revenue_range, 
      associated_company.properties.annual_revenue_range_c.value AS _annual_revenue_range_c,
      ROW_NUMBER() OVER( PARTITION BY associated_company.properties.domain.value,associated_company.company_id ORDER BY properties.createdate.value DESC) AS _order
   FROM
      `x-marketing.logicsource_hubspot.contacts` k
      LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l ON LOWER(l.email) = LOWER(property_email.value)
  )
  WHERE
    _order = 1
     UNION ALL 
      SELECT DISTINCT _domain AS _domain, 
      CAST(NULL AS STRING) AS _company,
      -- CAST(NULL AS STRING) AS _lastname,
      CAST(NULL AS STRING) AS _revenue,
      CAST(NULL AS STRING) AS _industry,
            CAST(NULL AS STRING) AS  _company_segment,
      CAST(NULL AS STRING) AS _employee_range, 
       CAST(NULL AS STRING) AS _employee_range_c,
     CAST(NULL AS NUMERIC) AS  _numberofemployees, 
     CAST(NULL AS NUMERIC) AS _annualrevenue,
       CAST(NULL AS STRING) AS  _annual_revenue_range,  
        CAST(NULL AS STRING) AS  _annual_revenue_range_c,

 FROM `logicsource.dashboard_mouseflow_kickfire`
 WHERE 
 (_domain IS NOT NULL AND _domain != '')
  UNION ALL 
 SELECT DISTINCT _accountdomain  AS _domain, 
      CAST(NULL AS STRING) AS _company,
      -- CAST(NULL AS STRING) AS _lastname,
      CAST(NULL AS STRING) AS _revenue,
    _industry AS _industry, 
            CAST(NULL AS STRING) AS  _company_segment,
      CAST(NULL AS STRING) AS _employee_range, 
       CAST(NULL AS STRING) AS _employee_range_c,
     CAST(NULL AS NUMERIC) AS  _numberofemployees, 
     CAST(NULL AS NUMERIC) AS _annualrevenue,
       CAST(NULL AS STRING) AS  _annual_revenue_range,  
        CAST(NULL AS STRING) AS  _annual_revenue_range_c,
 FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement` 
 WHERE 
 (_accountdomain IS NOT NULL AND _accountdomain != '')
)
) WHERE _order = 1
), zoominfo AS (
   SELECT DISTINCT
    _domain, 
    _company,
    CAST(NULL AS STRING) AS _revenue,
    _companyindustry AS _industry,
    CAST(NULL AS STRING) AS  _company_segment,
    CAST(NULL AS STRING) AS _employee_range, 
    CAST(NULL AS STRING) AS _employee_range_c,
    CAST(NULL AS NUMERIC) AS  _numberofemployees, 
    CAST(NULL AS NUMERIC) AS _annualrevenue,
    CAST(NULL AS STRING) AS  _annual_revenue_range,  
    CAST(NULL AS STRING) AS  _annual_revenue_range_c,
    "Zoominfo" AS _zi_intent
    FROM `x-marketing.logicsource_mysql.db_zoominfo_intent`
)
, zoominfo_domain AS ( 
  SELECT * EXCEPT(_order)
  FROM (
SELECT CASE WHEN mainAcc._domain IS NULL THEN zoominfo._domain ELSE mainAcc._domain END AS _domain,
CASE WHEN mainAcc._domain IS NULL THEN zoominfo._company ELSE mainAcc._company END AS _company,
CASE WHEN mainAcc._industry IS NULL THEN zoominfo._industry ELSE mainAcc._industry END AS _industry,
mainAcc.* EXCEPT (_source,_domain,_company,_industry),
 CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Net New" END AS _source,
CASE WHEN zoominfo._domain IS NOT NULL THEN _zi_intent ELSE source_zi_intent END AS _zi_intent,
ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY zoominfo._company DESC) AS _order
FROM  zoominfo  
LEFT JOIN  account mainAcc  USING (_domain)
  ) WHERE _order = 1
), hubspot_domain AS ( 
   SELECT CASE WHEN mainAcc._domain IS NULL THEN zoominfo_domain._domain ELSE mainAcc._domain END AS _domain,
CASE WHEN mainAcc._domain IS NULL THEN zoominfo_domain._company ELSE mainAcc._company END AS _company,
CASE WHEN mainAcc._industry IS NULL THEN zoominfo_domain._industry ELSE mainAcc._industry  END AS _industry,
mainAcc.* EXCEPT (_source,_domain,_company,_industry),
 CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Net New" END AS _source,
CASE WHEN zoominfo_domain._domain IS NOT NULL THEN _zi_intent ELSE mainAcc.source_zi_intent END AS _zi_intent,
FROM account mainAcc
LEFT JOIN  zoominfo_domain USING (_domain)
WHERE zoominfo_domain._domain IS NULL
--WHERE _domain NOT IN (SELECT DISTINCT _domain FROM zoominfo_domain)
), contacts AS (
SELECT * FROM zoominfo_domain 
UNION ALL
SELECT * FROM hubspot_domain 
),  dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    -- _date,
    DATE_TRUNC(_date, WEEK(MONDAY)) AS _extract_date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 WEEK)) AS _date 
  ORDER BY 
    1 DESC
)
,email_engagements AS (
  SELECT * EXCEPT (_email_score), CASE WHEN _email_score >= 20 THEN 20 ELSE _email_score END AS _email_score  FROM (
    SELECT _domain,_emailOpentotal,_emailClickedtotal,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)) AS _emailopenscore_more,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 5 THEN  1 * 10 ELSE 0 END)) AS _emailopenscore,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END)) AS _emailclickscore_more,
  
  ((CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 5 THEN  1 * 10 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END) ) AS _email_score
    FROM
    (
      SELECT  
     _domain,
     SUM(_emailOpened) AS _emailOpentotal, 
     SUM(_emailClicked) AS _emailClickedtotal, 
     FROM (
      SELECT
      _domain,
      SUM(CASE WHEN _engagement = 'Email Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
      SUM( CASE WHEN _engagement = 'Email Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    ) a
    --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3
  ORDER BY _emailOpentotal DESC
  )
)
, email_last_engagementdate AS(
 SELECT email_engagements.*,
 CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_email,
 EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _email_week,
 EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _email_year
 FROM (
  SELECT * EXCEPT (rownum)
  FROM (
    SELECT 
    _domain,
    _email,
    _id,
    MAX(_date) OVER(PARTITION BY _domain)  AS _last_engagement_TS,
    ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _date DESC) AS rownum 
    FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN email_engagements ON  email_engagements._domain = _last_engagement._domain
)
,formfilled_engagements AS (
  SELECT *,CASE WHEN  _GatedContentscore  >= 20 THEN 20 ELSE _GatedContentscore END AS _GatedContentscore_total,
  CASE WHEN  _formFilled_webinarscore  >= 50 THEN 50 ELSE _formFilled_webinarscore END AS _formFilled_webinarscore_total,
  CASE WHEN  _GatedContentscore + _formFilled_webinarscore >= 80 THEN 80 ELSE _GatedContentscore + _formFilled_webinarscore END AS _form_fill_score_total 
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
  SELECT 
  _domain,
  
  _formFilled_webinartotal,
  _distinctGatedContenttotal, 
    SUM(DISTINCT(CASE WHEN _formFilled_webinartotal >= 1 THEN 1 * 50 ELSE 0 END)) AS _formFilled_webinarscore,
    SUM(DISTINCT(CASE WHEN _distinctGatedContenttotal >= 1 THEN  1 * 20 ELSE 0 END)) AS _GatedContentscore,
    
    FROM
    (
      SELECT  
     _domain,
 
     SUM(_formFilled_webinar) AS _formFilled_webinartotal, 
     SUM(_distinctGatedContent) AS _distinctGatedContenttotal, 
     FROM (
      SELECT 
      _domain,
      SUM( CASE WHEN (_engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo|webinar'))  THEN 1 ELSE 0 END ) AS _formFilled_webinar,  
     SUM( CASE WHEN (_engagement = 'Form Filled' AND  NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn')) OR (_engagement = 'Form Filled' AND _contentTitle = "Other Content Engagement") OR (_engagement = 'Form Filled' AND _description = "Visited booth")THEN 1 ELSE 0 END) AS _distinctGatedContent,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Registered" THEN 1 ELSE 0 END) AS _distinctWebinarForm,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Attended event" THEN 1 ELSE 0 END)  AS _distinctWebinarattended,
     -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
     -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3
  ORDER BY _formFilled_webinartotal DESC
) 
)
, formfill_last_engagementdate AS(
  SELECT formfilled_engagements.*,
  CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_formfilled,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _formfilled_week,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _formfilled_year
  FROM (
    SELECT * EXCEPT (rownum)
    FROM (
      SELECT _domain,
      MAX(_date) OVER(PARTITION BY _domain)  AS _last_engagement_TS,
      ROW_NUMBER() OVER(PARTITION BY _domain  ORDER BY _date DESC) AS rownum 
      FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
      ) WHERE rownum = 1
      ) _last_engagement 
      RIGHT JOIN formfilled_engagements ON  formfilled_engagements._domain = _last_engagement._domain
) 
,paid_sosial_engagements AS (
  SELECT *,
  CASE WHEN  _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore >= 35 THEN 35 ELSE _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore  END AS _paid_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
SELECT _domain,_paidadssharetotal,_paidadscommenttotal,_paidadsfollowtotal,_paidadsvisittotal,_paidadsclick_liketotal,
    SUM(DISTINCT(CASE WHEN _paidadssharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _paidadssharescore,
    SUM(DISTINCT(CASE WHEN _paidadscommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _paidadscommentscore,
    SUM(DISTINCT(CASE WHEN _paidadsfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _paidadsfollowscore,
    SUM(DISTINCT(CASE WHEN _paidadsvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsvisitscore,
    SUM(DISTINCT(CASE WHEN _paidadsclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsclick_likescore,
    
    FROM
    (
      SELECT  
     _domain,
     SUM(_paidadsshare) AS _paidadssharetotal, 
     SUM(_paidadscomment) AS _paidadscommenttotal,
     SUM(_paidadsfollow) AS _paidadsfollowtotal,
     SUM(_paidadsvisit) AS _paidadsvisittotal,
     SUM(_paidadsclick_like) AS _paidadsclick_liketotal,
  FROM (
    SELECT  _domain,
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _paidadsshare,  
     SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _paidadscomment,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _paidadsfollow,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _paidadsvisit,  
    SUM( CASE WHEN  _engagement = 'Paid Ads'  AND SUBSTR(_description, 1,7) LIKE  '%Clicks%' OR SUBSTR(_description, 1,4) LIKE  '%Like%'  THEN 1 ELSE 0 END ) AS  _paidadsclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT *  FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Paid Ads') 
    )
  --WHERE 
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5,6
  ORDER BY _paidadssharescore  DESC
)
)
, paid_social_last_engagement AS(
 SELECT paid_sosial_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_paid_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _paid_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _paid_social_year
            FROM (
  SELECT * EXCEPT (rownum)
  FROM (
  SELECT _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END AS _id,
  MAX(_date) OVER(PARTITION BY _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END )  AS _last_engagement_TS,
  ROW_NUMBER() OVER(PARTITION BY 
    _domain,_email,_id  ORDER BY _date DESC) AS rownum 
   FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ("Paid Ads")
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN paid_sosial_engagements ON  paid_sosial_engagements._domain = _last_engagement ._domain
)
,organic_sosial_engagements AS (
  SELECT *,
  CASE WHEN  _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore >= 35 THEN 35 ELSE _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore  END AS _organic_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM (
SELECT _domain,_organicsharetotal,_organiccommenttotal,_organicfollowtotal,_organicvisittotal,_organicclick_liketotal,
    SUM(DISTINCT(CASE WHEN _organicsharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _organischarescore,
    SUM(DISTINCT(CASE WHEN _organiccommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _organiccommentscore,
    SUM(DISTINCT(CASE WHEN _organicfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _organicfollowscore,
    SUM(DISTINCT(CASE WHEN _organicvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicvisitscore,
    SUM(DISTINCT(CASE WHEN _organicclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicclick_likescore,
    
    FROM
    (
      SELECT  
     _domain,
     SUM(_organicsshare) AS _organicsharetotal, 
    SUM(_organiccomment) AS _organiccommenttotal,
    SUM(_organicfollow) AS _organicfollowtotal,
    SUM(_organicvisit) AS _organicvisittotal,
    SUM(_organicclick_like) AS _organicclick_liketotal,


  FROM (
    SELECT  _domain,
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _organicsshare,  
     SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _organiccomment,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _organicfollow,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _organicvisit,  
    SUM( CASE WHEN  _engagement = 'Organic Social'  AND SUBSTR(_description, 1,7) LIKE  '%Clicks%' OR SUBSTR(_description, 1,4) LIKE  '%Like%' THEN 1 ELSE 0 END ) AS  _organicclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Organic Social')
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4,5,6
  ORDER BY _organicsharetotal  DESC
)
)
, organic_social_last_engagement AS(
 SELECT organic_sosial_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_organc_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _organc_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _organc_social_year
            FROM (
  SELECT * EXCEPT (rownum)
  FROM (
  SELECT _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END AS _id,
  MAX(_date) OVER(PARTITION BY _domain,_email,CASE WHEN _email IS NULL THEN _domain
WHEN _domain  IS NULL THEN _email
ELSE CONCAT(_email, " ",_domain) END )  AS _last_engagement_TS,
  ROW_NUMBER() OVER(PARTITION BY 
    _domain,_email,_id  ORDER BY _date DESC) AS rownum 
   FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ("Paid Ads")
  ) WHERE rownum = 1
  ) _last_engagement 
  RIGHT JOIN organic_sosial_engagements ON  organic_sosial_engagements._domain = _last_engagement ._domain
),weekly_web_data  AS (
  SELECT
        _domain,
        -- _week,
        -- _year,
        -- COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
        COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
        COALESCE(SUM(CASE WHEN _pageName IS NOT NULL THEN 1 END), 0) AS _website_page_view,
        COALESCE(COUNT(DISTINCT _visitorid), 0) AS _website_visitor_count,
        COALESCE(COUNT(DISTINCT CASE WHEN _pageName LIKE "%careers%" THEN _visitorid END), 0) AS _career_page,
        TRUE AS _visited_website,
        -- MAX(_timestamp) AS last_engaged_date
      FROM (
        /* SELECT
          DATE(_starttime) AS _timestamp,
          company._domain,
          SUM(CAST(_engagementtime AS INT64)) AS _website_time_spent,
          COUNT(DISTINCT(_page)) AS _website_page_view,
          COUNT(DISTINCT msflow._visitorid) AS _website_visitor_count,
          -- newsletter_subscription in the future,
        FROM
          `logicsource_mysql.mouseflow_pageviews` msflow
        LEFT JOIN (
          SELECT
            DISTINCT _ipaddr,
            _website AS _domain
          FROM
            `webtrack_ipcompany.webtrack_ipcompany_6sense`) company
          USING
            (_ipaddr)
        GROUP BY
          1, 2  */
          SELECT 
            _domain, 
            _visitorid,
            DATETIME(_timestamp) AS _timestamp, 
            EXTRACT(WEEK FROM _timestamp) AS _week,  
            EXTRACT(YEAR FROM _timestamp) AS _year, 
            _entrypage AS _pageName, 
            -- "Web Visit" AS _engagement, 
            CAST(_engagementtime AS INT64) AS _website_time_spent,
            _totalPages AS _website_page_view
          FROM 
            `logicsource.dashboard_mouseflow_kickfire` web 
          WHERE 
            NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email') 
            AND _webactivity IS NOT NULL
          ORDER BY
            _timestamp DESC
          )
        WHERE
          --(_timestamp BETWEEN date_start AND date_end)
        --AND  
          LENGTH(_domain) > 2
        GROUP BY
          1 
     )
     -- Get scores for web visits activity
   , weekly_web_score AS (
        SELECT
          * EXCEPT(website_time_spent_score,
            website_page_view_score,
            website_visitor_count_score,
            visited_website_score),
            website_time_spent_score AS _website_time_spent_score,
            website_page_view_score AS _website_page_view_score,
            website_visitor_count_score AS _website_visitor_count_score,
            visited_website_score AS _visited_website_score,
            CASE
              WHEN (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + career_page_score) > 40 THEN 40
              ELSE (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + career_page_score)
            END AS _web_score_total
        FROM (
          SELECT
            *,
            COALESCE((_website_time_spent), 0)
              AS website_time_spent_score,
           ( CASE 
              WHEN _website_page_view >= 5 THEN 15
              -- WHEN _website_page_view < 5 THEN 10
              ELSE 0
            END
            +
            CASE 
              WHEN _website_page_view >= 5 THEN 15
              -- WHEN _website_page_view < 5 THEN 10
              ELSE 0
            END
            )
              AS website_page_view_score,
            (CASE WHEN _website_visitor_count >= 3 THEN 10
              -- WHEN _website_visitor_count < 3 THEN 5
              ELSE 0
            END
            + 
            CASE WHEN _website_visitor_count < 3 THEN 5
              -- WHEN _website_visitor_count < 3 THEN 5
              ELSE 0
            END)
              AS website_visitor_count_score,
              CASE WHEN _career_page > 1 THEN -5 ELSE 0 END AS career_page_score,
            5 AS visited_website_score
          FROM
            weekly_web_data ) 
  ), web_last_engagement AS (           
  SELECT 
 web_data._domain,web_data.* EXCEPT (_domain),
  _last_engagement.last_engaged_date AS _last_engagement_web,
  EXTRACT(WEEK FROM _last_engagement.last_engaged_date) AS week_web,
  EXTRACT(YEAR FROM _last_engagement.last_engaged_date) AS year_web
  FROM (
    SELECT 
        /* _website, */  _domain,
        MAX(_timestamp) AS last_engaged_date
        FROM (
         SELECT 
                   _domain AS _domain, 
                    _visitorid,
                    DATETIME(_timestamp) AS _timestamp, 
                    _engagementtime AS _website_time_spent,
                    _totalPages AS _website_page_view
                FROM `logicsource.dashboard_mouseflow_kickfire`
                WHERE NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email') 
                AND _webactivity IS NOT NULL
                AND (_domain IS NOT NULL AND _domain != '')
                ORDER BY _timestamp DESC
                )
    -- WHERE REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') = 'opcw.org'
    GROUP BY 1
    
    
    ) _last_engagement
    RIGHT JOIN weekly_web_score web_data ON   web_data._domain = _last_engagement._domain 
), combine_all AS ( #combine all channel data and calculate into the max data. 
   SELECT *,(COALESCE(_GatedContentscore_total ,0) + COALESCE(_formFilled_webinarscore_total,0)  + COALESCE(_email_score,0) 
  + COALESCE(_paid_ads_score_total,0) + COALESCE(_organic_ads_score_total,0) +  COALESCE(_web_score_total,0)
   ) AS _total_score,
   CASE 
   WHEN (
     _last_engagement_email_date >= formfilled_last_engaged_date
     AND
  
     _last_engagement_email_date >= organic_social_last_engagement
     AND 
     _last_engagement_email_date >= paid_social_engaged_date
     AND 
     _last_engagement_email_date >=  engagement_web_date
     ) THEN _last_engagement_email_date
     WHEN (
      formfilled_last_engaged_date >= _last_engagement_email_date
      AND 
      formfilled_last_engaged_date >= organic_social_last_engagement
      AND 
      formfilled_last_engaged_date >= paid_social_engaged_date
       AND 
      formfilled_last_engaged_date >= engagement_web_date
    
    ) THEN formfilled_last_engaged_date
    WHEN (
     paid_social_engaged_date >= _last_engagement_email_date
      AND 
      paid_social_engaged_date >= formfilled_last_engaged_date
      AND 
      paid_social_engaged_date >= organic_social_last_engagement
       AND 
      paid_social_engaged_date >= engagement_web_date
    
    ) THEN paid_social_engaged_date
     WHEN (
     organic_social_last_engagement >= _last_engagement_email_date
      AND 
      organic_social_last_engagement>= formfilled_last_engaged_date
      AND 
       organic_social_last_engagement >= paid_social_engaged_date
         AND 
       organic_social_last_engagement >= engagement_web_date
    
    ) THEN organic_social_last_engagement
         WHEN (
      engagement_web_date >= _last_engagement_email_date
      AND 
       engagement_web_date >= formfilled_last_engaged_date
      AND 
        engagement_web_date >= paid_social_engaged_date
         AND 
        engagement_web_date >= organic_social_last_engagement
    
    ) THEN  engagement_web_date
  
    END AS _last_engagement_date
    FROM (
      SELECT main.*,
      email_engagement.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS  _last_engagement_email_date,
      --paid_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
     -- organic_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
      formfill.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_formfilled AS DATE), DATE('2000-01-01')) AS  formfilled_last_engaged_date,
     
      cs.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organc_social AS DATE), DATE('2000-01-01')) AS  organic_social_last_engagement,
      search_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  paid_social_engaged_date,
      web_data.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_web AS DATE), DATE('2000-01-01')) AS  engagement_web_date,
      FROM contacts AS main
    LEFT JOIN email_last_engagementdate  AS email_engagement ON (main._domain = email_engagement._domain )
    LEFT JOIN formfill_last_engagementdate AS formfill ON (main._domain = formfill._domain)
    LEFT JOIN organic_social_last_engagement AS cs ON (main._domain = cs._domain)
    LEFT JOIN paid_social_last_engagement AS search_ads ON (main._domain = search_ads._domain) 
    LEFT JOIN web_last_engagement AS web_data ON (main._domain = web_data._domain)

)
), icp_score AS (
   SELECT 
 _domain AS _domain, 
 total_employee, total_score_divide_2, total_score, max_score
  FROM `x-marketing.logicsource.account_icp_score`
),all_data AS (
SELECT *,  EXTRACT(YEAR FROM _last_engagement_date ) AS _last_engagemtn_year,
  EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,          
  DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
  CASE 
  WHEN DATE_DIFF(CURRENT_DATE(),_last_engagement_date, DAY) > 180  THEN (_total_score - 50)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90  THEN (_total_score - 25)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 20)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30  THEN (_total_score - 15)
  ELSE _total_score
  END AS _score_new
FROM combine_all
--WHERE _domain =  'pepsico.com'
ORDER BY _last_engagement_date DESC,_total_score DESC
) 
SELECT all_data.*,
icp_score.* EXCEPT(_domain),
COALESCE(_total_score,0)   + COALESCE(max_score,0)  AS _total_score_icp_intent,
CASE WHEN COALESCE(_total_score,0)   + COALESCE(max_score,0)  >= 10 AND COALESCE(_total_score,0)   + COALESCE(max_score,0)  <= 20 THEN 'Low'
WHEN  COALESCE(_total_score,0)   + COALESCE(max_score,0)  >= 21 AND COALESCE(_total_score,0)   + COALESCE(max_score,0)  <= 49 THEN 'Medium'
WHEN COALESCE(_total_score,0)   + COALESCE(max_score,0)  >= 50  THEN 'High' ELSE "Low" END AS legend
FROM all_data
LEFT JOIN icp_score on all_data._domain = icp_score._domain  ;

CREATE OR REPLACE TABLE `logicsource.zoominfo_intent_score` AS
WITH account AS (
SELECT 
_company, 
_domain, 
_revenue, 
_industry, 
_company_segment, 
_employee_range, 
_employee_range_c, 
_numberofemployees, 
_annualrevenue, 
_annual_revenue_range, 
_annual_revenue_range_c, 
_source,
days_since_last_engaged, 
_score_new, 
_last_engagement_date, 
_total_score, 
total_employee, total_score_divide_2, total_score, max_score, _total_score_icp_intent, legend,source_zi_intent,_zi_intent
FROM logicsource.zoominfo_account_engagement_scoring
WHERE _domain IS NOT NULL

)
,dummy_date AS (
  SELECT *, 
  DENSE_RANK() OVER (ORDER BY _date DESC) AS dense_rank  FROM (
  SELECT DISTINCT CAST(_lastsignal AS DATE) AS _date ,CAST(_exporteddate AS DATE) AS _exporteddate,
  FROM `x-marketing.logicsource_mysql.db_zoominfo_intent`
  )
)
,all_account AS ( 
SELECT acc.*, report._score AS _intent_score,report._topic,
_date,
_exporteddate,
EXTRACT(WEEK FROM _date)-1 AS _week, 
EXTRACT(YEAR FROM _date) AS _year, 
CAST(report._score AS INT64) AS _score,
 AVG(CAST(report._score AS INT64))  AS _avgCompositeScore,
FROM account acc 
--CROSS JOIN dummy_date 
LEFT JOIN (SELECT _score,_topic,_domain , CAST(_lastsignal AS DATE) AS _date,CAST(_exporteddate AS DATE) AS _exporteddate,
FROM `x-marketing.logicsource_mysql.db_zoominfo_intent` ) report ON acc._domain = report._domain

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
), _engagement AS (
SELECT  
     *,
    SUM(_emailOpened) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_opened,
    SUM(_emailClicked) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_clicked,
    SUM(_formfilled) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_formfilled,
    SUM(_paidads) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_paidads,
    SUM(_organicsocial) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_organicsocial,
    SUM(_webvisit) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_webvisit,
     FROM (
      SELECT
      _domain,EXTRACT(WEEK FROM _date) AS _week,  EXTRACT(YEAR FROM _date) AS _year,
      SUM(CASE WHEN _engagement = 'Email Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
      SUM( CASE WHEN _engagement = 'Email Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
      SUM( CASE WHEN _engagement = 'Form Filled' THEN 1 ELSE 0 END) AS _formfilled,
      SUM( CASE WHEN _engagement = 'Paid Ads' THEN 1 ELSE 0 END) AS _paidads,
      SUM( CASE WHEN _engagement = 'Organic Social' THEN 1 ELSE 0 END) AS _organicsocial,
      SUM( CASE WHEN _engagement = 'Web Visit' THEN 1 ELSE 0 END) AS _webvisit,
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked','Form Filled','Paid Ads','Organic Social','Web Visit') AND EXTRACT(DATE FROM _date) BETWEEN (SELECT MIN(_date) FROM dummy_date) AND CURRENT_DATE()
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
    ) a
    --WHERE _domain = 'foodtravelexperts.com'
   ORDER BY 1, 3 DESC, 2 DESC
) SELECT all_account.*, 
_engagement.* EXCEPT (_domain,_week,_year),
CASE WHEN _source = 'Target' THEN "Existing"  ELSE "Net New" END AS _account_status,
CAST(NULL AS STRING) AS _intent

FROM all_account 
LEFT JOIN _engagement ON (all_account._domain = _engagement._domain AND 
all_account._week = _engagement._week AND
all_account._year = _engagement._year )

;


#Set NULLS to 0 for aggregation in
UPDATE `logicsource.zoominfo_intent_score` 
SET running_opened = CASE WHEN running_opened IS NOT NULL THEN running_opened ELSE 0 END,
running_clicked = CASE WHEN running_clicked IS NOT NULL THEN running_clicked ELSE 0 END,
running_formfilled = CASE WHEN running_formfilled IS NOT NULL THEN running_formfilled ELSE 0 END,
running_paidads = CASE WHEN running_paidads IS NOT NULL THEN running_paidads ELSE 0 END,
running_organicsocial = CASE WHEN running_organicsocial IS NOT NULL THEN running_organicsocial ELSE 0 END,
running_webvisit = CASE WHEN running_webvisit IS NOT NULL THEN running_webvisit ELSE 0 END,
_avgCompositeScore = CASE WHEN _avgCompositeScore IS NOT NULL THEN _avgCompositeScore ELSE 0 END,
max_score = CASE WHEN max_score IS NOT NULL THEN max_score ELSE 0 END
WHERE _domain IS NOT NULL;



#Set Intent based on the rules on dashboard
UPDATE `logicsource.zoominfo_intent_score`
SET _intent = 
  CASE 
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore >= 60  AND _total_score_icp_intent
 >= 60 THEN "High"
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore < 60  AND _total_score_icp_intent
 >= 60 THEN "High"
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore >= 60  AND _total_score_icp_intent
 < 60 THEN "Medium"
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore < 60 AND _total_score_icp_intent
 < 60 THEN "Low"
        /* WHEN _tier = 3 AND _avgCompositeScore >= 60  AND _total_score_icp_intent
 >= 60 THEN "High"
        WHEN _tier = 3 AND _avgCompositeScore < 60  AND _total_score_icp_intent
 >= 80 THEN "High"
        WHEN _tier = 3 AND _avgCompositeScore >= 60  AND _total_score_icp_intent
 < 60 THEN "Medium"
        WHEN _tier = 3 AND _avgCompositeScore < 60  AND _total_score_icp_intent
 BETWEEN 60 AND 79 THEN "Medium"
        WHEN _tier = 3 AND _avgCompositeScore < 60 AND _total_score_icp_intent
 < 60 THEN "Low" */
    END
WHERE _domain IS NOT NULL;