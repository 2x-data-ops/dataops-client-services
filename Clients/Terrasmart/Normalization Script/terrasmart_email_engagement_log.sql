------------------------------------------------------------------------------
---------------------------- Email Engagement Log ----------------------------
------------------------------------------------------------------------------

/* 
  This script is used typically for the email performance page/dashboard 
  CRM/Platform: Pardot
  Data type: Email Engagement
  Depedency Table: db_terrasmart_pardot/db_gibraltar_mysql
  Target table: db_email_engagements_log
*/


TRUNCATE TABLE `x-marketing.terrasmart.db_email_engagements_log`;

INSERT INTO `x-marketing.terrasmart.db_email_engagements_log` (
  _sdc_sequence,
  _prospectID,
  _email,
  _campaignID,
  _utmcampaign,
  _campaignCode,
  _timestamp,
  _description,
  _email_id,
  email_template_id,
  _engagement,
  _subject,
  _campaignSentDate,
  _screenshot,
  _landingpage,
  _name,
  _title,
  _phone,
  _seniority,
  _company,
  _industry,
  _city,
  _state,
  _country,
  _function,
  _isBot,
  _isPageview,
  _totalPageViews,
  _averagePageViews
)

WITH airtable_info AS (
  SELECT 
    _pardotID,
    CASE
      WHEN airtable._senddate != ''
      THEN CAST(airtable._senddate AS TIMESTAMP)
    END AS _liveDate,
    airtable._code,
    airtable._subject,
    airtable._screenshot,
    airtable._landingPage
  FROM `x-marketing.gibraltar_mysql.db_airtable_pardot_email` airtable
  QUALIFY ROW_NUMBER() OVER(PARTITION BY airtable._pardotid ORDER BY airtable._id DESC) = 1
),
prospect_info AS (
  SELECT
    LOWER(prospect.email) AS _email,
    CONCAT(prospect.first_name, ' ', prospect.last_name) AS _name,
    prospect.job_title AS _title,
    prospect.phone AS _phone,
    CASE
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Assistant to%") THEN "Non-Manager"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Senior Counsel%") THEN "VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%General Counsel%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Founder%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%C-Level%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%CDO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%CIO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%CMO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%CFO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%CEO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Chief%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%coordinator%") THEN "Non-Manager"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%COO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Sr.VP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%srvp%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Senior VP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%SR VP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Sr. VP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%S.V.P%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Exec Vp%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Executive VP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Exec VP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Executive Vice President%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%EVP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%E.V.P%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%SVP%") THEN "Senior VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%V.P%") THEN "VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%VP%") THEN "VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Vice Pres%") THEN "VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%V P%") THEN "VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%President%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Director%") THEN "Director"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%CTO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Dir%") THEN "Director"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Dir.%") THEN "Director"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%MDR%") THEN "Non-Manager"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%MD%") THEN "Director"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%GM%") THEN "Director"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Head%") THEN "VP"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Manager%") THEN "Manager"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%escrow%") THEN "Non-Manager"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%cross%") THEN "Non-Manager"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%crosse%") THEN "Non-Manager"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Partner%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%CRO%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Chairman%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Owner%") THEN "C-Level"
      WHEN LOWER(prospect.job_title) LIKE LOWER("%Team Lead%") THEN "Manager"
    END AS _seniority,
    prospect.company AS _company,
    prospect.industry AS _industry,
    -- prospect.annual_revenue AS _revenuerange,
    -- prospect.employees AS _employees,
    prospect.city AS _city,
    prospect.state AS _state,
    prospect.country AS _country,
    _function
    -- prospect.crm_lead_fid AS _sfdcLeadid,
    -- prospect.crm_contact_fid AS _sfdcContactid,
    -- prospect.crm_owner_fid AS _sfdcOwnerid,
    -- prospect.source AS _source,
  FROM `x-marketing.terrasmart_pardot.prospects` prospect
  LEFT JOIN `x-marketing.gibraltar_mysql.w_routables` routable 
    ON LOWER(prospect.email) = LOWER(routable._email)
  WHERE email NOT LIKE '%@2x.marketing%' 
    AND email NOT LIKE '%2X%' 
    AND email NOT LIKE '%@terrasmart.com' 
    AND email NOT LIKE '%test%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY LOWER(prospect.email) ORDER BY id DESC) = 1
),
main_table AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    LOWER(prospect.email) AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _utmcampaign,
    _code AS _campaignCode,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id,
    activity.type
  FROM
    `x-marketing.terrasmart_pardot.visitor_activities` activity
  LEFT JOIN
    `x-marketing.terrasmart_pardot.prospects` prospect
  ON
    activity.prospect_id = prospect.id
  LEFT JOIN
    `x-marketing.terrasmart_pardot.campaigns` campaign
  ON
    activity.campaign_id = campaign.id
  JOIN
    airtable_info airtable
  ON
    CAST(activity.list_email_id AS STRING) = airtable._pardotid
  WHERE activity.type_name IN ('Email', 'Email Tracker')
    AND email NOT LIKE '%@2x.marketing%' 
    AND email NOT LIKE '%2X%' 
    AND email NOT LIKE '%@terrasmart.com' 
    AND email NOT LIKE '%test%'
),
delivered_email AS (
  SELECT
    main_table.* EXCEPT (type),
    'Delivered' AS _engagement
  FROM main_table
  WHERE main_table.type = 6 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY main_table._prospectID, main_table._email_id ORDER BY main_table._timestamp DESC) = 1
), 
open_email AS (
  SELECT
    main_table.* EXCEPT (type),
    'Opened' AS _engagement
  FROM main_table
  WHERE main_table.type = 11 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY main_table._prospectID, main_table._email_id ORDER BY main_table._timestamp DESC) = 1
), 
click_email AS (
  SELECT
    main_table.* EXCEPT (type),
    'Clicked' AS _engagement
  FROM main_table
  WHERE main_table.type = 1 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY main_table._prospectID, main_table._email_id ORDER BY main_table._timestamp DESC) = 1
),
sent_email AS (
  SELECT
    main_table.* EXCEPT (type),
    'Sent' AS _engagement
  FROM main_table
  WHERE main_table.type = 6 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY main_table._prospectID, main_table._email_id ORDER BY main_table._timestamp DESC) = 1
),
bounce_email AS (
  SELECT
    main_table.* EXCEPT (type),
    'Bounced' AS _engagement
  FROM main_table
  WHERE main_table.type = 13
  QUALIFY ROW_NUMBER() OVER(PARTITION BY main_table._prospectID, main_table._email_id ORDER BY main_table._timestamp DESC) = 1
),
softbounce_email AS (
  SELECT 
    main_table.* EXCEPT (type),
    'Soft Bounced' AS _engagement
  FROM main_table
  WHERE main_table.type = 36
  QUALIFY ROW_NUMBER() OVER(PARTITION BY main_table._prospectID, main_table._email_id ORDER BY main_table._timestamp DESC) = 1 
),
allbounced_email AS (
  SELECT 
    * 
  FROM bounce_email
  UNION ALL
  SELECT 
    * 
  FROM softbounce_email
),
new_delivered_email AS(
  SELECT 
    delivered.*
  FROM delivered_email delivered
  LEFT JOIN allbounced_email bounce 
    ON delivered._campaignID = bounce._campaignID 
    AND delivered._prospectID = bounce._prospectID
  WHERE bounce._campaignID IS NULL 
    AND bounce._prospectID IS NULL
),
opt_outs_email AS (
  SELECT
    main_table.* EXCEPT (type),
    'Unsubscribed' AS _engagement
  FROM main_table
  WHERE main_table.type IN (12, 35) 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY main_table._prospectID, main_table._email_id ORDER BY main_table._timestamp DESC) = 1
),
clicks_downloads AS (
  SELECT
    main_table.* EXCEPT (type),
    'Clicked' AS _engagement
  FROM main_table
  WHERE main_table.type = 1   /* Click */
    AND (
    (_email NOT LIKE '%2x.marketing%' AND _email NOT LIKE '%terrasmart%')
    OR
    _email IS NULL
    )

  UNION ALL
  # To get all downloads without matching campaigns
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    LOWER(prospect.email) AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _utmcampaign,
    _code AS _campaignCode,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id,
    'Downloaded' AS _engagement
  FROM `x-marketing.terrasmart_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.terrasmart_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.terrasmart_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  LEFT JOIN airtable_info AS airtable
    ON CAST(activity.list_email_id AS STRING) = airtable._pardotid
  WHERE activity.type = 4   /* Success */
    AND (
      (email NOT LIKE '%2x.marketing%' AND email NOT LIKE '%terrasmart%')
      OR
      email IS NULL
    )
),
clicks_downloads_timeline AS (
  # Order clicks and downloads in a timeline series
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY _prospectID ORDER BY _timestamp) AS _rownum
  FROM clicks_downloads
),
mql_submission_email AS (
  # Get those downloads that follow right after a click 
  SELECT
    download._sdc_sequence,
    download._prospectID,
    download._email,
    click._campaignID,
    click._utmcampaign,
    click._campaignCode,
    download._timestamp,
    download._description,
    --download._engagement,
    click._email_id,
    click.email_template_id,
    download._engagement,
  FROM clicks_downloads_timeline download
  JOIN clicks_downloads_timeline click
    ON download._prospectID = click._prospectID
    AND EXTRACT(DAY FROM download._timestamp) = EXTRACT(DAY FROM click._timestamp)
    AND download._rownum = click._rownum + 1
    AND click._engagement = 'Clicked'
  WHERE download._engagement = 'Downloaded'
    
),
engagements_consolidated AS (
  SELECT * FROM open_email 
  UNION ALL
  SELECT * FROM click_email 
  UNION ALL
  SELECT * FROM sent_email 
  UNION ALL 
  SELECT * FROM bounce_email 
  UNION ALL 
  SELECT * FROM softbounce_email 
  UNION ALL 
  SELECT * FROM opt_outs_email
  UNION ALL 
  SELECT * FROM mql_submission_email
  UNION ALL 
  SELECT * FROM new_delivered_email
)

SELECT 
  engagements_consolidated.*,
  -- airtable_info._utm_source,
  -- airtable_info._utm_medium,
  airtable_info._subject, 
  CASE 
    WHEN LENGTH(CAST(airtable_info._livedate AS STRING)) > 0 
    THEN airtable_info._livedate
    ELSE NULL 
  END AS _campaignSentDate,
  airtable_info._screenshot, 
  airtable_info._landingpage,
  -- airtable_info._code AS _campaignCode,
  prospect_info.* EXCEPT(_email),
  -- airtable_info._segment,
  -- airtable_info._emailname,
  CAST(NULL AS BOOL) AS _isBot,
  CAST(NULL AS BOOL) AS _isPageview,
  CAST(0 AS INTEGER) AS _totalPageViews,
  CAST(0 AS INTEGER) AS _averagePageViews
FROM engagements_consolidated
-- LEFT JOIN campaign_info ON CAST(engagements._campaignID AS STRING) = campaign_info._pardotid
LEFT JOIN airtable_info 
  ON engagements_consolidated._campaignCode = airtable_info._code
LEFT JOIN prospect_info 
  ON LOWER(engagements_consolidated._email) = LOWER(prospect_info._email);


------------------------------------------------------------------------------
------------------------------- Labelling Bots -------------------------------
------------------------------------------------------------------------------

UPDATE `x-marketing.terrasmart.db_email_engagements_log` origin  
SET origin._isBot = true
FROM (
  WITH opened_emails AS (
    SELECT
      _email, 
      _campaignCode, 
      _timestamp
    FROM `x-marketing.terrasmart.db_email_engagements_log`
    WHERE _engagement = 'Opened'     
  ),
  clicked_emails AS (
    SELECT
      _email, 
      _campaignCode, 
      _timestamp
    FROM `x-marketing.terrasmart.db_email_engagements_log`
    WHERE _engagement = 'Clicked' 
  )
  SELECT DISTINCT
    click._email, 
    click._campaignCode, 
    open._timestamp AS open_timestamp, 
    click._timestamp AS click_timestamp
  FROM opened_emails AS open
  JOIN clicked_emails AS click
    ON open._email = click._email
    AND open._campaignCode = click._campaignCode
) scenario
WHERE origin._email = scenario._email
  AND origin._campaignCode = scenario._campaignCode
  AND TIMESTAMP_DIFF(click_timestamp, open_timestamp, SECOND) < 3
;
