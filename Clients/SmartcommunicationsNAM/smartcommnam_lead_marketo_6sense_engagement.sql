 WITH alldata AS (
  WITH lead_marketo AS (
    WITH prospect_info AS (
      SELECT DISTINCT 
        CAST(id AS STRING) AS _id,
        email AS _email,
        CONCAT(firstname,' ', lastname) AS _name,
        RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain, 
        -- title AS _jobtitle,
        -- job_function__c AS _function,
        phone AS _phone,
        company AS _company,
        CAST(annualrevenue AS STRING) AS _revenue,
        industry AS _industry,
        city AS _city,
        state AS _state, 
        country AS _country,
        "" AS _persona,
        leadscore AS _leadscore,
        -- lead_lifecycle_stage__c AS _lifecycleStage,
        -- leadsourcedetail,
        -- mostrecentleadsource,
        -- mostrecentleadsourcedetail,
        -- programs.name,
        -- programs.channel
        FROM `smartcomm_marketo.leads`
        QUALIFY ROW_NUMBER() OVER( PARTITION BY email ORDER BY id DESC) = 1
      ),
    open_email AS (
      SELECT _sdc_sequence,
      CAST(primary_attribute_value_id AS STRING) AS _campaignID,
      primary_attribute_value AS _campaign,
      -- '' AS _subject,
      -- '' AS _email,
      activitydate AS _timestamp,
      'Opened' AS _engagement,
      -- '' AS _description,
      CAST(leadid AS STRING) AS _leadid, 
    FROM `x-marketing.smartcomm_marketo.activities_open_email`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
    ),
    click_email AS (
      SELECT _sdc_sequence,
      CAST(primary_attribute_value_id AS STRING) AS _campaignID,
      primary_attribute_value AS _campaign,
      -- '' AS _subject,
      -- '' AS _email,
      activitydate AS _timestamp,
      'Clicked' AS _engagement,
      -- '' AS _description,
      CAST(leadid AS STRING) AS _leadid, 

    FROM `x-marketing.smartcomm_marketo.activities_click_email`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
    ),
    engagements_combined AS (
      SELECT * FROM open_email
      UNION ALL
      SELECT * FROM click_email
    )
    SELECT
      engagements_combined.* EXCEPT (_leadid),
      prospect_info.*
    FROM engagements_combined
    RIGHT JOIN prospect_info
    ON engagements_combined._leadid = prospect_info._id
  ),
  sixsense_engagement AS (
    SELECT
      _6sensedomain,
      _6sensecompanyname,
      _6sensecountry,
      CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account

    FROM `x-marketing.smartcommnam.db_6sense_engagement_log`
  )
  SELECT *
  FROM lead_marketo
  INNER JOIN sixsense_engagement
  ON sixsense_engagement._6sensedomain = lead_marketo._domain
    -- WHERE _engagement IN ('Opened', 'Clicked')
  QUALIFY ROW_NUMBER () OVER (PARTITION BY _id, _campaignID ORDER BY _timestamp DESC) = 1
 )
SELECT
  _sdc_sequence,
  _id,
  _email,
  _name,
  _domain,
  _campaignID,
  _campaign,
  _timestamp,
  _engagement,
  _phone,
  _company,
  _6sensedomain,
  _6sensecompanyname,
  _6sensecountry,
  _country_account,
  _revenue,
  _industry,
  _city,
  _state,
  _country,
  _persona,
  _leadscore,
  -- _6sensedomain,
  SUM(CASE WHEN _engagement = 'Opened' THEN 1 ELSE 0 END) OVER(PARTITION BY _id) AS _total_opened,
  SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) OVER(PARTITION BY _id) AS _total_clicked,
  -- SUM(CASE WHEN _engagement = NULL THEN 1 END) OVER(PARTITION BY _id) AS _total_clicked,
FROM alldata
