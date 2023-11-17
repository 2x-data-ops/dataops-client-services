--------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------- Accounth Health Script ------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------


--CREATE OR REPLACE TABLE `3x.db_consolidated_engagements_log` AS 
TRUNCATE TABLE`3x.db_consolidated_engagements_log`;

INSERT INTO `3x.db_consolidated_engagements_log`
WITH 
  contacts AS (

    SELECT
      _id,
      _email,
      _name,
      _domain,
      _jobtitle,
      _function,
      _seniority,
      _phone,
      _company,
      _revenue,
      _industry,
      _employee,
      _city,
      _state,
      _country, 
      _persona,
      _lifecycleStage,
      _sfdccontactid, 
      _sfdcaccountid,
      _sfdcleadid, 
      _target_contacts, 
      _target_accounts,
      _account_type
    FROM
      `3x.db_icp_database_log`
    WHERE
      _domain NOT LIKE '%2x.marketing%' 
  ),
  accounts AS (

    WITH accounts AS (
    SELECT * EXCEPT (rownum)
    FROM (
      SELECT
        DISTINCT 
        CAST(NULL AS INTEGER) AS _id,
        CAST(NULL AS STRING) AS _email,
        CAST(NULL AS STRING) AS _name,
        _domain,
        CAST(NULL AS STRING) AS _jobtitle,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _seniority,
        _phone,
        _company,
        _revenue,
        _industry,
        _employee,
        _city,
        _state,
        _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        CAST(NULL AS STRING) AS _sfdccontactid,
        _sfdcaccountid,
        CAST(NULL AS STRING) AS _sfdcleadid, 
        _target_contacts, 
        _target_accounts,
        _account_type,
        ROW_NUMBER() OVER(
            PARTITION BY _domain
             ORDER BY _target_accounts DESC
          ) 
          AS rownum
      FROM
        contacts
    ) WHERE rownum = 1
  ), _6sense_segment AS 
  (
    SELECT * EXCEPT (rownum)
    FROM (
    SELECT 
        DISTINCT 
        CAST(NULL AS INTEGER) AS _id,
        CAST(NULL AS STRING) AS _email,
        CAST(NULL AS STRING) AS _name,
        _6sensedomain AS _domain,
        CAST(NULL AS STRING) AS _jobtitle,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _seniority,
        CAST(NULL AS STRING) AS  _phone,
        _6sensecompanyname AS _company,
        CAST(NULL AS FLOAT64) AS _revenue,
        _industry,
        CAST(NULL AS FLOAT64) AS _employee,
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,
        _6sensecountry AS _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        CAST(NULL AS STRING) AS _sfdccontactid,
        CAST(NULL AS STRING) AS _sfdcaccountid,
        CAST(NULL AS STRING) AS _sfdcleadid, 
        CAST(NULL AS INT64) AS _target_contacts, 
        CAST(0 AS INT64) AS _target_accounts,
        CAST(NULL AS STRING) AS _account_type,
        ROW_NUMBER() OVER(
            PARTITION BY _6sensedomain
            ORDER BY LENGTH(_6sensecompanyname) DESC
          ) 
          AS rownum
      FROM
        `webtrack_ipcompany.db_6sense_3x_segments`
      WHERE
        _segment != '3X_230109 (Bombora 60+)_Intent Segment'
        AND _6sensedomain NOT IN (SELECT DISTINCT _domain FROM accounts)
    ) WHERE rownum = 1
  ),_campaign_accounts AS (
    SELECT * EXCEPT (rownum)
    FROM (
     SELECT
        DISTINCT CAST(NULL AS INTEGER) AS _id,
        CAST(NULL AS STRING) AS _email,
        CAST(NULL AS STRING) AS _name,
        _6sensedomain AS _domain,
        CAST(NULL AS STRING) AS _jobtitle,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _seniority,
        CAST(NULL AS STRING) AS  _phone,
        _6sensecompanyname AS _company,
        CAST(NULL AS FLOAT64) AS _revenue,
        CAST(NULL AS STRING) AS _industry,
        CAST(NULL AS FLOAT64) AS _employee,
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,
        _6sensecountry AS _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        CAST(NULL AS STRING) AS _sfdccontactid,
        CAST(NULL AS STRING) AS _sfdcaccountid,
        CAST(NULL AS STRING) AS _sfdcleadid, 
        CAST(NULL AS INT64) AS _target_contacts, 
        CAST(0 AS INT64) AS _target_accounts,
        CAST(NULL AS STRING) AS _account_type,
          ROW_NUMBER() OVER(
            PARTITION BY _6sensedomain
            ORDER BY LENGTH(_6sensecompanyname) DESC
          ) 
          AS rownum
      FROM
        `webtrack_ipcompany.db_6sense_3x_campaign_accounts`
      WHERE
        _6sensedomain NOT IN (SELECT DISTINCT _6sensedomain FROM `webtrack_ipcompany.db_6sense_3x_segments`)
        AND _6sensedomain NOT IN (SELECT DISTINCT _domain FROM accounts)
    ) WHERE rownum = 1
  ), webtrack AS (
      SELECT
        DISTINCT CAST(NULL AS INTEGER) AS _id,
        CAST(NULL AS STRING) AS _email,
        CAST(NULL AS STRING) AS _name,
        _website AS _domain,
        CAST(NULL AS STRING) AS _jobtitle,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _seniority,
        CAST(NULL AS STRING) AS  _phone,
        _name AS _company,
        CAST(NULL AS FLOAT64) AS _revenue,
        CAST(NULL AS STRING) AS _industry,
        CAST(NULL AS FLOAT64) AS _employee,
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,
        _country AS _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        CAST(NULL AS STRING) AS _sfdccontactid,
        CAST(NULL AS STRING) AS _sfdcaccountid,
        CAST(NULL AS STRING) AS _sfdcleadid, 
        CAST(NULL AS INT64) AS _target_contacts, 
        CAST(0 AS INT64) AS _target_accounts,
        CAST(NULL AS STRING) AS _account_type
      FROM (
        SELECT * EXCEPT (rownum)
        FROM (
        SELECT
          _website,
          _name,
          _country,
          ROW_NUMBER() OVER(
            PARTITION BY _website
            ORDER BY LENGTH(_name) DESC
          ) 
          AS rownum
        FROM  
          `webtrack_ipcompany.webtrack_ipcompany_6sense`
        ) WHERE rownum = 1
      )
      WHERE
        _website NOT IN (
          SELECT DISTINCT _6sensedomain FROM `webtrack_ipcompany.db_6sense_3x_segments`
          UNION DISTINCT
          SELECT DISTINCT _6sensedomain FROM `webtrack_ipcompany.db_6sense_3x_campaign_accounts`
        ) AND _website NOT IN (SELECT DISTINCT _domain FROM accounts)
  ) 
  SELECT *
  FROM (
  SELECT *
   FROM accounts
  UNION ALL 
  SELECT *
   FROM _6sense_segment
  UNION ALL 
  SELECT *
   FROM _campaign_accounts
  UNION ALL 
  SELECT *
   FROM webtrack
  ) 
    -- LEFT JOIN
    --   opps AS opp_w_id ON main._sfdcaccountid IS NOT NULL AND opp_w_id._account_id = main._sfdcaccountid

  ),
  opps_created AS (

    SELECT
      DISTINCT _opportunity_id,
      _account_id,
      -- _contact_id,
      _account_name,
      _opportunity_name, 
      _current_stage,
      _createdate,
      _close_date,
      _amount,
      _domain,
      -- _industry,
      CAST(NULL AS INTEGER) AS _tier,
      -- _annualrevenue,
      _type,
      _lost_reason,
      _last_stage_change_date,
      _leadsource
    FROM
      `3x.db_opportunity_log` main

  ),
  opps_hist AS(

    SELECT
      opps_created._domain,
      _opportunity_name,
      _leadsource,
      _amount,
      main.*
    FROM
    (
      SELECT
        DISTINCT opportunityid AS _opportunity_id,
        createddate AS _last_stage_change,
        oldvalue__st AS _previous_stage,
        newvalue__st AS _current_stage,
        ROW_NUMBER() OVER(PARTITION BY opportunityid ORDER BY createddate DESC) AS _order
      FROM
        `x3x_salesforce.OpportunityFieldHistory`
      WHERE
        field = 'StageName'
      ORDER BY
        2 DESC
    ) main
    JOIN
      opps_created USING(_opportunity_id)
    WHERE
      _order = 1

  ),
  opps_combined AS (

    SELECT
      CAST(NULL AS STRING) AS _email,
      _domain,
      _createdate, 
      EXTRACT(WEEK FROM _createdate) AS _week,  
      EXTRACT(YEAR FROM _createdate) AS _year, 
      _opportunity_name AS _page_name,
      "Opportunity Created",
      CONCAT('Amount: $', FORMAT("%'.2f", _amount), "\n", 
              "Current Stage: ", _current_stage, "\n",
              "Lead Source", _leadsource) AS _description,
      _leadsource AS _utmsource,
      CAST(NULL AS STRING) AS _utmcampaign,
      CAST(NULL AS STRING) AS _utmmedium,
      CAST(NULL AS STRING) AS _utmcontent,
      CONCAT('https://df2000001lyxeeao.lightning.force.com/lightning/r/Opportunity/', _opportunity_id, '/view') AS _fullurl,
       CAST(NULL AS INT64) AS _frequency
    FROM
      opps_created

    UNION DISTINCT

    SELECT
      CAST(NULL AS STRING) AS _email,
      _domain,
      _last_stage_change, 
      EXTRACT(WEEK FROM _last_stage_change) AS _week,  
      EXTRACT(YEAR FROM _last_stage_change) AS _year, 
      _opportunity_name AS _page_name,
      "Opportunity Stage Change",
      CONCAT('Amount: $', FORMAT("%'.2f", _amount), "\n", 
              "Current Stage: ", _current_stage, "\n",
              "Previous Stage: ", _previous_stage, "\n",
              "Lead Source", _leadsource) AS _description,
      _leadsource AS _utmsource,
      CAST(NULL AS STRING) AS _utmcampaign,
      CAST(NULL AS STRING) AS _utmmedium,
      CAST(NULL AS STRING) AS _utmcontent,
      CONCAT('https://df2000001lyxeeao.lightning.force.com/lightning/r/Opportunity/', _opportunity_id, '/view') AS _fullurl,
       CAST(NULL AS INT64) AS _frequency
    FROM
      opps_hist

  ),
  email_engagement AS (

    SELECT 
      * 
    FROM ( 
      SELECT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      _timestamp, 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _subject AS _description,
      REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)') AS _utmsource,
      REGEXP_EXTRACT(_description, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
      REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
      REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent, 
      _description AS _fullurl,
       CAST(NULL AS INT64) AS _frequency,
      FROM 
        (SELECT DISTINCT * FROM `3x.db_email_engagements_log`)
      WHERE 
        LOWER(_engagement) NOT IN ('sent','delivered', 'bounced', 'unsubscribed')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing') 
      AND LENGTH(_domain)  > 1 
    ORDER BY 
      1, 3 DESC, 2 DESC

  ),
  web_views AS (

    SELECT 
      DISTINCT CAST(NULL AS STRING) AS _email, 
      _domain, 
      _timestamp, 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year, 
      _page AS _pageName, 
      "Web Visit" AS _engagement, 
      CONCAT("Time spent (secs): ",CAST(_engagementtime AS STRING)) AS _description,
      _utmsource,
      _utmcampaign,
      _utmmedium,
      _utmcontent,
      _fullurl,
       CAST(NULL AS INT64) AS _frequency,
    FROM 
      `3x.web_metrics` web 
    -- WHERE 
    --   LENGTH(_domain) > 1
    --   AND (NOT REGEXP_CONTAINS(LOWER(_fullurl), 'unsubscribe|career') OR _fullurl IS NULL)
    --   AND (NOT REGEXP_CONTAINS(LOWER(_utmsource), '6sense|linkedin|google|email') OR _utmsource IS NULL) 

  ),
  ----web click 
  ad_clicks AS (

    SELECT 
      DISTINCT CAST(NULL AS STRING) AS _email, 
      _domain, 
      _timestamp, 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year, 
      _page AS _pageName, 
      "Paid Ads Clicks" AS _engagement, 
      CONCAT("Time spent (secs): ",CAST(_engagementtime AS STRING), " \n", "Clicked on: ", _utmcontent) AS _description,
      _utmsource,
      _utmcampaign,
      _utmmedium,
      _utmcontent,
      _fullurl,
       CAST(NULL AS INT64) AS _frequency,
    FROM 
      `3x.web_metrics` web 
    WHERE 
      LENGTH(_domain) > 1
      AND NOT REGEXP_CONTAINS(LOWER(_page), 'unsubscribe')
      AND REGEXP_CONTAINS(_utmmedium, 'cpc|social')
      AND (NOT REGEXP_CONTAINS(LOWER(_utmsource), '6sense|email') OR _utmsource IS NULL)

  ),
  ---- 6sense web trend 
  ad_6sense_webeng_trend AS (

    SELECT
      DISTINCT *EXCEPT(_order)
    FROM (
      SELECT
        DISTINCT CAST(NULL AS STRING) AS _email, 
        _6sensedomain AS _domain, 
        TIMESTAMP(_date) AS _date,
        EXTRACT(WEEK FROM _date) AS _week,  
        EXTRACT(YEAR FROM _date) AS _year, 
        _campaignname,
        CONCAT("6Sense ", _websiteengagement) AS _engagement,
        "6Sense Engagement Trend" AS _description,
        "6Sense" AS _utmsource,
        _campaignname AS _utmcampaign,
        CAST(NULL AS STRING) AS _utmmedium,
        CAST(NULL AS STRING) AS _utmcontent,
        CAST(NULL AS STRING) AS _fullurl,
         CAST(NULL AS INT64) AS _frequency,
        ROW_NUMBER() OVER(PARTITION BY _6sensedomain, _campaignname, _websiteengagement ORDER BY DATE(_date)) AS _order
      FROM
        `webtrack_ipcompany.db_6sense_3x_campaign_accounts` main
      LEFT JOIN
        `webtrack_ipcompany.db_airtable_3x_ads` airtable ON main._campaignid = airtable._campaignid
      WHERE
        _websiteengagement != '-'
      )
    WHERE
      _order = 1
    ORDER BY 
      _domain
      

  ),
  ----6sense_click 
  ad_6sense_clicks AS (

    SELECT
      DISTINCT *EXCEPT(_order)
    FROM (
      SELECT
        DISTINCT CAST(NULL AS STRING) AS _email, 
        _6sensedomain AS _domain, 
        TIMESTAMP(_date) AS _date,
        EXTRACT(WEEK FROM _date) AS _week,  
        EXTRACT(YEAR FROM _date) AS _year, 
        _campaignname,
        "6Sense Clicks" AS _engagement,
        CONCAT(_clicks, " (Cumulative)") AS _description,
        "6Sense" AS _utmsource,
        _campaignname AS _utmcampaign,
        CAST(NULL AS STRING) AS _utmmedium,
        CAST(NULL AS STRING) AS _utmcontent,
        CAST(NULL AS STRING) AS _fullurl,
        CAST(_clicks AS INT64) AS _frequency,
        ROW_NUMBER() OVER(PARTITION BY _6sensedomain, _campaignname,EXTRACT(WEEK FROM _date),EXTRACT(YEAR FROM _date) ORDER BY DATE(_date) DESC) AS _order
      FROM
        `webtrack_ipcompany.db_6sense_3x_campaign_accounts` main
       JOIN
        `webtrack_ipcompany.db_airtable_3x_ads` airtable ON main._campaignid = airtable._campaignid
      WHERE
        CAST(_clicks AS INTEGER) >= 1 
      )
    WHERE
      _order = 1
    ORDER BY 
      _domain

  ), linkedin_click AS (
        SELECT
      DISTINCT *EXCEPT(_order)
    FROM (
     SELECT
        DISTINCT CAST(NULL AS STRING) AS _email, 
        _6sensedomain AS _domain, 
        TIMESTAMP(_date) AS _date,
        EXTRACT(WEEK FROM _date) AS _week,  
        EXTRACT(YEAR FROM _date) AS _year, 
        _campaignname,
        "Paid Ads Clicks" AS _engagement,
        CONCAT(_clicks, " (Cumulative)") AS _description,
        "6Sense" AS _utmsource,
        _campaignname AS _utmcampaign,
        CAST(NULL AS STRING) AS _utmmedium,
        CAST(NULL AS STRING) AS _utmcontent,
        CAST(NULL AS STRING) AS _fullurl,
        CAST(_clicks AS INT64) AS _frequency,
        ROW_NUMBER() OVER(PARTITION BY _6sensedomain, _campaignname,EXTRACT(WEEK FROM _date),EXTRACT(YEAR FROM _date) ORDER BY DATE(_date) DESC) AS _order
      FROM
        `webtrack_ipcompany.db_6sense_3x_campaign_accounts` main
      JOIN
        `x-marketing.x_mysql.db_airtable_3x_6sense_linkedin` airtable ON main._campaignid = CAST(airtable._6senseid AS STRING)
      WHERE
        CAST(_clicks AS INTEGER) >= 1 
              )
    WHERE
      _order = 1
    ORDER BY 
      _domain

  ), 
  content_engagement AS (

    SELECT 
      DISTINCT CAST(NULL AS STRING) AS _email, 
      _domain, 
      _timestamp, 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year, 
      _page AS _pageName, 
      "Content Engagement" AS _engagement, 
      CONCAT("Time spent (secs): ",CAST(_engagementtime AS STRING)) AS _description,
      _utmsource,
      _utmcampaign,
      _utmmedium,
      _utmcontent,
      _fullurl,
      CAST(NULL AS INT64) AS _frequency,
    FROM 
      `3x.web_metrics` web 
    WHERE 
      LENGTH(_domain) > 1
      AND NOT REGEXP_CONTAINS(_fullurl, 'Unsubscribe')
      AND REGEXP_CONTAINS(LOWER(_utmsource), 'case_study|blog')
      -- AND REGEXP_CONTAINS(_utmmedium, 'organic')

  ),
  /* account_engagement_6sense AS (
    SELECT 
      DISTINCT CAST(NULL AS STRING) AS _email, 
      _6sensedomain AS _domain,
      DATE(_date) AS _date,
      EXTRACT(WEEK FROM _date) AS _week,  
      EXTRACT(YEAR FROM _date) AS _year,
      _campaignname AS _pageName,
      (CAST(_clicks AS INTEGER)) AS _clicks,
      _websiteengagement
    FROM 
      `webtrack_ipcompany.db_6sense_3x_campaign_accounts` 
    ORDER BY 
      _date DESC
  ), */
  form_fills AS (

    SELECT 
      _email,
      _domain,
      _timestamp,
      _week,  
      _year,
      _form_title,
      'Form Filled' AS _engagement,
      _description,
      _utmsource,
      _utmcampaign,
      _utmmedium,
      _utmcontent,
      _fullurl,
      CAST(NULL AS INT64) AS _frequency,
    FROM 
      `3x.db_form_fill_log`

),website_subsribe AS (
    SELECT DISTINCT 
    property_email.value,
    COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain, 
    TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))) AS _date,
    EXTRACT( WEEK FROM TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))) ) AS _week ,
    EXTRACT( YEAR FROM TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))) ) AS _year,
    "Subsribe Newsletter" AS _type,
    'Website' AS _engagement,
    _engagement AS _description,
    CAST(NULL AS STRING) AS _utmsource,
    CAST(NULL AS STRING) AS _utmcampaign,
    property_utm_medium.value  AS _utmmedium,
    property_utm_content.value  AS _utmcontent,
    CAST(NULL AS STRING) AS _fullurl,
    CAST(NULL  AS INT64) AS _frequency,
    FROM `x-marketing.x3x_hubspot.contacts`, UNNEST(SPLIT(property_engagement.value, ';') ) AS _engagement
    WHERE _engagement IN (SELECT DISTINCT
    _engagement AS event
    FROM `x-marketing.x3x_hubspot.contacts`, UNNEST(SPLIT(property_engagement.value, ';') ) AS _engagement
    WHERE _engagement  IN ("Subscribed"))
), event_engagement  AS (
  SELECT 
  DISTINCT 
  property_email.value,
  COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain, 
  TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))),
  EXTRACT( WEEK FROM TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))) ) AS _week ,
  EXTRACT( YEAR FROM TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))) ) AS _year,
  "Event Engagement" AS form_title,
  'Events' AS _engagement,
  _engagement AS _description,
  CAST(NULL AS STRING) AS _utmsource,
  CAST(NULL AS STRING) AS _utmcampaign,
  property_utm_medium.value  AS _utmmedium,
  property_utm_content.value  AS _utmcontent,
  CAST(NULL AS STRING) AS _fullurl,
  CAST(NULL  AS INT64) AS _frequency,
  FROM `x-marketing.x3x_hubspot.contacts`, UNNEST(SPLIT(property_engagement.value, ';') ) AS _engagement
  WHERE _engagement IN (SELECT DISTINCT 
  _engagement AS event 
  FROM `x-marketing.x3x_hubspot.contacts`,
  UNNEST(SPLIT(property_engagement.value, ';') ) AS _engagement
   WHERE _engagement LIKE "%Events%")

), webinar_engagement AS  (
  SELECT 
  DISTINCT 
  property_email.value,
  COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain, 
  TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))),
  EXTRACT( WEEK FROM TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))) ) AS _week ,
  EXTRACT( YEAR FROM TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(property_timestamp.value AS INT64) / 1000 AS INT64))) ) AS _year,
  "Webinar Engagement" AS form_title,
  'Webinar' AS _engagement,
  _engagement AS _description,
  CAST(NULL AS STRING) AS _utmsource,
  CAST(NULL AS STRING) AS _utmcampaign,
  property_utm_medium.value  AS _utmmedium,
  property_utm_content.value  AS _utmcontent,
  CAST(NULL AS STRING) AS _fullurl,
  CAST(NULL  AS INT64) AS _frequency,
  FROM `x-marketing.x3x_hubspot.contacts`, UNNEST(SPLIT(property_engagement.value, ';') ) AS _engagement
  WHERE _engagement IN (SELECT DISTINCT
  _engagement AS event 
  FROM `x-marketing.x3x_hubspot.contacts`,
  UNNEST(SPLIT(property_engagement.value, ';') ) AS _engagement
   WHERE _engagement LIKE "%Webinars%")
) ,
  dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements

    SELECT
      _date,
      EXTRACT(WEEK FROM _date) AS _week,
      EXTRACT(YEAR FROM _date) AS _year
    FROM 
      UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 

  ),
  bombora_report AS (

    SELECT
      CAST(NULL AS STRING) AS _email, 
      _domain,
      _date,
      EXTRACT(WEEK FROM _date) AS _week, 
      EXTRACT(YEAR FROM _date) AS _year,
      CAST(NULL AS STRING) AS _pageName,
      "Bombora Report" AS _engagement,
      STRING_AGG(CONCAT(_topicname, " - ", _compositescore), "\n") OVER(PARTITION BY _domain, EXTRACT(WEEK FROM _date)) AS _description,
      CAST(NULL AS STRING) AS _utmsource,
      CAST(NULL AS STRING) AS _utmcampaign,
      CAST(NULL AS STRING) AS _utmmedium,
      CAST(NULL AS STRING) AS _utmcontent,
      CAST(NULL AS STRING) AS _fullurl,
      CAST(NULL AS INT64) AS _frequency,
      ROUND(AVG(COALESCE(_compositescore, 0)) OVER(PARTITION BY _domain, _date), 0, "ROUND_HALF_AWAY_FROM_ZERO") AS _avg_bombora_score,
    FROM (
      SELECT 
          DISTINCT TIMESTAMP(_date) AS _date, 
          _domain, 
          _companyname, 
          _intentcountry,
          COALESCE(_topicname, NULL) AS _topicname, 
          COALESCE(CAST(_compositescore AS NUMERIC), 0) AS _compositescore,
          COALESCE(_compositescoredelta, NULL) AS _compositescoredelta
      FROM 
        `x-marketing.webtrack_ipcompany.db_bombora_2x_3x_report` 
      )

    ),
  account_scores AS (

    SELECT
      *,
      EXTRACT(WEEK FROM _extract_date) AS _week,
      EXTRACT(YEAR FROM _extract_date) AS _year,
      (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score , 0)+ COALESCE(_quarterly_organic_social_score , 0)+ COALESCE(_quarterly_form_fill_score , 0)+ COALESCE(_quarterly_paid_ads_score , 0)+ COALESCE(_quarterly_web_score, 0) + COALESCE(_quarterly_6sense_score, 0) + COALESCE(_quarterly_event_score,0) + COALESCE(_quarterly_webinar_score,0) ) AS _account_90days_score
    FROM 
      x-marketing.3x.account_90days_score_new
    ORDER BY
      _extract_date DESC

  ),/* 
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
      `3x.contact_engagement_scoring` 
    ORDER BY 
      _week DESC

  ),  */
  account_engagements AS (

    SELECT 
      DISTINCT /* dummy_dates.*, */
      engagements.* /* EXCEPT(_date , _week, _year) */,
      accounts.*EXCEPT(_domain, _email)
    FROM
      /* dummy_dates
    JOIN */
      (
        SELECT * FROM bombora_report UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM web_views UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM ad_clicks UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM content_engagement UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM ad_6sense_webeng_trend UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM ad_6sense_clicks UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM opps_combined UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM linkedin_click
      ) engagements /* USING(_week, _year) */
    LEFT JOIN
      accounts USING(_domain)

  ),
  contact_engagements AS (

    SELECT 
      DISTINCT /* dummy_dates.*, */
      engagements.* /* EXCEPT(_date, _week, _year) */,
      contacts.*EXCEPT(_domain, _email)
    FROM
      /* dummy_dates
    JOIN */
      (
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM email_engagement 
        UNION ALL
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM form_fills
         UNION ALL
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM website_subsribe
        UNION ALL 
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM  event_engagement 
        UNION ALL 
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score  FROM webinar_engagement
      ) engagements /* USING(_week, _year) */
    LEFT JOIN
      contacts USING(_email)

  ),
  consolidated_engagement AS (

    SELECT * FROM contact_engagements
    UNION ALL
    SELECT * FROM account_engagements
    
  )
SELECT 
  DISTINCT consolidated_engagement.*EXCEPT(_avg_bombora_score), 
  COALESCE(consolidated_engagement._avg_bombora_score,b_scores._avg_bombora_score) AS _avg_bombora_score,
  latest_bombora._avg_bombora_score AS _latest_avg_bombora_score,
  account_scores. _account_90days_score,
  latest_score._account_90days_score AS _latest_account_score,
  /* COALESCE(_weekly_first_party_score, 0) AS _weekly_first_party_score, 
  COALESCE(_ytd_first_party_score, 0) AS _ytd_first_party_score, 
  engagement_grade._weekly_contact_score, 
  engagement_grade._ytd_contact_score,
  engagement_grade._ytd_grade */
FROM 
  consolidated_engagement
LEFT JOIN 
  account_scores 
    ON consolidated_engagement._domain = account_scores._domain AND DATE(consolidated_engagement._timestamp) = account_scores._extract_date
LEFT JOIN 
  account_scores AS latest_score 
    ON latest_score._domain =  consolidated_engagement._domain AND latest_score._week = EXTRACT(WEEK FROM CURRENT_DATE())
LEFT JOIN
  (SELECT DISTINCT _domain, _avg_bombora_score, _week, _year, ROW_NUMBER() OVER(PARTITION BY _domain, _year, _week) AS _order FROM bombora_report) b_scores 
    ON b_scores._order = 1 
      AND b_scores._domain = consolidated_engagement._domain 
      AND b_scores._week = consolidated_engagement._week
      AND b_scores._year = consolidated_engagement._year  
LEFT JOIN
  (SELECT DISTINCT _domain, _avg_bombora_score, ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _date DESC) AS _order FROM bombora_report ) AS latest_bombora 
    ON latest_bombora._domain =  consolidated_engagement._domain AND latest_bombora._order = 1
WHERE
  LENGTH(consolidated_engagement._domain) > 1
  AND consolidated_engagement._year = 2023
  AND consolidated_engagement._domain NOT LIKE '%pubpng.com%'
  AND  consolidated_engagement._domain NOT LIKE '%2x.marketing%'
  ---AND LOWER(consolidated_engagement._country) IN ('united states', 'us')
  ---AND consolidated_engagement._domain = '2u.com'
ORDER BY 
  _week DESC
;