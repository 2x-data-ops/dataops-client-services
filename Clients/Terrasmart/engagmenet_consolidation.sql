------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

#This is the script to power the Account Health reporting page



-- CREATE OR REPLACE TABLE terrasmart.db_consolidated_engagements_log
TRUNCATE TABLE `terrasmart.db_consolidated_engagements_log`;
INSERT INTO `terrasmart.db_consolidated_engagements_log`
WITH 
#Query to pull all the contacts in the leads table from Marketo
  contacts AS (
    SELECT 
      _id,
      _email,
      _name,
      _domain, 
      _jobtitle,
      _seniority,
      _function,
      _phone,
      _company,
      _revenue,
      _industry,
      _city, 
      _state,
      _country,
      _persona,
      _lifecycleStage,
      _sfdcaccountid,
      _sfdccontactid,
    FROM 
      `terrasmart.db_icp_database_log` tam
  ),
  accounts AS (
      SELECT 
        DISTINCT _domain, 
        CAST(NULL AS INT64) AS _id,
        CAST(NULL AS STRING) AS _name, 
        -- CAST(NULL AS STRING) AS _lastname,
        CAST(NULL AS STRING) AS _title,
        CAST(NULL AS STRING) AS _seniority,
        MAX(_phone) AS _phone,
        MIN(_company) AS _company, 
        MAX(_revenue) AS _revenue,
        MAX(_industry) AS _industry, 
        MAX(_city) AS _city,
        MAX(_state) AS _state,
        MAX(_country) AS _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        CAST(NULL AS STRING) _sfdcaccountid,
        CAST(NULL AS STRING) _sfdccontactid,
        -- ROW_NUMBER() OVER(PARTITION BY _sfdcaccountid ORDER BY lastactivitydate DESC) AS _order
      FROM 
        contacts
      WHERE
        _country IS NOT NULL
        OR _industry IS NOT NULL
        OR _company IS NOT NULL
        OR _revenue IS NOT NULL
        OR _phone IS NOT NULL
        OR _city IS NOT NULL
        OR _state IS NOT NULL
      GROUP BY 
        _domain
      ORDER BY
        _domain
      /* RIGHT JOIN
        `terrasmart_salesforce.Account` sfdc ON contacts._sfdcaccountid = sfdc.id */
  ),
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
        _utmcampaign AS _contentTitle, 
        CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
        _description
        FROM 
          (SELECT * FROM `terrasmart.db_email_engagements_log`)
        WHERE 
          /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
          AND */ LOWER(_engagement) NOT IN ('sent','delivered', 'downloaded', 'bounced', 'unsubscribed', 'processed', 'deffered', 'spam', 'suppressed', 'dropped')
      ) a
      WHERE 
        NOT REGEXP_CONTAINS(_domain,'2x.marketing|terrasmart') 
        AND _domain IS NOT NULL 
      ORDER BY 
        1, 3 DESC, 2 DESC
  ),
  /*web_engagements AS (
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
    FROM
      `x-marketing.terrasmart.db_web_engagements_log`
    WHERE 
      NOT REGEXP_CONTAINS(LOWER(_fullurl), 'unsubscribe')
      AND NOT REGEXP_CONTAINS(LOWER(_fullurl), '=linkedin|=google|=6sense')
    ORDER BY 
      _domain, _timestamp DESC
  ),
  ad_clicks AS (
    SELECT 
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
      `x-marketing.terrasmart.db_web_engagements_log`
    WHERE 
      NOT REGEXP_CONTAINS(LOWER(_fullurl), 'unsubscribe')
      AND REGEXP_CONTAINS(LOWER(_fullurl), '=linkedin|=google|=6sense')
  ),*/
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
    FROM 
      `x-marketing.terrasmart.db_content_engagements_log`
    /* WHERE 
      REGEXP_CONTAINS(LOWER(_page), '/blog/') */
  ),
  -- engagement_6sense AS (
  -- -- Consolidation of all the 6Sense engagements based on the email alert & campaigns account table
  --   WITH
  --     main AS (
  --       SELECT 
  --         DISTINCT *EXCEPT(_timeframe, _is6qa), 
  --         CAST(_is6qa AS BOOL) AS _is6qa, 
  --         PARSE_DATE('%h %d, %Y', _timeframe) AS _timeframe 
  --       FROM 
  --         `x-marketing.terrasmart_mysql.db_6qa_alert`
  --     ),
  --     became_6qa AS (
  --       SELECT 
  --         DISTINCT
  --         _domain,
  --         "Became 6QA" AS _description, 
  --         "6Sense 6QA" AS _engagement,
  --         1 AS _notes,
  --         MIN(_timeframe) OVER(PARTITION BY _domain) AS _timestamp
  --       FROM
  --         main
  --       WHERE
  --         _is6qa = true
  --     ),
  --     web_activities AS (
  --       SELECT
  --         DISTINCT _domain,
  --         SPLIT(weburls, " (")[OFFSET(0)] AS _weburls,
  --         "6Sense Web Visits" AS _engagement,
  --         IF(REGEXP_CONTAINS(weburls, r'\(' ), 
  --             CAST(REGEXP_EXTRACT(weburls, r'\((\d+)\)') AS INTEGER),
  --             1
  --         ) AS _webvisitcount,
  --         _timeframe AS _timestamp
  --         -- CONCAT(_country, _accountname) AS _country_account
  --       FROM
  --         main,
  --         UNNEST(SPLIT(_weburls, ", ")) AS weburls
  --       WHERE
  --         LENGTH(_weburls) > 1
  --     ),
  --     surging_keywords AS (
  --       SELECT
  --         DISTINCT _domain,
  --         SPLIT(keywords, " (")[OFFSET(0)] AS _description,
  --         "6Sense Keywords" AS _engagement,
  --         CAST(REGEXP_EXTRACT(keywords, r'\((\d+)\)') AS INTEGER) AS _notes,
  --         _timeframe AS _timestamp,
  --         -- CONCAT(_country, _accountname) AS _country_account
  --       FROM
  --         main,
  --         UNNEST(SPLIT(_keywords, ", ")) AS keywords
  --       WHERE
  --         LENGTH(_keywords) > 1
  --       ORDER BY 
  --         _domain, _timeframe DESC
  --     ) /*, -- Campaign reached data is missing
  --      ad_click AS (
  --       SELECT 
  --         DISTINCT domain AS _domain,
  --         campaignName AS _description,
  --         "6Sense Ad Clicks" AS _engagement,
  --         clicks AS _notes,
  --         extractDate AS _timestamp
  --       FROM
  --         `tecsys_6sense.tecsys_db_reached_accounts2`
  --       ORDER BY 
  --         _domain, _timestamp DESC 
  --     ),
  --     influenced_formfill AS (
  --       SELECT 
  --         DISTINCT domain AS _domain,
  --         campaignName AS _description,
  --         "6Sense Influenced Form Fill" AS _engagement,
  --         influencedFormFills AS _notes,
  --         extractDate AS _timestamp
  --       FROM
  --         `tecsys_6sense.tecsys_db_reached_accounts2`
  --       ORDER BY 
  --         _domain, _timestamp DESC 
  --     ), 
  --     reached_account AS (
  --       SELECT 
  --         DISTINCT domain AS _domain,
  --         campaignName AS _description,
  --         "6Sense Campaign Reached" AS _engagement,
  --         1 AS _notes,
  --         MIN(extractDate) OVER(PARTITION BY domain, campaignName ORDER BY extractDate) AS _timestamp
  --       FROM
  --         `tecsys_6sense.tecsys_db_reached_accounts2`
  --       ORDER BY 
  --         _domain, _timestamp DESC 
  --     ) */
  --   SELECT 
  --     DISTINCT CAST(NULL AS STRING) AS _email, 
  --     _domain, 
  --     _timestamp, 
  --     EXTRACT(WEEK FROM _timestamp) AS _week,  
  --     EXTRACT(YEAR FROM _timestamp) _year, 
  --     _description, 
  --     _engagement, 
  --     CAST(_notes AS STRING) AS _notes
  --   FROM
  --   (
  --     SELECT * FROM surging_keywords
  --     UNION DISTINCT
  --     SELECT * FROM web_activities
  --     UNION DISTINCT
  --     SELECT * FROM became_6qa
  --     /* UNION DISTINCT
  --     SELECT * FROM ad_click
  --     UNION DISTINCT
  --     SELECT * FROM influenced_formfill
  --     UNION DISTINCT
  --     SELECT * FROM reached_account */
  --   )
  -- ),
  form_fills AS (
    SELECT 
      *
    FROM (
      SELECT 
        _email,
        _domain,
        _timestamp,
        EXTRACT(WEEK FROM _timestamp) AS _week,  
        EXTRACT(YEAR FROM _timestamp) AS _year,
        _form_title,
        'Form Filled' AS _engagement,
        _description,
      FROM (
          SELECT
            CAST(NULL AS STRING) AS devicetype,
            _utmcontent, #utm_content
            _utmcampaign,
            _utmsource,
            _form_title,
            _email, 
            _domain,
            _timestamp, 
            _engagement,
            _description,
          FROM 
            `terrasmart.db_form_fill_log`
          ) activity
      LEFT JOIN 
        `x-marketing.terrasmart_pardot.campaigns` campaign ON activity._utmcontent = CAST(campaign.id AS STRING)
    )
  ),
  dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
    SELECT
      _date,
      EXTRACT(WEEK FROM _date) AS _week,
      EXTRACT(YEAR FROM _date) AS _year
    FROM 
      UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 WEEK)) AS _date 
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
            `terrasmart.bombora_surge_report` report
      )
  ),  */
  first_party_score AS (
    SELECT 
      DISTINCT 
      _domain, 
      _extract_date,
      EXTRACT(WEEK FROM _extract_date) -1 AS _week,  -- Minus 1 as the score is referring to the week before.
      EXTRACT(YEAR FROM _extract_date) AS _year,
      (COALESCE(_quarterly_email_score, 0) + 
      /*COALESCE(_content_synd_score , 0)+ COALESCE(_organic_social_score , 0)+ COALESCE(_form_fill_score , 0)+ COALESCE(_quarterly_web_score, 0)*/
      COALESCE(_quarterly_ads_score, 0)) AS _t90_days_score
    FROM
    `terrasmart.account_90days_score`
    ORDER BY
      _extract_date DESC
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
      `terrasmart.contact_engagement_scoring` 
    ORDER BY 
      _week DESC */
  ),
  # Combining the engagements - Contact based and account based engagements
  engagements AS (
  # Contact based engagement query
    SELECT 
      DISTINCT contacts._domain, 
      contacts._email,
      dummy_dates.*EXCEPT(_date), 
      engagements.*EXCEPT(_date, _week, _year, _domain, _email),
      CAST(NULL AS INTEGER) AS _avg_bombora_score,
      contacts.*EXCEPT(_domain, _email, _function),
      engagements._date
    FROM 
      dummy_dates
    JOIN (
      SELECT * FROM email_engagement UNION ALL
      SELECT * FROM form_fills
    ) engagements USING(_week, _year)
    RIGHT JOIN
      contacts USING(_email) 
    UNION DISTINCT
  # Account based engagement query
    SELECT 
      DISTINCT accounts._domain, 
      CAST(NULL AS STRING) AS _email,
      dummy_dates.*EXCEPT(_date), 
      engagements.*EXCEPT(_date, _week, _year, _domain, _email),
      accounts.*EXCEPT(_domain),
      CAST(engagements._date AS TIMESTAMP) AS _timestamp
    FROM 
      dummy_dates
    CROSS JOIN
      accounts
    JOIN (
      -- SELECT * FROM intent_score UNION ALL
      -- SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM web_engagements UNION ALL
      -- SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM ad_clicks UNION ALL
      SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM content_engagement
    ) engagements USING(_domain, _week, _year)
  /* ),
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
      cnt.mailingcountrycode AS _country
    FROM 
      `terrasmart_salesforce.Contact` cnt
    JOIN
      `terrasmart_salesforce.Account` acc ON cnt.accountid = acc.id
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
      `terrasmart_salesforce.Opportunity` main
      
    JOIN
      sfdc USING(accountid)
    WHERE
      main.isdeleted = False
      AND main.type !='Renewal'
      AND LOWER(_accountname) NOT LIKE '%terrasmart%'
      AND EXTRACT(YEAR FROM main.createddate ) IN (2022, 2023)
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
        `terrasmart_salesforce.OpportunityFieldHistory`
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
   */
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
  first_party_score USING(_domain, _week, _year)
WHERE
  LENGTH(_domain) > 1
/* UNION DISTINCT
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
  opps_stage_change
 */;

