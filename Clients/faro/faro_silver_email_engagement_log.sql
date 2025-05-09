-- TRUNCATE TABLE `x-marketing.faro.silver_email_engagement_log`;
-- INSERT INTO `x-marketing.faro.silver_email_engagement_log`
CREATE OR REPLACE TABLE `x-marketing.faro.silver_email_engagement_log` AS
WITH market_segment AS (
  SELECT 
    leadorcontactid, 
    pull_market_segment__c
  FROM `faro_salesforce.CampaignMember` main
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadorcontactid, pull_market_segment__c ORDER BY lastmodifieddate DESC) = 1
),
prospect_info AS (
  SELECT
    contact._email,
    contact._prospect_name AS _name,
    contact._title,
    contact._seniority AS _seniority,
    contact._company,
    contact._industry,
    contact._revenue_range AS _revenuerange,
    contact._employees,
    contact._city,
    contact._state,
    contact._country,
    contact._ownerid,
    contact._lead_source,
    contact._division_region,
    contact._initial_opt_in,
    contact._prospect_type,
    market_segment.pull_market_segment__c AS _market_segment
  FROM `x-marketing.faro.silver_contact_log` contact
  LEFT JOIN market_segment ON contact._prospect_id = market_segment.leadorcontactid 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY contact._email ORDER BY contact._email DESC) = 1
),
sent_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Sent' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name = 'Email'
    AND activity.type = 6   /* Sent */
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
opened_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Opened' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name = 'Email'
    AND activity.type = 11   /* Open */
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
clicked_email_cta AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Clicked' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name IN ('Email', 'Email Tracker')
    AND activity.type = 1   /* Click */
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
bounced_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Bounced' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name = 'Email'
    AND activity.type IN (13, 36)  /* Bounced / Indirect Bounce */
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
clicked_email_opt_out AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Unsubscribed' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id,
    'Clicked' AS type
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name IN ('Email', 'Email Tracker')
    AND activity.type = 1   /* Click */
    AND LOWER(details) LIKE '%opt%out%' 
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
opt_out_form_fill AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Unsubscribed' AS _engagement,
    CAST(NULL AS STRING) AS _email_id,
    CAST(NULL AS INT64) AS email_template_id,
    'Form Filled' AS type
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name IN ('Form', 'Form Handler')
    AND activity.type = 4   /* Download */
    AND form_handler_id = 8822
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
unsubscribed_email_form_fill AS (
  SELECT 
    * EXCEPT(type, next_type, next_type_timestamp)
  FROM (
    SELECT
      *,
      LEAD(type) OVER(
        PARTITION BY _prospectID
        ORDER BY _sdc_sequence
      ) AS next_type,
      LEAD(_timestamp) OVER(
        PARTITION BY _prospectID
        ORDER BY _sdc_sequence
      ) AS next_type_timestamp
    FROM ( 
      SELECT * FROM clicked_email_opt_out 
      UNION ALL
      SELECT * FROM opt_out_form_fill 
    )
    ORDER BY _sdc_sequence
  )
  WHERE type = 'Clicked'
    AND next_type = 'Form Filled'
),
unsubscribed_email_non_form_fill AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Unsubscribed' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name = 'Email'
    AND activity.type IN (12, 35)   /* Unsubscribe Page / Indirect Unsubscribe Open */
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
downloaded_email AS ( 
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Downloaded' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name IN ('Form', 'Form Handler')
    AND activity.type = 4   /* Download */
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
spam_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Spam' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id
  FROM `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.faro_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.faro_pardot.campaigns` campaign
    ON activity.campaign_id = campaign.id
  WHERE activity.type_name = 'Email'
    AND activity.type = 14   /* SPAM COMPLAINT */
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY activity.prospect_id, activity.email_template_id
    ORDER BY activity.created_at DESC
  ) = 1
),
campaign_sent_date AS ( #added since the airtable isnt updated
  SELECT DISTINCT 
    email_template_id, 
    EXTRACT(DATE FROM MIN(_timestamp)) AS _email_sent_date 
  FROM sent_email 
  GROUP BY 1
)
SELECT
  engagements.*,
  campaign_sent_date._email_sent_date,
  prospect_info.* EXCEPT(_email),
FROM (
  SELECT * FROM sent_email
  UNION ALL
  SELECT * FROM opened_email
  UNION ALL
  SELECT * FROM clicked_email_cta
  UNION ALL
  SELECT * FROM downloaded_email
  UNION ALL
  SELECT * FROM unsubscribed_email_form_fill
  UNION ALL
  SELECT * FROM unsubscribed_email_non_form_fill
  UNION ALL
  SELECT * FROM bounced_email
  UNION ALL
  SELECT * FROM spam_email
) AS engagements
LEFT JOIN prospect_info 
  ON engagements._email = prospect_info._email
LEFT JOIN campaign_sent_date
  ON engagements.email_template_id = campaign_sent_date.email_template_id;





---------------------------------------------------------------------------
--------------------------- ACCOUNT ENGAGEMENTS ---------------------------
---------------------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.faro.db_account_engagements` AS 
WITH 
#Query to pull all the contacts in the leads table from Marketo
tam_contacts AS (
  SELECT * EXCEPT(_rownum) 
  FROM (
    SELECT DISTINCT
        COALESCE(crm_contact_fid, crm_lead_fid) AS _leadorcontactid,
        CASE 
          WHEN crm_contact_fid IS NOT NULL THEN "Contact"
          WHEN crm_contact_fid IS NULL THEN "Lead"
        END AS _contact_type,
        first_name AS _firstname, 
        last_name AS _lastname, 
        job_title AS _title, 
        CAST(NULL AS STRING) AS _2xseniority,
        email AS _email,
        CAST(NULL AS STRING) AS _accountid,
        RIGHT(email, LENGTH(email)-STRPOS(email,'@')) AS _domain, 
        company AS _accountname, 
        industry AS _industry, 
        CAST(NULL AS STRING) AS _tier,
        COALESCE(annual_revenue, CAST(NULL AS STRING)) AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY email 
            ORDER BY prosp._sdc_received_at DESC
        ) _rownum
    FROM 
      `faro_pardot.prospects` prosp
    -- LEFT JOIN
    --    `faro_mysql.w_routables` main ON main._email = prosp.email
    WHERE 
      NOT REGEXP_CONTAINS(email, 'faro|2x.marketing') 
  )
  WHERE _rownum = 1
),
#Query to pull the email engagement 
email_engagement AS (
    SELECT * 
    FROM ( 
      SELECT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description
      FROM 
        (SELECT * FROM `faro.db_campaign_analysis`)
      WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ LOWER(_engagement) NOT IN ('sent','delivered', 'bounced', 'unsubscribed')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|faro|gmail|yahoo|outlook|hotmail') 
      AND NOT REGEXP_CONTAINS(_contentTitle, 'test')
      AND _domain IS NOT NULL
      -- AND _year = 2022
    ORDER BY 1, 3 DESC, 2 DESC
),
web_views AS (
  SELECT 
    CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _pageName, 
    "Web Visit" AS _engagement, 
    CAST(_engagementtime AS STRING) AS _description,
  FROM 
    `faro.web_metrics` web 
  WHERE 
    NOT REGEXP_CONTAINS(_page, 'Unsubscribe')
    AND NOT REGEXP_CONTAINS(LOWER(_utmsource), 'linkedin|google|email') 
    AND (_domain IS NOT NULL AND _domain != '')
  UNION ALL 
  SELECT 
    CAST('' AS STRING) AS _email,
    CASE 
      WHEN domain IS NULL  
      THEN company 
      ELSE domain 
    END  AS _domain,
    CAST(extractDate AS DATETIME) AS _date,   
    EXTRACT(WEEK FROM extractDate) AS _week,  
    EXTRACT(YEAR FROM extractDate) AS _year,    
    '' AS _pageName, 
    "Web Visit" AS _engagement, 
    CAST(websiteEngagement AS STRING) AS _description 
    FROM `x-marketing.faro_6sense.db_reached_accounts` main
  WHERE websiteEngagement IN ('New','Increased') AND  
    (domain IS NOT NULL AND domain != '')
  ORDER BY 
    _date DESC
  
),
ad_clicks AS (
  SELECT 
    CAST('' AS STRING) AS _email,
    CASE 
      WHEN domain IS NULL  
      THEN company 
      ELSE domain 
    END  AS _domain,
    CAST(extractDate AS DATETIME) AS _date,   
    EXTRACT(WEEK FROM extractDate) AS _week,  
    EXTRACT(YEAR FROM extractDate) AS _year,    
    '' AS _pageName, 
    "Ad Clicks" AS _engagement, 
    CAST(clicks AS STRING) AS _description 
    FROM `x-marketing.faro_6sense.db_reached_accounts` main
  WHERE clicks > 0 AND  
    (domain IS NOT NULL AND domain != '')
  ORDER BY 
    CAST(extractDate AS TIMESTAMP) DESC
),
content_engagement AS (
  SELECT 
    CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _pageName, 
    "Content Engagement" AS _engagement, 
    _page AS _description
  FROM 
    faro.web_metrics web 
  WHERE 
    NOT REGEXP_CONTAINS(_page, 'Unsubscribe')
    AND REGEXP_CONTAINS(LOWER(_page), 'blog|commid=')
    AND (_domain IS NOT NULL AND _domain != '')
  ORDER BY 
    _date DESC
),
form_fills AS (
    SELECT 
      DISTINCT email AS _email, 
      RIGHT(email, LENGTH(email)-STRPOS(email, '@')) AS _domain,
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _formTitle, 
      INITCAP(_engagement) AS _engagement,
      referrer_url AS _description
    FROM ( 
        SELECT
          DISTINCT email,
          COALESCE(form_id, campaign_id) AS _campaignid,
          activities.created_at AS _timestamp,
          details AS _formTitle,
          'form filled' AS _engagement,
          pages.url AS referrer_url,
          ROW_NUMBER() OVER(
              PARTITION BY activities.prospect_id, COALESCE(form_id, campaign_id) 
              ORDER BY activities.created_at DESC
          ) AS rownum
        FROM
          `faro_pardot.visitor_activities` activities
        LEFT JOIN
          (SELECT visitor_id, page.value.url FROM `faro_pardot.visits`, UNNEST(visitor_page_views.visitor_page_view) AS page ) pages USING(visitor_id)
        LEFT JOIN
          ( SELECT DISTINCT id, email FROM `faro_pardot.prospects` ) contacts ON contacts.id = activities.prospect_id
        WHERE 
          type = 4
          AND type_name LIKE 'Form%'
          AND NOT REGEXP_CONTAINS(LOWER(details),'unsubscribe|become a partner|test')
          
    ) A 
    WHERE 
      rownum = 1
),
dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
),
#Combining the engagements - Contact based and account based engagements
contact_engagement AS (
#Contact based engagement query
  SELECT 
    DISTINCT 
    tam_contacts._domain, 
    tam_contacts._email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    -- CAST(NULL AS INTEGER) AS _avg_bombora_score,
    tam_contacts.*EXCEPT(_domain, _email),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    SELECT * FROM email_engagement UNION ALL
    SELECT * FROM form_fills
  ) engagements USING(_week, _year)
  JOIN
    tam_contacts USING(_email) 
),
account_engagement AS (
#Account based engagement query
   SELECT 
    DISTINCT 
    tam_accounts._domain, 
    CAST(NULL AS STRING) AS _email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    CAST(NULL AS STRING) AS _id, 
    CAST(NULL AS STRING) AS _contact_type,
    CAST(NULL AS STRING) AS _firstname, 
    CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _2xseniority,
    tam_accounts.*EXCEPT(_domain),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    /* SELECT * FROM intent_score UNION ALL */
    SELECT * FROM web_views UNION ALL
    SELECT * FROM ad_clicks UNION ALL
    SELECT * FROM content_engagement
  ) engagements USING(_week, _year)
  JOIN
    (
      SELECT 
        DISTINCT _domain, 
        _accountid, 
        _accountname, 
        _industry, 
        _tier, 
        _annualrevenue 
      FROM 
        tam_contacts
    ) tam_accounts
    USING(_domain)
),
combined_engagements AS (
  SELECT * FROM contact_engagement
  UNION DISTINCT
  SELECT * FROM account_engagement
)
SELECT 
  DISTINCT
  _domain,
  _accountid,
  _date,
  SUM(IF(_engagement = 'Email Opened', 1, 0)) AS _emailOpens,
  SUM(IF(_engagement = 'Email Clicked', 1, 0)) AS _emailClicks,
  SUM(IF(_engagement = 'Email Downloaded', 1, 0)) AS _emailDownloads,
  SUM(IF(_engagement = 'Form Filled', 1, 0)) AS _gatedForms,
  SUM(IF(_engagement = 'Web Visit', 1, 0)) AS _webVisits,
  SUM(IF(_engagement = 'Ad Clicks', 1, 0)) AS _adClicks,
FROM combined_engagements
GROUP BY 1, 2, 3
ORDER BY _date DESC;