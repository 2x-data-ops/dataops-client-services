TRUNCATE TABLE `x-marketing.bluemantis.db_6sense_engagement_log`;
INSERT INTO `x-marketing.bluemantis.db_6sense_engagement_log` (
  _6sense_company_name,
  _6sense_country,
  _6sense_domain,
  _6sense_industry,
  _6sense_employee_range,
  _6sense_revenue_range,
  _added_on,
  _country_account,
  _first_impressions,
  _website_engagement,
  _email,
  _timestamp,
  _engagement,
  _engagement_data_source,
  _description,
  _notes,
  _total_6sense_campaign_reached,
  _total_6sense_ad_clicks,
  _total_searched_keywords,
  _total_web_visits,
  _total_email_opens,
  _total_form_submission
)
-- Get all target accounts and their unique info
WITH target_accounts AS (
  SELECT 
    * 
  FROM `x-marketing.bluemantis.db_6sense_account_current_state`
),
-- Prep the reached account data for use later
optimization_airtable_ads_6sense AS (
  SELECT DISTINCT 
    CAST(_campaign_id AS STRING) AS _campaign_id, 
    _campaign_name
  FROM `x-marketing.bluemantis_google_sheets.db_ads_optimization`
),
reached_accounts_data AS (
  SELECT DISTINCT 
    CAST(main._clicks AS INTEGER) AS _clicks,
    CAST(main._influencedformfills AS INTEGER) AS _influenced_form_fills,
    CASE 
      WHEN main._latestimpression LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', main._latestimpression)
      ELSE PARSE_DATE('%F', main._latestimpression)
    END AS _latest_impression, 
    CASE 
      WHEN main._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
      ELSE PARSE_DATE('%F', main._extractdate)
    END AS _activities_on, 
    main._campaignid AS _campaign_id,
    -- Need label to distingush 6sense and Linkedin campaigns
    side._campaign_name,
    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
  FROM `x-marketing.bluemantis_mysql.db_campaign_accounts_reached` main
  JOIN optimization_airtable_ads_6sense side
    ON main._campaignid = side._campaign_id
),
-- Get campaign reached engagements
campaign_reached AS (
  SELECT DISTINCT 
    _country_account, 
    CAST(NULL AS STRING) AS _email,
    MIN(_latest_impression) OVER(
        PARTITION BY _country_account, _campaign_name
        ORDER BY _latest_impression
    ) AS _timestamp,
    '6sense Campaign Reached' AS _engagement,
    '6sense' AS _engagement_data_source, 
    _campaign_name AS _description, 
    1 AS _notes
  FROM reached_accounts_data
),
-- Get ad clicks engagements
ad_clicks AS (
  SELECT DISTINCT 
    _country_account,
    CAST(NULL AS STRING) AS _email,  
    _activities_on AS _timestamp,
    '6sense Ad Clicks' AS _engagement, 
    '6sense' AS _engagement_data_source,
    _campaign_name AS _description,  
    _clicks AS _notes
  FROM reached_accounts_data
  WHERE _clicks >= 1
  -- Get those who have increased in numbers from the last period
  QUALIFY (_clicks - COALESCE(LAG(_clicks) OVER (
    PARTITION BY _country_account, _campaign_name
    ORDER BY _activities_on), 0)) >= 1
),
-- Get form fills engagements
influenced_form_fills AS (
  SELECT DISTINCT 
    _country_account, 
    CAST(NULL AS STRING) AS _email, 
    _activities_on AS _timestamp,
    '6sense Influenced Form Filled' AS _engagement,
    '6sense' AS _engagement_data_source,
    _campaign_name AS _description,  
    _influenced_form_fills AS _notes
  FROM reached_accounts_data 
  WHERE _influenced_form_fills >= 1
  QUALIFY (_influenced_form_fills - COALESCE(LAG(_influenced_form_fills) OVER(
    PARTITION BY _country_account, _campaign_name
    ORDER BY _activities_on), 0)) >= 1
),
sales_intelligence_activities AS (
  SELECT
    act.name AS _account_name,
    sales._websiteaddress AS _domain,
    sales._contactcountry AS _country,
    sales._contactname AS _contact_name,
    sales._email,
    sales._activitytype AS _activity_type,
    sales._activitytarget AS _activity_target,
    sales._activitydate AS _activity_date
  FROM `x-marketing.bluemantis_mysql.db_sales_intelligence_update` sales
  LEFT JOIN `x-marketing.bluemantis_salesforce.Account` act
    ON REPLACE(sales._crmaccountid, 'CMA', '') = act.id
),
-- Prep the sales intelligence data for use later
sales_intelligence_data AS (
  SELECT 
    side._activity_type,
    side._activity_target,
    side._contact_name,
    side._email,
    CASE 
      WHEN side._activity_date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', side._activity_date)
      WHEN side._activity_date = '' THEN CAST(NULL AS DATE)
      ELSE PARSE_DATE('%F', side._activity_date)
    END AS _date,
    main._country_account,
    COUNT(*) AS _count
  FROM target_accounts AS main
  JOIN sales_intelligence_activities side
    ON main._6sense_domain = side._domain
    --AND main._6sense_company_name = side._account_name
    -- Tie with target accounts to get their 6sense account info, instead of using Salesforce's
    -- ON ( 
    --     side._domain LIKE CONCAT('%', main._6sense_domain, '%')
    --     AND (LENGTH(main._6sense_domain) > 0 AND side._domain IS NOT NULL)
    --     AND main._6sense_company_name = side._account_name
    --     AND main._6sense_country = side._country
    -- )
    -- OR (
    --     side._domain NOT LIKE CONCAT('%', main._6sense_domain, '%')
    --     AND main._6sense_company_name = side._account_name
    --     AND main._6sense_country = side._country
    -- ) 
  GROUP BY 1, 2, 3, 4, 5, 6
),
-- Get all the different types of engagements
sales_intelligence_engagements AS (
  SELECT DISTINCT 
    _country_account,
    _email, 
    _date AS _timestamp,
    CASE 
      WHEN _activity_type LIKE '%Web Visit%' THEN '6sense Web Visits'
      WHEN _activity_type = 'KW Research' THEN '6sense Searched Keywords'
      WHEN _activity_type LIKE '%Page Click%' THEN 'Page Clicked'
      WHEN _activity_type LIKE '%Submit%' THEN 'Form Submission'
      WHEN _activity_type IN (
          'Form Fill',
          'Email Open',
          'Email Click'
        )
      THEN CONCAT(_activity_type, 'ed')
      ELSE _activity_type
    END AS _engagement, 
    'Sales Intelligence' AS _engagement_data_source,
    CASE 
      WHEN _activity_type IN (
        'Web Visit (Anonymous)',
        'KW Research',
        'Bombora',
        'Form Fill',
        'Registered for Event',
        'Web Visit (Known)',
        'Page Click (Anonymous)',
        'Submit (Anonymous)',
        'Attended Webinar',
        'Attended Meeting',
        'Attended Event'
      )
      THEN _activity_target
    END AS _description,
    _count AS _notes
  FROM sales_intelligence_data
  WHERE _activity_type != ''
),
final_activities AS (
  SELECT 
    * 
  FROM campaign_reached 
  UNION DISTINCT
  SELECT 
    * 
  FROM ad_clicks 
  UNION DISTINCT
  SELECT 
    * 
  FROM influenced_form_fills
  UNION DISTINCT
  SELECT 
    * 
  FROM sales_intelligence_engagements
),
-- Only activities involving target accounts are considered
combined_data AS (
  SELECT DISTINCT 
    target_accounts._6sense_company_name,
    target_accounts._6sense_country,
    target_accounts._6sense_domain,
    target_accounts._6sense_industry,
    target_accounts._6sense_employee_range,
    target_accounts._6sense_revenue_range,
    target_accounts._added_on,
    target_accounts._country_account,
    target_accounts._first_impressions,
    target_accounts._website_engagement,
    -- target_accounts._6qa_date,
    -- target_accounts._is_6qa,
    -- target_accounts._6sense_score,
    -- target_accounts._prev_stage,
    -- target_accounts._prev_order,
    -- target_accounts._current_stage,
    -- target_accounts._curr_order,
    -- target_accounts._movement,
    -- target_accounts._movement_date,
    -- target_accounts._crm_account_id,
    -- target_accounts._crm_domain,
    -- target_accounts._crm_account,
    final_activities._email,
    final_activities._timestamp,
    final_activities._engagement,
    final_activities._engagement_data_source,
    final_activities._description,
    final_activities._notes 
  FROM final_activities
  JOIN target_accounts
  USING (_country_account)
),
-- Get accumulated values for each engagement
accumulated_engagement_values AS (
  SELECT
    *,
    SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
    SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
    SUM(CASE WHEN _engagement = '6sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
    SUM(CASE WHEN _engagement = '6sense Web Visits' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_web_visits,
    SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_opens,
    SUM(CASE WHEN _engagement = 'Form Submission' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_form_submission
  FROM combined_data
)
SELECT 
  * 
FROM accumulated_engagement_values;