CREATE OR REPLACE TABLE `x-marketing.smartcommnam.db_6sense_consolidated_engagement_log`

CLUSTER BY _6sensecompanyname, _engagement, _engagement_data_source AS

WITH target_accounts AS (
    SELECT * FROM `smartcommnam.db_6sense_account_current_state`
),

-- Prep the reached account data for use later
reached_accounts_data AS (
    SELECT DISTINCT

        CAST(main._clicks AS INTEGER) AS _clicks,
        CAST(main._influencedformfills AS INTEGER) AS _influencedformfills,
        CASE 
            WHEN main._latestimpression LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._latestimpression)
            ELSE PARSE_DATE('%F', main._latestimpression)
        END 
        AS _latestimpression, 
        CASE 
            WHEN main._extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
            ELSE PARSE_DATE('%F', main._extractdate)
        END 
        AS _activities_on, 
        main._campaignid,
        -- Need label to distingush 6sense and Linkedin campaigns
        -- side._campaigntype,
        side._campaignname,
        CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account   
    FROM 
        `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` main  
    JOIN (
        SELECT DISTINCT 
            _campaignid, 
            _campaignname,  
            -- _campaigntype       
        FROM
            `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense`
    ) side
    USING(_campaignid)
),

-- Get campaign reached engagement for 6sense
sixsense_campaign_reached AS (

    SELECT DISTINCT 
        -- CAST(NULL AS STRING) AS _email, 
        _country_account, 
        -- CAST(NULL AS STRING) AS _city,
        -- CAST(NULL AS STRING) AS _state,
        MIN(_latestimpression) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _latestimpression
        ) 
        AS _timestamp,
        '6sense Campaign Reached' AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaignname AS _description, 
        1 AS _notes,
        CAST(NULL AS STRING) AS _email,
    FROM
        reached_accounts_data
    -- WHERE
    --     _campaigntype = '6sense Advertising'
),

-- Get ad clicks engagement for 6sense
sixsense_ad_clicks AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (
        SELECT DISTINCT 
            -- CAST(NULL AS STRING) AS _email, 
            _country_account, 
            -- CAST(NULL AS STRING) AS _city,
            -- CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6sense Ad Clicks' AS _engagement, 
            '6sense' AS _engagement_data_source,
            _campaignname AS _description,  
            _clicks AS _notes,
            CAST(NULL AS STRING) AS _email,
            -- Get last period's clicks to compare
            LAG(_clicks) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            )
            AS _old_notes
        FROM
            reached_accounts_data 
        WHERE
            _clicks >= 1
        -- AND
        --     _campaigntype = '6sense Advertising'
    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1
),

-- Get form fills engagement for 6sense
sixsense_form_fills AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 
            -- CAST(NULL AS STRING) AS _email, 
            _country_account, 
            -- CAST(NULL AS STRING) AS _city,
            -- CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6sense Influenced Form Fill' AS _engagement, 
            '6sense' AS _engagement_data_source,
            _campaignname AS _description,  
            _influencedformfills AS _notes,
            CAST(NULL AS STRING) AS _email,
            -- Get last period's clicks to compare
            LAG(_influencedformfills) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            )
            AS _old_notes
        FROM
            reached_accounts_data 
        WHERE
            _influencedformfills >= 1
        -- AND
        --     _campaigntype = '6sense Advertising'
    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),
-- adding email engagement from marketo
marketo_email_engagements AS (
  SELECT 
       DISTINCT 
        -- CAST(NULL AS STRING) AS _email, 
        CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account,
        -- marketo._domain,
        -- CAST(NULL AS STRING) AS _city,
        -- CAST(NULL AS STRING) AS _state,
        DATE(marketo._timestamp) AS _timestamp,
        CONCAT('Email', ' ', marketo._engagement) AS _engagement,
        'Marketo' AS _engagement_data_source, 
        _campaign AS _description,
        1 AS _notes,
        marketo._email
  FROM `smartcommnam.db_6sense_account_current_state` sixsense
  FULL OUTER JOIN `x-marketing.smartcommnam.db_email_engagements_log` marketo
    ON sixsense._6sensedomain = marketo._domain
),
account_activity_summary AS (
  SELECT
    _activitytype,
    _activitytarget,
    -- _contactname,
    -- _contactemail,
    _6sensecompanyname,
    _companyinfo,
    REGEXP_EXTRACT(_companyinfo, r'^(.*?) -') AS _6sensecountry,
    REGEXP_EXTRACT(_companyinfo, r'- (.*?)$') AS _6sensedomain,
    CASE 
      WHEN _activitydate LIKE '%/%'
      THEN PARSE_DATE('%m/%e/%Y', _activitydate)
      ELSE PARSE_DATE('%F', _activitydate)
    END  
    AS _activitydate,
    COUNT(*) AS _count
  FROM
    `smartcommnam_mysql.smartcommnam_db_6sense_activity_summary_nam`
  GROUP BY ALL
),
acccount_activity_summary_main AS (
  SELECT 
    _activitytype,
    _activitytarget,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account,
    _activitydate,
    _count
  FROM account_activity_summary
),
acccount_activity_summary_keyword_researched AS (
  SELECT DISTINCT
    _country_account,
    _activitydate AS _timestamp,
    _activitytype AS _engagement, 
    'Activity Summary Account' AS _engagement_data_source,
    _activitytarget AS _description,
    _count AS _notes,
    CAST(NULL AS STRING) AS _email,
  FROM acccount_activity_summary_main
  WHERE _activitytype = 'KW Research'
),
account_activity_summary_web_visited AS (
  SELECT DISTINCT
    _country_account,
    _activitydate AS _timestamp,
    _activitytype AS _engagement, 
    'Activity Summary Account' AS _engagement_data_source,
    _activitytarget AS _description,
    _count AS _notes,
    CAST(NULL AS STRING) AS _email,
  FROM acccount_activity_summary_main
  WHERE _activitytype = 'Website Visit'
),
account_activity_summary_bombora_topics AS (
  SELECT DISTINCT
    _country_account,
    _activitydate AS _timestamp,
    _activitytype AS _engagement, 
    'Activity Summary Account' AS _engagement_data_source,
    _activitytarget AS _description,
    _count AS _notes,
    CAST(NULL AS STRING) AS _email,
  FROM acccount_activity_summary_main
  WHERE _activitytype = 'Current Bombora Company Surge Topics'
),


-- Only activities involving target accounts are considered
combined_data AS (
    SELECT DISTINCT 
        target_accounts.*,
        activities.* EXCEPT(_country_account)     
    FROM (
        SELECT * FROM sixsense_campaign_reached 
        UNION DISTINCT
        SELECT * FROM sixsense_ad_clicks 
        UNION DISTINCT
        SELECT * FROM sixsense_form_fills
        UNION DISTINCT
        SELECT * FROM acccount_activity_summary_keyword_researched
        UNION DISTINCT
        SELECT * FROM account_activity_summary_web_visited
        UNION DISTINCT
        SELECT * FROM account_activity_summary_bombora_topics
        UNION DISTINCT
        SELECT * FROM marketo_email_engagements     
    ) activities
    JOIN
        target_accounts
    USING (_country_account)
),

-- Get accumulated values for each engagement
accumulated_engagement_values AS (
    SELECT *,
        -- The aggregated values
        SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
        SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
        SUM(CASE WHEN _engagement = '6sense Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_form_fills,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_campaign_reached,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_ad_clicks,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_form_fills,
        -- SUM(CASE WHEN _engagement = 'SEM Engagement' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_sem_engagements,
        SUM(CASE WHEN _engagement = 'Website Visit' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_webpage_visits,
        SUM(CASE WHEN _engagement = 'KW Research' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
        SUM(CASE WHEN _engagement = 'Current Bombora Company Surge Topics' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_bombora_topics,
        SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
        SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click
    FROM 
        combined_data      
),
--get visit_age and company_flag
get_visit_age AS (
    SELECT DISTINCT
        (_country_account) AS _country_account,
        COUNT(_country_account) AS engagement_count,
        DATE_DIFF(CURRENT_DATE('America/New_York'), MAX(CAST(_timestamp AS DATE)), DAY) AS _visit_age
    FROM accumulated_engagement_values
    GROUP BY _country_account
)
SELECT *,
    CASE
        WHEN get_visit_age.engagement_count =1 then "New"
        WHEN get_visit_age.engagement_count >1 AND _visit_age < 10 then "Recent with Intent"
        WHEN get_visit_age.engagement_count >1 AND _visit_age > 10 AND _visit_age <= 90 then "Recent"
        WHEN get_visit_age.engagement_count >1 AND _visit_age > 90 then "Returning"
        ELSE "Unknown"
    END AS _company_flag
FROM accumulated_engagement_values
LEFT JOIN get_visit_age
USING (_country_account)