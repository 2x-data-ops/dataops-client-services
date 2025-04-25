
CREATE OR REPLACE TABLE `jellyvision.db_6sense_engagement_log` AS
WITH target_accounts AS (

    SELECT * FROM `jellyvision.db_6sense_account_current_state`

)
-- Prep the reached account data for use later

, reached_accounts_data AS (

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

        main._campaignid AS _campaign_id,

        -- Need label to distingush 6sense and Linkedin campaigns
        -- side._campaigntype AS _campaign_type,
        side._campaignname AS _campaign_name,
        _6sensecompanyname, _6sensedomain,
        CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account
    
    FROM 
        `x-marketing.jellyvision_mysql_2.db_campaign_reached_accounts` main
    
    JOIN (

        SELECT DISTINCT 

            CAST(_campaignid AS STRING) AS _campaignid, 
            _campaignname,  
            -- IF(_platform = '6Sense', '6sense', _platform) AS _campaigntype
            
        FROM
           `x-marketing.jellyvision_mysql_2.optimization_airtable_ads_6sense`

    ) side

    USING(_campaignid)

)
-- Get campaign reached engagements

, campaign_reached AS (

    SELECT DISTINCT 
        _country_account, 
         _6sensecompanyname, _6sensedomain,
        CAST(NULL AS STRING) AS _email,

        MIN(_latestimpression) OVER(
            PARTITION BY 
                _country_account, 
                _campaign_name
            ORDER BY 
                _latestimpression
        ) 
        AS _timestamp,

        -- CONCAT(_campaign_type, ' ', 'Campaign Reached') AS _engagement,
        '6sense Campaign Reached' AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaign_name AS _description, 
        1 AS _notes

    FROM
        reached_accounts_data

)
-- Get ad clicks engagements

, ad_clicks AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            _country_account,
             _6sensecompanyname, _6sensedomain,
            CAST(NULL AS STRING) AS _email,  
            _activities_on AS _timestamp,
            -- CONCAT(_campaign_type, ' ', 'Ad Clicks') AS _engagement,
            '6sense Ad Clicks' AS _engagement, 
            '6sense' AS _engagement_data_source,
            _campaign_name AS _description,  
            _clicks AS _notes,

            -- Get last period's clicks to compare
            LAG(_clicks) OVER(
                PARTITION BY 
                    _country_account, 
                    _campaign_name
                ORDER BY 
                    _activities_on
            )
            AS _old_notes

        FROM
            reached_accounts_data 
        WHERE
            _clicks >= 1

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

)
-- Get form fills engagements

, influenced_form_fills AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            _country_account,
            _6sensecompanyname, _6sensedomain,
            CAST(NULL AS STRING) AS _email, 
            _activities_on AS _timestamp,
            -- CONCAT(_campaign_type, ' ', 'Influenced Form Filled') AS _engagement, 
            '6sense Influenced Form Filled' AS _engagement,
            '6sense' AS _engagement_data_source,
            _campaign_name AS _description,  
            _influencedformfills AS _notes,

            -- Get last period's clicks to compare
            LAG(_influencedformfills) OVER(
                PARTITION BY 
                    _country_account, 
                    _campaign_name
                ORDER BY 
                    _activities_on
            )
            AS _old_notes

        FROM
            reached_accounts_data 
        WHERE
            _influencedformfills >= 1

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

)
-- Prep the sales intelligence data for use later

,side AS (
  SELECT

            act.name AS _account_name,
            _websiteaddress AS _domain,
            _contactcountry AS _country,
            act.id AS crmid,
            sales.* EXCEPT(_accountname)
        
        FROM 
            `x-marketing.jellyvision_mysql_2.db_sales_intelligence_activities` sales
        
        LEFT JOIN 
            `jellyvision_salesforce.Account` act 
        ON 
            REPLACE(sales._crmaccountid, 'CMA', '') = act.id
        
        WHERE 
            act.accountupdatedate6sense__c IS NOT NULL
) , sales_intelligence_data AS (
    SELECT 

        side._activitytype,
        side._activitytarget,
        side._activitymetainfo,
        side._contactname,
        side._email,
        _account_name,
        _country,
        crmid,
        side._domain,

        CASE 
            WHEN _activitydate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _activitydate)
            ELSE PARSE_DATE('%F', _activitydate)
        END  
        AS _date,

        CONCAT(_country, _account_name) AS _country_account,
        COUNT(*) AS _count

    FROM 
       side  

    LEFT JOIN target_accounts AS main 

    -- Tie with target accounts to get their 6sense account info, instead of using Salesforce's
    ON (
            side._domain LIKE CONCAT('%', main._6sensedomain, '%')
        AND
            (LENGTH(main._6sensedomain) > 0 AND side._domain IS NOT NULL)
        AND
            main._6sensecompanyname = side._account_name
        AND
            main._6sensecountry = side._country
    )
        
    OR (
            side._domain NOT LIKE CONCAT('%', main._6sensedomain, '%')
        AND 
            main._6sensecompanyname = side._account_name
        AND
            main._6sensecountry = side._country
    ) 
   -- WHERE _account_name= 'PepGen'
    GROUP BY 
        1, 2, 3, 4, 5, 6,7,8,9,10

)
-- Get all the different types of engagements

, sales_intelligence_engagements AS (

    SELECT DISTINCT 

        _country_account,
        _email, 
        _date AS _timestamp,
        _account_name,
        _country,
        crmid,
        _domain,
        
        CASE 
            WHEN 
                _activitytype LIKE '%Web Visit%'
            THEN 
                '6sense Web Visits'
            -- WHEN 
            --     _activitytype = 'KW Research'
            -- THEN 
            --     '6sense Searched Keywords'
            WHEN 
                _activitytype LIKE '%Page Click%'
            THEN 
                'Page Clicked'
            WHEN 
                _activitytype IN (
                    'Form Fill',
                    'Email Open',
                    'Email Click'
                )
            THEN 
                CONCAT(_activitytype, 'ed')
            ELSE 
                _activitytype

        END 
        AS _engagement, 

        'Sales Intelligence' AS _engagement_data_source,
        
        CASE 
            WHEN 
                _activitytype IN (
                    'Web Visit (Anonymous)',
                    'KW Research',
                    'Bombora',
                    'Form Fill',
                    'Registered for Event',
                    'Web Visit (Known)',
                    'Page Click (Anonymous)',
                    'Attended Webinar',
                    'Attended Meeting',
                    'Attended Event'
                )
            THEN 
                _activitytarget
            WHEN 
                _activitytype IN (
                    'Email Open', 
                    'TrustRadius',
                    'Unsubscribed', 
                    'Email Click'
                )  
            THEN 
                _activitymetainfo
        END 
        AS _description,
        
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype NOT IN ( '', 'KW Research')

)
-- Only activities involving target accounts are considered

, combined_data AS (
    
    SELECT DISTINCT 
         IFNULL(target_accounts._6sensecompanyname,activities._6sensecompanyname) AS _6sensecompanyname,
          _6sensecountry AS _6sensecountry , 
        IFNULL(target_accounts._6sensedomain,activities._6sensedomain) AS _6sensedomain, 
        IFNULL(target_accounts._domain,activities._6sensedomain) AS _domain, 
        _6senseindustry, 
        _6senseemployeerange, 
        _6senserevenuerange, 
        _added_on, _country_account, _first_impressions, _website_engagement, _6qa_date, _is_6qa, _6sensescore, _prev_stage, _prev_order, _current_stage, _curr_order, _movement, _movement_date, 
        _crmaccountid AS _crmaccountid , 
        IFNULL(_crmdomain, activities._6sensedomain) AS _crmdomain, 
        IFNULL(_crmaccount,activities._6sensecompanyname) AS  _crmaccount, 
        activities.* EXCEPT(_country_account, _6sensecompanyname, _6sensedomain)
        
    FROM (

        SELECT * FROM campaign_reached 
        UNION DISTINCT
        SELECT * FROM ad_clicks 
        UNION DISTINCT
        SELECT * FROM influenced_form_fills
        
    ) activities

    LEFT JOIN
        target_accounts

    USING (_country_account)
    UNION ALL

    SELECT DISTINCT 

        IFNULL(target_accounts._6sensecompanyname,_account_name) AS _6sensecompanyname, 
        IFNULL(_6sensecountry,_country) AS _6sensecountry , 
        IFNULL(_6sensedomain,activities._domain) AS _6sensedomain, 
        IFNULL(target_accounts._domain,activities._domain) AS _domain, 
        _6senseindustry, 
        _6senseemployeerange, 
        _6senserevenuerange, 
        _added_on, _country_account, _first_impressions, _website_engagement, _6qa_date, _is_6qa, _6sensescore, _prev_stage, _prev_order, _current_stage, _curr_order, _movement, _movement_date, 
        IFNULL(_crmaccountid,crmid) AS _crmaccountid , 
        IFNULL(_crmdomain, activities._domain) AS _crmdomain, 
        IFNULL(_crmaccount,_account_name) AS  _crmaccount,
        activities.* EXCEPT(_country_account,_account_name,_country,_domain,crmid)
        
    FROM (
        SELECT *  FROM sales_intelligence_engagements
        
    ) activities

    LEFT JOIN
        target_accounts

    USING (_country_account)
)
-- Get accumulated values for each engagement

, accumulated_engagement_values AS (

    SELECT
        *,

        -- The aggregated values
        SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
        SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_campaign_reached,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_ad_clicks,
        SUM(CASE WHEN _engagement = '6sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
        SUM(CASE WHEN _engagement = '6sense Web Visits' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_web_visits,
        SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_opens,

    FROM 
        combined_data
        
)

SELECT * FROM accumulated_engagement_values;