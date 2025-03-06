
CREATE OR REPLACE TABLE `syniti.db_6sense_engagement_log` AS

-- Get all target accounts and their unique info
WITH target_accounts AS (

    SELECT * FROM `syniti.db_6sense_account_current_state`

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

        main._campaignid AS _campaign_id,

        -- Need label to distingush 6sense and Linkedin campaigns
        side._campaigntype AS _campaign_type,
        side._campaignname AS _campaign_name,
        CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account
    
    FROM 
        `syniti_mysql.syniti_db_campaign_reached_accounts` main
    
    JOIN (

        SELECT DISTINCT 

            _campaignid, 
            _campaignname,  
            IF(_platform = '6Sense', '6sense', _platform) AS _campaigntype
            
        FROM
            `syniti_mysql.syniti_optimization_airtable_ads_6sense`
        WHERE 
            _campaignid != ''

    ) side

    USING(_campaignid)

),


-- Get campaign reached engagements
campaign_reached AS (

    SELECT DISTINCT 
 
        _country_account, 
        CAST(NULL AS STRING) AS _email,

        MIN(_latestimpression) OVER(
            PARTITION BY 
                _country_account, 
                _campaign_name
            ORDER BY 
                _latestimpression
        ) 
        AS _timestamp,

        CONCAT(_campaign_type, ' ', 'Campaign Reached') AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaign_name AS _description, 
        1 AS _notes

    FROM
        reached_accounts_data

),

-- Get ad clicks engagements
ad_clicks AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            _country_account,
            CAST(NULL AS STRING) AS _email,  
            _activities_on AS _timestamp,
            CONCAT(_campaign_type, ' ', 'Ad Clicks') AS _engagement, 
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

),

-- Get form fills engagements
influenced_form_fills AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            _country_account, 
            CAST(NULL AS STRING) AS _email, 
            _activities_on AS _timestamp,
            CONCAT(_campaign_type, ' ', 'Influenced Form Filled') AS _engagement, 
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

),

-- Prep the sales intelligence data for use later
sales_intelligence_data AS (

    SELECT 

        side._activitytype,
        side._activitytarget,
        side._activitymetainfo,
        side._contactname,
        side._email,

        CASE 
            WHEN _activitydate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _activitydate)
            ELSE PARSE_DATE('%F', _activitydate)
        END  
        AS _date,

        main._country_account,
        COUNT(*) AS _count

    FROM 
        target_accounts AS main

    JOIN (

        SELECT

            act.name AS _account_name,
            act.website AS _domain,
            COALESCE(act.billingcountry, act.shippingcountry) AS _country,
            sales.* EXCEPT(_accountname)
        
        FROM 
            `syniti_mysql.syniti_db_sales_intelligence_activities` sales
        
        LEFT JOIN 
            `syniti_salesforce.Account` act 
        ON 
            REPLACE(sales._crmaccountid, 'CMA', '') = act.id
        
        -- WHERE 
        --     act.accountupdatedate6sense__c IS NOT NULL
        
        -- AND 
        --     sales._sdc_deleted_at IS NULL
        AND
            act.isdeleted = false
        
    ) side

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
        
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7

),

-- Get all the different types of engagements
sales_intelligence_engagements AS (

    SELECT DISTINCT 

        _country_account,
        _email, 
        _date AS _timestamp,
        
        CASE 
            WHEN 
                _activitytype LIKE '%Web Visit%'
            THEN 
                '6sense Web Visits'
            WHEN 
                _activitytype = 'KW Research'
            THEN 
                '6sense Searched Keywords'
            WHEN 
                _activitytype LIKE '%Page Click%'
            THEN 
                'Page Clicked'
            WHEN 
                _activitytype IN (
                    'Form Fill'/* ,
                    'Email Open',
                    'Email Click' */
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
                    -- 'Email Open', 
                    'TrustRadius',
                    'Unsubscribed'/* , 
                    'Email Click' */
                )  
            THEN 
                _activitymetainfo
        END 
        AS _description,
        
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype != ''
    
    -- Exclude email events from sales intelligence
    AND 
        _activitytype NOT LIKE '%Email%'

),

-- Get email engagements from Hubspot
-- hubspot_email_engagements AS (

--     SELECT  

--         CONCAT(_country, _company) AS _country_account,
--         _email,
--         DATE(_timestamp) AS _timestamp,
--         CONCAT('Email ', _engagement) AS _engagement,
--         'Hubspot' AS _engagement_data_source,
--         _utmcampaign AS _description,
--         1 AS _notes

--     FROM 
--         `syniti.db_email_engagements_log` 

--     WHERE 
--         _engagement IN('Opened', 'Clicked')

-- ),

-- Only activities involving target accounts are considered
combined_data AS (

    SELECT DISTINCT 

        target_accounts.*,
        activities.* EXCEPT(_country_account)
        
    FROM (

        SELECT * FROM campaign_reached 
        UNION DISTINCT
        SELECT * FROM ad_clicks 
        UNION DISTINCT
        SELECT * FROM influenced_form_fills
        UNION DISTINCT
        SELECT * FROM sales_intelligence_engagements
        -- UNION DISTINCT
        -- SELECT * FROM hubspot_email_engagements
        
    ) activities

    JOIN
        target_accounts

    USING (_country_account)

)

SELECT * FROM combined_data;