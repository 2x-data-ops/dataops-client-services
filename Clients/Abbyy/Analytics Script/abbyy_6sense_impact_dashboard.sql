
-- 6sense Buying Stage Movement

CREATE OR REPLACE TABLE `abbyy.db_6sense_buying_stages_movement` AS

-- Set 6sense buying stages and their order
WITH sixsense_stage_order AS (

    SELECT 'Target' AS _buying_stage, 1 AS _order 
    UNION ALL
    SELECT 'Awareness' AS _buying_stage, 2 AS _order 
    UNION ALL
    SELECT 'Consideration' AS _buying_stage, 3 AS _order 
    UNION ALL
    SELECT 'Decision' AS _buying_stage, 4 AS _order 
    UNION ALL
    SELECT 'Purchase' AS _buying_stage, 5 AS _order 

),

-- Get 6sense buying stage data
sixsense_buying_stage_data AS (

    SELECT DISTINCT

        ROW_NUMBER() OVER(

            PARTITION BY 
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            ORDER BY 
                CASE 
                    WHEN _extractdate LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                    ELSE PARSE_DATE('%F', _extractdate)
                END 
            DESC 

        )
        AS _rownum,

        CASE 
            WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate)
        END 
        AS _activities_on,

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account,
        '6sense' AS _data_source,
        _buyingstagestart AS _previous_stage,
        _buyingstageend AS _current_stage

    FROM 
        `abbyy_mysql.db_account_buying_stages`

),

-- Get latest week data for each account
-- Add 6sense buying stage order into the mix
-- Evaluate the movement of the stage
latest_sixsense_buying_stage_with_order_and_movement AS (

    SELECT

        main.* EXCEPT(_rownum),
        prev._order AS _previous_stage_order,
        curr._order AS _current_stage_order,

        CASE
            WHEN curr._order > prev._order 
            THEN '+ve'
            WHEN curr._order < prev._order 
            THEN '-ve'
            ELSE 'Stagnant'
        END 
        AS _movement

    FROM 
        sixsense_buying_stage_data AS main 
    
    LEFT JOIN 
        sixsense_stage_order AS prev 
    ON 
        main._previous_stage = prev._buying_stage
    
    LEFT JOIN 
        sixsense_stage_order AS curr 
    ON 
        main._current_stage = curr._buying_stage
    
    WHERE 
        main._rownum = 1

),

-- Set Intentsify buying stages and their order
intentsify_stage_order AS (

    SELECT 'Awareness' AS _buying_stage, 1 AS _order 
    UNION ALL
    SELECT 'Interest' AS _buying_stage, 2 AS _order 
    UNION ALL
    SELECT 'Consideration' AS _buying_stage, 3 AS _order 
    UNION ALL
    SELECT 'Decision' AS _buying_stage, 4 AS _order

),

-- Get Intentsify buying stage data
intentsify_buying_stage_data AS (

    SELECT DISTINCT

        ROW_NUMBER() OVER(

            PARTITION BY 
                _account
            ORDER BY 
                CASE 
                    WHEN _date LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', _date)
                    ELSE PARSE_DATE('%F', _date)
                END 
            DESC 

        )
        AS _rownum,

        CASE 
            WHEN _date LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _date)
            ELSE PARSE_DATE('%F', _date)
        END 
        AS _activities_on,

        CAST(NULL AS STRING) AS _6sensecompanyname,
        CAST(NULL AS STRING) _6sensecountry,
        _account AS _6sensedomain,
        CAST(NULL AS STRING) AS _country_account,
        'Intentsify' AS _data_source,
        _buyerresearchstagelastweek AS _previous_stage,
        _buyerresearchstagethisweek	 AS _current_stage

    FROM 
        `abbyy_mysql.db_its_buyer_stage_ia`

),

-- Get latest week data for each account
-- Add Intentsify buying stage order into the mix
-- Evaluate the movement of the stage
latest_intentsify_buying_stage_with_order_and_movement AS (

    SELECT

        main.* EXCEPT(_rownum),
        prev._order AS _previous_stage_order,
        curr._order AS _current_stage_order,

        CASE
            WHEN curr._order > prev._order 
            THEN '+ve'
            WHEN curr._order < prev._order 
            THEN '-ve'
            ELSE 'Stagnant'
        END 
        AS _movement

    FROM 
        intentsify_buying_stage_data AS main 
    
    LEFT JOIN 
        intentsify_stage_order AS prev 
    ON 
        main._previous_stage = prev._buying_stage
    
    LEFT JOIN 
        intentsify_stage_order AS curr 
    ON 
        main._current_stage = curr._buying_stage
    
    WHERE 
        main._rownum = 1

),

-- Merge 6sense and Intentsify data together
combined_data AS (

    SELECT * FROM latest_sixsense_buying_stage_with_order_and_movement

    UNION ALL 

    SELECT * FROM latest_intentsify_buying_stage_with_order_and_movement

)

SELECT * FROM combined_data;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- 6sense Account Current State

CREATE OR REPLACE TABLE `abbyy.db_6sense_account_current_state` AS

-- Get all target accounts and their segments
WITH target_accounts AS (

    SELECT 

        * EXCEPT(_rownum)
    
    FROM (
    
        SELECT DISTINCT 

            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            _industrylegacy AS _6senseindustry,
            _6senseemployeerange,
            _6senserevenuerange,

            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END 
            AS _added_on,

            '6sense' AS _data_source,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account,
            
            -- Get the earliest date of appearance of each account
            ROW_NUMBER() OVER(

                PARTITION BY 
                    _6sensecompanyname,
                    _6sensecountry,
                    _6sensedomain
                ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 

            )
            AS _rownum

        FROM 
            `abbyy_mysql.db_target_account`

    ) 

    WHERE 
        _rownum = 1
    
),

-- Get date when account had first impression
reached_related_info AS (

    SELECT

        * EXCEPT(_rownum)

    FROM (

        SELECT DISTINCT

            MIN(
                CASE 
                    WHEN _extractdate LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                    ELSE PARSE_DATE('%F', _latestimpression)
                END
            )
            OVER(

                PARTITION BY 
                    CONCAT(_6sensecountry, _6sensecompanyname) 
            
            )
            AS _first_impressions,

            CASE
                WHEN _websiteengagement = '-'
                THEN CAST(NULL AS STRING)
                ELSE _websiteengagement
            END 
            AS _website_engagement,

            ROW_NUMBER() OVER(

                PARTITION BY 
                    CONCAT(_6sensecountry, _6sensecompanyname) 
                ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                        ELSE PARSE_DATE('%F', _latestimpression)
                    END
                DESC

            )
            AS _rownum,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM 
            `abbyy_mysql.db_campaign_reached_account`

        WHERE 
            _campaignid IN (

                SELECT DISTINCT 
                    _campaignid 
                FROM 
                    `abbyy_mysql.db_campaign_segment`

            )

    )
    WHERE 
        _rownum = 1

),

-- Get the date when account first became a 6QA
six_qa_related_info AS (

    SELECT DISTINCT

        CASE 
            WHEN _6qadate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _6qadate)
            ELSE PARSE_DATE('%F', _6qadate)
        END 
        AS _6qa_date,

        true _is_6qa,
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

    FROM 
        `abbyy_mysql.db_6qa_status`

),

-- Get the buying stage info each account
buying_stage_related_info AS (

    SELECT DISTINCT

        _previous_stage,
        _previous_stage_order,
        _current_stage,
        _current_stage_order,
        _movement,
        _activities_on AS _movement_date,
        _country_account

    FROM
        `abbyy.db_6sense_buying_stages_movement`
    
    WHERE 
        _data_source = '6sense'

),

-- Attach all other data parts to target accounts
combined_data AS (

    SELECT DISTINCT 

        target.*, 
        reached.* EXCEPT(_country_account),
        six_qa.* EXCEPT(_country_account),
        stage.* EXCEPT(_country_account)   

    FROM
        target_accounts AS target

    LEFT JOIN
        reached_related_info AS reached 
    USING(
        _country_account
    )

    LEFT JOIN
        six_qa_related_info AS six_qa 
    USING(
        _country_account
    ) 

    LEFT JOIN
        buying_stage_related_info AS stage
    USING(
        _country_account
    ) 

)

SELECT * FROM combined_data;


--////////////////////////////////////////////////////////////////////////

-- Add Intentsify's data into the 6sense table

INSERT INTO `abbyy.db_6sense_account_current_state` (

    _6sensedomain,
    _added_on,
    _data_source,
    _previous_stage,
    _previous_stage_order,
    _current_stage,
    _current_stage_order,
    _movement,
    _movement_date,
    _first_impressions

)

-- Get all accounts from the buying stage data
-- The buying stage accounts has most accounts for Intentsify
WITH buying_stage_accounts AS (

    SELECT DISTINCT

        _6sensedomain,
        _activities_on AS _added_on,
        'Intentsify' AS _data_source,
        _previous_stage,
        _previous_stage_order,
        _current_stage,
        _current_stage_order,
        _movement,
        _activities_on AS _movement_date

    FROM
        `abbyy.db_6sense_buying_stages_movement`
    
    WHERE 
        _data_source = 'Intentsify'

),

-- Get date when account had first impression
reached_related_info AS (

    SELECT

        * EXCEPT(_rownum)

    FROM (

        SELECT DISTINCT

            _domain,

            MIN(
                CASE 
                    WHEN _date LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', _date)
                    ELSE PARSE_DATE('%F', _date)
                END
            )
            OVER(

                PARTITION BY 
                    _domain
            
            )
            AS _first_impressions,

            ROW_NUMBER() OVER(

                PARTITION BY 
                    _domain 
                ORDER BY 
                    CASE 
                        WHEN _date LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _date)
                        ELSE PARSE_DATE('%F', _date)
                    END
                DESC

            )
            AS _rownum

        FROM 
            `abbyy_mysql.db_its_acc_eng_dp`

        WHERE 
            _campaignid IN (

                SELECT DISTINCT 

                    _campaignid 

                FROM 
                    `abbyy_mysql.db_campaign_segment`

                WHERE 
                    _sdc_deleted_at IS NULL

            )

    )
    WHERE 
        _rownum = 1

),

-- Attach all other data parts to buying stage accounts
combined_data AS (

    SELECT DISTINCT 

        buyer.*, 
        reached.* EXCEPT(_domain)

    FROM
        buying_stage_accounts AS buyer

    LEFT JOIN
        reached_related_info AS reached 
    
    ON
        buyer._6sensedomain = reached._domain

)

SELECT * FROM combined_data;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- 6sense Engagement Log

CREATE OR REPLACE TABLE `abbyy.db_6sense_engagement_log` AS

-- Get all 6sense target accounts and their unique info
WITH target_accounts AS (

    SELECT * FROM `abbyy.db_6sense_account_current_state`

    WHERE _data_source = '6sense'

),

-- Prep the old engagement data for use later
old_engagement_data AS (

    SELECT

        CONCAT(main._country, main._accountname) AS _country_account,

        CASE 
            WHEN main._engagementdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._engagementdate)
            ELSE PARSE_DATE('%F', main._engagementdate)
        END 
        AS _engagementdate,

        main._type AS _activity_type,

        CASE 
            WHEN side._campaigntype LIKE '%6sense%'
            THEN '6sense'
            WHEN side._campaigntype LIKE '%Linked%'
            THEN 'LinkedIn'
        END
        AS _campaign_type,
        
        main._engagement,

        CAST(REPLACE(IF(main._impressions = '', NULL, main._impressions), ',', '') AS INT64) AS _impressions,
        CAST(REPLACE(IF(main._adsclick = '', NULL, main._adsclick), ',', '') AS INT64) AS _adsclick,
        CAST(REPLACE(IF(main._frequency = '', NULL, main._frequency), ',', '') AS INT64) AS _frequency

    FROM 
        `abbyy_mysql.db_engagement_list` main

    LEFT JOIN 
        `abbyy_mysql.db_campaign_segment` side
    ON 
        main._engagement = side._campaignname
    
    WHERE 
        side._sdc_deleted_at IS NULL

),

-- Prep the new reached accounts data for use later
new_reached_accounts_data AS (

    SELECT DISTINCT
        
        CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account,

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
        side._campaignname AS _campaign_name,

        CASE 
            WHEN side._campaigntype LIKE '%6sense%'
            THEN '6sense'
            WHEN side._campaigntype LIKE '%Linked%'
            THEN 'LinkedIn'
        END
        AS _campaign_type,

        CAST(REPLACE(main._impressions, ',', '') AS INTEGER) AS _impressions,
        CAST(main._clicks AS INTEGER) AS _clicks,
        CAST(main._influencedformfills AS INTEGER) AS _influencedformfills
 
    FROM 
        `abbyy_mysql.db_campaign_reached_account` main

    JOIN `abbyy_mysql.db_campaign_segment` side

    USING(_campaignid)

    -- Exclude those that are part of the old data
    WHERE 
        main._extractdate != '9/22/2023'
    AND 
        side._sdc_deleted_at IS NULL
    
),

-- Get campaign reached engagements
campaign_reached AS (

    -- For older data
    SELECT

        _country_account,
        _engagementdate AS _timestamp,
        CONCAT(_campaign_type, ' Campaign Reached') AS _engagement,
        _campaign_type AS _channel, 
        _engagement AS _description,
        _impressions AS _notes

    FROM  
        old_engagement_data

    WHERE
        _activity_type = 'Reach'
    AND 
        _campaign_type IS NOT NULL

    UNION ALL 

    -- For newer data
    SELECT 
        
        * EXCEPT(_notes, _old_notes),
        (_notes - COALESCE(_old_notes, 0)) AS _notes
    
    FROM (

        SELECT DISTINCT 

            _country_account,
            _latestimpression AS _timestamp,
            CONCAT(_campaign_type, ' Campaign Reached') AS _engagement,
            _campaign_type AS _channel, 
            _campaign_name AS _description,  
            _impressions AS _notes,

            -- Get last period's numbers to compare
            LAG(_impressions) OVER(

                PARTITION BY 
                    _country_account, 
                    _campaign_name
                ORDER BY 
                    _activities_on

            )
            AS _old_notes

        FROM
            new_reached_accounts_data

    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

-- Get ad clicks engagements
ad_clicks AS (

    -- For older data
    SELECT

        _country_account,
        _engagementdate AS _timestamp,
        CONCAT(_campaign_type, ' Ad Clicks') AS _engagement,
        _campaign_type AS _channel, 
        _engagement AS _description,
        _adsclick AS _notes

    FROM  
        old_engagement_data

    WHERE
        _activity_type = 'Ads Click'

    UNION ALL 

    -- For newer data
    SELECT 
        
        * EXCEPT(_notes, _old_notes),
        (_notes - COALESCE(_old_notes, 0)) AS _notes
    
    FROM (

        SELECT DISTINCT 

            _country_account,
            _activities_on AS _timestamp,
            CONCAT(_campaign_type, ' Ad Clicks') AS _engagement,
            _campaign_type AS _channel, 
            _campaign_name AS _description,  
            _clicks AS _notes,

            -- Get last period's numbers to compare
            LAG(_clicks) OVER(

                PARTITION BY 
                    _country_account, 
                    _campaign_name
                ORDER BY 
                    _activities_on

            )
            AS _old_notes

        FROM
            new_reached_accounts_data
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
        
        * EXCEPT(_notes, _old_notes),
        (_notes - COALESCE(_old_notes, 0)) AS _notes
    
    FROM (

        SELECT DISTINCT 

            _country_account,
            _activities_on AS _timestamp,
            CONCAT(_campaign_type, ' Influenced Form Filled') AS _engagement,
            _campaign_type AS _channel, 
            _campaign_name AS _description,  
            _influencedformfills AS _notes,

            -- Get last period's numbers to compare
            LAG(_influencedformfills) OVER(

                PARTITION BY 
                    _country_account, 
                    _campaign_name
                ORDER BY 
                    _activities_on

            )
            AS _old_notes

        FROM
            new_reached_accounts_data
        WHERE 
            _influencedformfills >= 1

    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

intent_engagements AS (

    -- For older data
    SELECT

        _country_account,
        _engagementdate AS _timestamp,
        '6sense Searched Keywords' AS _engagement,
        '6sense' AS _channel,  
        _engagement AS _description,
        _frequency AS _notes

    FROM  
        old_engagement_data

    WHERE
        _activity_type = 'Keywords'
    
    UNION ALL 

    -- For newer data
    SELECT

        CONCAT(SPLIT(_companyinfo, ' - ')[ORDINAL(1)], _companyname) AS _country_account,

        CASE 
            WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate)
        END 
        AS _timestamp,

        CASE
            WHEN _categoryname LIKE '%Keyword%'
            THEN '6sense Searched Keywords'
            WHEN _categoryname LIKE '%Website%'
            THEN '6sense Website Visited'
            WHEN _categoryname LIKE '%Topic%'
            THEN '6sense Bombora Topics'
        END 
        AS _engagement,

        '6sense' AS _channel,
        _categoryvalue AS _description,
        1 AS _notes

    FROM 
        `abbyy_mysql.db_account_activity_summary`

),

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
        SELECT * FROM intent_engagements
        
    ) activities

    JOIN
        target_accounts

    USING (_country_account)

),

-- Get accumulated values for each engagement
accumulated_engagement_values AS (

    SELECT
        *,

        -- The aggregated values
        SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6s_campaign_reached,
        SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6s_ad_clicks,
        SUM(CASE WHEN _engagement = '6sense Influenced Form Filled' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6s_form_fills,  
        SUM(CASE WHEN _engagement = 'LinkedIn Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_campaign_reached,
        SUM(CASE WHEN _engagement = 'LinkedIn Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_ad_clicks,
        SUM(CASE WHEN _engagement = 'LinkedIn Influenced Form Filled' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_form_fills,  
        SUM(CASE WHEN _engagement = '6sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6s_searched_keywords,
        SUM(CASE WHEN _engagement = '6sense Website Visited' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6s_web_visits,
        SUM(CASE WHEN _engagement = '6sense Bombora Topics' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6s_bombora_topics,

        -- Create total fields for Intentsify engagements
        CAST(NULL AS INT64) AS _total_int_campaign_reached,
        CAST(NULL AS INT64) AS _total_int_ad_clicks,
        CAST(NULL AS INT64) AS _total_int_website_visits,
        CAST(NULL AS INT64) AS _total_int_video_views,
        CAST(NULL AS INT64) AS _total_int_searched_topics

    FROM 
        combined_data
        
)

SELECT * FROM accumulated_engagement_values;


--//////////////////////////////////////////////////////////////////////////////////////////////

-- Add Intentsify's data into the 6sense table

INSERT INTO `abbyy.db_6sense_engagement_log` (

    _6sensedomain,
    _added_on,
    _data_source,
    _previous_stage,
    _previous_stage_order,
    _current_stage,
    _current_stage_order,
    _movement,
    _movement_date,
    _first_impressions,
    _timestamp,
    _engagement,
    _channel,
    _description,
    _notes,
    _total_int_campaign_reached,
    _total_int_ad_clicks,
    _total_int_website_visits,
    _total_int_video_views,
    _total_int_searched_topics

)

-- Get all Intentsify target accounts and their unique info
WITH target_accounts AS (

    SELECT 

        _6sensedomain,
        _added_on,
        _data_source,
        _previous_stage,
        _previous_stage_order,
        _current_stage,
        _current_stage_order,
        _movement,
        _movement_date,
        _first_impressions 
    
    FROM 
        `abbyy.db_6sense_account_current_state`

    WHERE 
        _data_source = 'Intentsify'

),

-- Prep the reached account data for use later
reached_accounts_data AS (

    SELECT 

        _domain,

        CASE 
            WHEN main._date LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._date)
            ELSE PARSE_DATE('%F', main._date)
        END 
        AS _date,

        side._campaignname,
        CAST(REPLACE(main._impressions, ',', '') AS INT64) AS _impressions,
        CAST(REPLACE(main._clicks, ',', '') AS INT64) AS _clicks,
        CAST(REPLACE(main._totalsitevisits, ',', '') AS INT64) AS _totalsitevisits,
        CAST(REPLACE(main._videoviews, '-', '0') AS INT64) AS _videoviews

    FROM  
        `abbyy_mysql.db_its_acc_eng_dp` main

    JOIN 
        `abbyy_mysql.db_campaign_segment` side
    
    USING(_campaignid)

    WHERE 
        side._sdc_deleted_at IS NULL

),

-- Get campaign reached engagement for Intentsify
intentsify_campaign_reached AS (

    SELECT 

        _domain,
        _date AS _timestamp,
        'Intentsify Campaign Reached' AS _engagement,
        'Intentsify' AS _channel, 
        _campaignname AS _description,
        _impressions AS _notes

    FROM  
        reached_accounts_data
    WHERE 
        _impressions > 0

),

-- Get ad clicks engagement for Intentsify
intentsify_ad_clicks AS (

    SELECT

        _domain,
        _date AS _timestamp,
        'Intentsify Ad Clicks' AS _engagement,
        'Intentsify' AS _channel, 
        _campaignname AS _description,
        _clicks AS _notes

    FROM  
        reached_accounts_data
    WHERE 
        _clicks > 0

),

-- Get web visits engagement for Intentsify
intentsify_web_visits AS (

    SELECT

        _domain,
        _date AS _timestamp,
        'Intentsify Website Visits' AS _engagement,
        'Intentsify' AS _channel, 
        _campaignname AS _description,
        _totalsitevisits AS _notes

    FROM  
        reached_accounts_data
    WHERE 
        _totalsitevisits > 0
        
),

-- Get video views engagement for Intentsify
intentsify_video_views AS (

    SELECT

        _domain,
        _date AS _timestamp,
        'Intentsify Video Views' AS _engagement,
        'Intentsify' AS _channel,
        _campaignname AS _description,
        _videoviews AS _notes

    FROM  
        reached_accounts_data
    WHERE 
        _videoviews > 0

),

-- Get searched topics engagement for Intentsify
intentsify_searched_topics AS (

    SELECT
        
        * EXCEPT(_topic_list, _topic, _notes),
        _topic AS _description,
        _notes

    FROM (

        SELECT

            _account AS _domain,

            CASE 
                WHEN main._date LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', main._date)
                ELSE PARSE_DATE('%F', main._date)
            END 
            AS _timestamp,

            'Intentsify Searched Topics' AS _engagement,
            'Intentsify' AS _channel, 

            SPLIT(main._topics, ', ') AS _topic_list,
            1 AS _notes

        FROM  
            `abbyy_mysql.db_its_acc_topic_ia` main

        JOIN 
            `abbyy_mysql.db_campaign_segment` side
        
        USING(_campaignid)

        WHERE 
            side._sdc_deleted_at IS NULL

    ),

    UNNEST(_topic_list) AS _topic

),

-- Only activities involving target accounts are considered
combined_data AS (

    SELECT DISTINCT 

        target_accounts.*,
        activities.* EXCEPT(_domain)
        
    FROM (

        SELECT * FROM intentsify_campaign_reached 
        UNION DISTINCT
        SELECT * FROM intentsify_ad_clicks 
        UNION DISTINCT
        SELECT * FROM intentsify_web_visits
        UNION DISTINCT
        SELECT * FROM intentsify_video_views
        UNION DISTINCT
        SELECT * FROM intentsify_searched_topics
        
    ) activities

    JOIN
        target_accounts

    ON 
        activities._domain = target_accounts._6sensedomain

),

-- Get accumulated values for each engagement
accumulated_engagement_values AS (

    SELECT
        *,

        -- The aggregated values
        SUM(CASE WHEN _engagement = 'Intentsify Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _6sensedomain) AS _total_int_campaign_reached,
        SUM(CASE WHEN _engagement = 'Intentsify Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _6sensedomain) AS _total_int_ad_clicks,
        SUM(CASE WHEN _engagement = 'Intentsify Website Visits' THEN _notes ELSE 0 END) OVER(PARTITION BY _6sensedomain) AS _total_int_website_visits,
        SUM(CASE WHEN _engagement = 'Intentsify Video Views' THEN _notes ELSE 0 END) OVER(PARTITION BY _6sensedomain) AS _total_int_video_views,
        SUM(CASE WHEN _engagement = 'Intentsify Searched Topics' THEN _notes ELSE 0 END) OVER(PARTITION BY _6sensedomain) AS _total_int_searched_topics

    FROM 
        combined_data
        
)

SELECT * FROM accumulated_engagement_values;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- Opportunity Influenced + Accelerated

CREATE OR REPLACE TABLE `abbyy.opportunity_influenced_accelerated` AS

-- Get account engagements of target account 
WITH target_account_engagements AS (

    SELECT DISTINCT 

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain, 
        _6qa_date,
        _engagement, 
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        _notes AS _eng_notes,

        CASE
            WHEN _engagement LIKE '%6sense%' THEN '6sense'
            WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
            WHEN _engagement LIKE '%Intentsify%' THEN 'Intentsify'
        END 
        AS _channel

    FROM 
        `abbyy.db_6sense_engagement_log` 

),

-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
opps_created AS (

    SELECT DISTINCT 

        _accountid AS _account_id, 
        _accountname AS _account_name,
        _website AS _domain,
        _shippingcountry AS _country,
        _opportunityid AS _opp_id,
        _opportunityname AS _opp_name,
        _opportunityowner AS _opp_owner_name,
        _opptype AS _opp_type,

        CASE 
            WHEN _createddate = '' 
            THEN CAST(NULL AS DATE)
            WHEN _createddate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _createddate)
            ELSE PARSE_DATE('%F', _createddate)
        END 
        AS _created_date,
        
        CASE 
            WHEN _closedate = '' 
            THEN CAST(NULL AS DATE)
            WHEN _closedate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _closedate)
            ELSE PARSE_DATE('%F', _closedate)
        END 
        AS _closed_date,
        
        CASE
            WHEN _amount != ''
            THEN ROUND(CAST(_amount AS FLOAT64), 2)
            ELSE CAST(NULL AS FLOAT64)
        END 
        AS _amount,

        CASE
            WHEN _salesarrconverted != ''
            THEN ROUND(CAST(_salesarrconverted AS FLOAT64), 2)
            ELSE CAST(NULL AS FLOAT64)
        END 
        AS _arr

    FROM 
        `abbyy_mysql.db_sf_opportunity` 

),

-- Get all historical stages of opp
-- Perform necessary cleaning of the data
opps_historical_stage AS (

    SELECT DISTINCT

        _opportunityid AS _opp_id,

        CASE 
            WHEN _editdate LIKE '%/%'
            THEN PARSE_TIMESTAMP('%m/%e/%Y %R', _editdate)
            ELSE PARSE_TIMESTAMP('%F', _editdate)
        END 
        AS _historical_stage_change_timestamp, 

        CASE 
            WHEN _editdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', SPLIT(_editdate, ' ')[ORDINAL(1)])
            ELSE PARSE_DATE('%F', SPLIT(_editdate, ' ')[ORDINAL(1)])
        END 
        AS _historical_stage_change_date, 

        _newvalue AS _next_stage,
        CAST(LEFT(_newvalue, 1) AS INT64) AS _next_stage_prob,
        _oldvalue AS _previous_stage,

        CASE
            WHEN _oldvalue = 'Negotiation/Review' THEN 6
            ELSE CAST(LEFT(_oldvalue, 1) AS INT64) 
        END 
        AS _previous_stage_prob

    FROM 
        `abbyy_mysql.db_sf_opportunity_history` 

),

-- There are several stages that can occur on the same day
-- Get unique stage on each day 
unique_opps_historical_stage AS (

    SELECT
        * EXCEPT(_rownum),

        -- Setting the rank of the historical stage based on stage change date
        ROW_NUMBER() OVER(

            PARTITION BY  
                _opp_id
            ORDER BY 
                _historical_stage_change_date DESC

        )
        AS _stage_rank

    FROM (

        SELECT
            *,
            
            -- Those on same day are differentiated by timestamp
            ROW_NUMBER() OVER(

                PARTITION BY  
                    _opp_id,
                    _historical_stage_change_date
                ORDER BY 
                    _historical_stage_change_timestamp DESC

            )
            AS _rownum

        FROM 
            opps_historical_stage

    )
    WHERE
        _rownum = 1

),

-- Generate a log to store stage history from latest to earliest
get_aggregated_stage_history_text AS (

    SELECT
        *,

        STRING_AGG( 
            CONCAT(
                '[ ', _historical_stage_change_date, ' ]',
                ' : ', _next_stage
            ),
            '; '
        ) 
        OVER(
            
            PARTITION BY 
                _opp_id
            ORDER BY 
                _stage_rank

        )
        AS _stage_history

    FROM 
        unique_opps_historical_stage

),

-- Obtain the current stage and the stage date in this CTE 
get_current_stage_and_date AS (

    SELECT
        *,

        CASE 
            WHEN _stage_rank = 1 THEN _historical_stage_change_date
        END  
        AS _stage_change_date,

        CASE 
            WHEN _stage_rank = 1 THEN _next_stage
        END  
        AS _current_stage

    FROM 
        get_aggregated_stage_history_text

),

-- Add the stage related fields to the opps data
opps_history AS (

    SELECT

        main.*,
        
        -- Fill the current stage and date for an opp
        -- Will be the same in each row of an opp
        MAX(side._stage_change_date) OVER (PARTITION BY side._opp_id) AS _stage_change_date,
        MAX(side._current_stage) OVER (PARTITION BY side._opp_id) AS _current_stage,

        -- Set the stage history to aid crosscheck
        MAX(side._stage_history) OVER (PARTITION BY side._opp_id) AS _stage_history,

        -- The stage and date fields here represent those of each historical stage
        -- Will be different in each row of an opp
        side._historical_stage_change_date,
        side._next_stage AS _historical_stage,

        -- Set the stage movement 
        CASE
            WHEN side._previous_stage_prob > side._next_stage_prob
            THEN 'Downward' 
            ELSE 'Upward'
        END 
        AS _stage_movement

    FROM
        opps_created AS main

    JOIN 
        get_current_stage_and_date AS side

    ON 
        main._opp_id = side._opp_id

),

-- Tie opportunities with stage history and account engagements
combined_data AS (

    SELECT

        opp.*,
        act.*,

        CASE
            WHEN act._engagement IS NOT NULL
            THEN true 
        END 
        AS _is_matched_opp

    FROM 
        opps_history AS opp

    LEFT JOIN 
        target_account_engagements AS act
        
    ON (
            opp._domain = act._6sensedomain
        AND 
            LENGTH(opp._domain) > 1
        AND 
            LENGTH(act._6sensedomain) > 1
    )
        
    OR (
            LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
        AND 
            LENGTH(opp._account_name) > 1
        AND 
            LENGTH(act._6sensecompanyname) > 1
    )
        
),

-- Label the activty that influenced the opportunity
set_influencing_activity AS (

    SELECT

        *,

        CASE 
            WHEN 
                DATE(_eng_timestamp) 
                    BETWEEN 
                        DATE_SUB(_created_date, INTERVAL 90 DAY) 
                    AND 
                        DATE(_created_date)                     
            THEN true 
        END 
        AS _is_influencing_activity

    FROM 
        combined_data

),

-- Mark every other rows of the opportunity as influenced 
-- If there is at least one influencing activity
label_influenced_opportunity AS (
    
    SELECT

        *,

        MAX(_is_influencing_activity) OVER(
            PARTITION BY _opp_id
        )
        AS _is_influenced_opp

    FROM 
        set_influencing_activity

),

-- Label accounts that became 6QA before the influenced opportunity was created
label_6qa_before_influenced_opportunity AS (

    SELECT

        *,

        CASE
            WHEN 
                _is_influenced_opp IS NOT NULL
            AND 
                _6qa_date < _created_date
            -- AND 
            --     DATE(_6qa_date) 
            --         BETWEEN 
            --             DATE(_created_date)
            --         AND 
            --             DATE_ADD(_created_date, INTERVAL 90 DAY)                       
            THEN true 
        END 
        AS _is_6qa_before_influenced_opp

    FROM 
        label_influenced_opportunity

),

-- Label accounts that became 6QA after the influenced opportunity was created
label_6qa_after_influenced_opportunity AS (

    SELECT

        *,

        CASE
            WHEN 
                _is_influenced_opp IS NOT NULL
            AND 
                _6qa_date > _created_date
            -- AND 
            --     DATE(_6qa_date) 
            --         BETWEEN 
            --             DATE(_created_date)
            --         AND 
            --             DATE_ADD(_created_date, INTERVAL 90 DAY)                       
            THEN true 
        END 
        AS _is_6qa_after_influenced_opp

    FROM 
        label_6qa_before_influenced_opportunity

),

-- Label the activty that accelerated the opportunity
set_accelerating_activity AS (

    SELECT 

        *,
        
        CASE 
            WHEN 
                _is_influenced_opp IS NULL
            AND 
                _eng_timestamp > _created_date 
            AND 
                _eng_timestamp <= _historical_stage_change_date
            AND 
                _stage_movement = 'Upward'

            THEN true
        END 
        AS _is_accelerating_activity

    FROM
        label_6qa_after_influenced_opportunity

),

-- Mark every other rows of the opportunity as accelerated 
-- If there is at least one accelerating activity
label_accelerated_opportunity AS (
    
    SELECT
    
        *,

        MAX(_is_accelerating_activity) OVER(
            PARTITION BY _opp_id
        )
        AS _is_accelerated_opp

    FROM 
        set_accelerating_activity

),

-- Label the activty that accelerated an influenced opportunity
set_accelerating_activity_for_influenced_opportunity AS (

    SELECT 

        *,
        
        CASE 
            WHEN 
                _is_influenced_opp IS NOT NULL
            AND 
                _eng_timestamp > _created_date 
            AND 
                _eng_timestamp <= _historical_stage_change_date
            AND 
                _stage_movement = 'Upward'

            THEN true
        END 
        AS _is_later_accelerating_activity

    FROM
        label_accelerated_opportunity

),

-- Mark every other rows of the opportunity as infuenced cum accelerated 
-- If there is at least one accelerating activity for the incluenced opp
label_influenced_opportunity_that_continue_to_accelerate AS (
    
    SELECT
    
        *,

        MAX(_is_later_accelerating_activity) OVER(
            PARTITION BY _opp_id
        )
        AS _is_later_accelerated_opp

    FROM 
        set_accelerating_activity_for_influenced_opportunity

),

-- Mark opportunities that were matched but werent influenced or accelerated or influenced cum accelerated as stagnant 
label_stagnant_opportunity AS (

    SELECT
        *,

        CASE
            WHEN 
                _is_matched_opp = true 
            AND 
                _is_influenced_opp IS NULL 
            AND 
                _is_accelerated_opp IS NULL 
            AND 
                _is_later_accelerated_opp IS NULL
            THEN
                true 
        END 
        AS _is_stagnant_opp

    FROM 
        label_influenced_opportunity_that_continue_to_accelerate

),


-- Get the latest stage of each opportunity 
-- While carrying forward all its boolean fields' value caused by its historical changes 
latest_stage_opportunity_only AS (

    SELECT
        * EXCEPT(_rownum)
    FROM (

        SELECT DISTINCT

            -- Remove fields that are unique for each historical stage of opp
            * EXCEPT(
                _historical_stage_change_date,
                _historical_stage,
                _stage_movement
            ),

            -- For removing those with values in the activity boolean fields
            -- Different historical stages may have caused the influencing or accelerating
            -- This is unlike the opportunity boolean that is uniform among the all historical stage of opp 
            ROW_NUMBER() OVER(
                PARTITION BY 
                    _opp_id,
                    _eng_timestamp,
                    _engagement,
                    _eng_description
                ORDER BY 
                    _is_influencing_activity DESC,
                    _is_accelerating_activity DESC,
                    _is_later_accelerating_activity DESC
            )
            AS _rownum

        FROM 
            label_stagnant_opportunity
    
    )
    WHERE _rownum = 1

)

SELECT * FROM latest_stage_opportunity_only;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- Opportunity Influenced + Accelerated Without Engagements

CREATE OR REPLACE TABLE `abbyy.opportunity_summarized` AS

-- Opportunity information are duplicated by channel field which has ties to engagement
-- The influencing and accelerating boolean fields together with the channel are unique
-- Remove the duplicate channels and prioritize the channels with boolean values
SELECT DISTINCT
    
    _account_id,
    _account_name,
    _country,
    _domain,
    _6qa_date,
    _opp_id,
    _opp_name,
    _opp_owner_name,
    _opp_type,
    _created_date,
    _closed_date,
    _amount,
    _arr,
    _stage_change_date,
    _current_stage,
    _stage_history,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _is_matched_opp,
    _is_influenced_opp,

    MAX(_is_influencing_activity) OVER(
        PARTITION BY 
            _opp_id,
            _channel
    )
    AS _is_influencing_activity,

    _is_6qa_before_influenced_opp,
    _is_6qa_after_influenced_opp,
    _is_accelerated_opp,

    MAX(_is_accelerating_activity) OVER(
        PARTITION BY 
            _opp_id,
            _channel
    )
    AS _is_accelerating_activity,

    _is_later_accelerated_opp,

    MAX(_is_later_accelerating_activity) OVER(
        PARTITION BY 
            _opp_id,
            _channel
    )
    AS _is_later_accelerating_activity,

    _is_stagnant_opp,
    _channel

FROM 
    `abbyy.opportunity_influenced_accelerated`;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- Create a separate table to store Intentsify's ad data 

CREATE OR REPLACE TABLE `abbyy.db_intentsify_ad_performance` AS

-- Get all ad lvl data
WITH ad_data AS (

    SELECT  

        PARSE_DATE('%m/%e/%Y', main._date) AS _date,
        main._campaignid AS _campaign_id,
        main._creativename AS _ad_variation,
        CAST(REGEXP_REPLACE(main._frequency, r',', '') AS DECIMAL) AS _frequency,
        CAST(REGEXP_REPLACE(main._impressions, r',', '') AS INT64) AS _impressions,
        CAST(REGEXP_REPLACE(side._total, r',', '') AS INT64) AS _clicks,
        CAST(REGEXP_REPLACE(side._awarenessclicks, r',', '') AS INT64) AS _awareness_clicks,
        CAST(REGEXP_REPLACE(side._interestclicks, r',', '') AS INT64) AS _interest_clicks,
        CAST(REGEXP_REPLACE(side._considerationclicks, r',', '') AS INT64) AS _consideration_clicks,
        CAST(REGEXP_REPLACE(side._decisionclicks, r',', '') AS INT64) AS _decision_clicks

    FROM 
        `abbyy_mysql.db_its_creative_dp` main

    -- For getting clicks number for each ad
    JOIN 
        `abbyy_mysql.db_its_creative_clicks_dp` side
    ON 
        main._date = side._date
    AND 
        main._campaignid = side._campaignid
    AND 
        main._creativename = side._creative
    
),

-- Add airtable fields into the mix
add_airtable_fields AS (

    SELECT 
        
        main.*,
        side.* EXCEPT(_campaign_id),
        extra.* EXCEPT(_campaign_id, _ad_variation)

    FROM 
        ad_data AS main

    LEFT JOIN (

        SELECT DISTINCT 

            _campaignid AS _campaign_id, 
            _campaignname AS _campaign_name,
            _campaigntype AS _campaign_type

        FROM
            `abbyy_mysql.db_campaign_segment`
        
        WHERE 
            _campaigntype = 'Intentsify'


    ) side

    USING(_campaign_id)

    LEFT JOIN (

        SELECT DISTINCT 

            _adscreenshot AS _ad_screenshot,
            CAST(_advariation AS STRING) AS _ad_variation,
            CAST(_campaignid AS STRING) AS _campaign_id,
            _status AS _campaign_status

        FROM 
            `abbyy_mysql.db_airtable_6sense_campaign` 

    ) extra

    USING(_campaign_id, _ad_variation)

)

SELECT * FROM add_airtable_fields;


---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

-- Create a separate table to store Intentsify's campaign data 

CREATE OR REPLACE TABLE `abbyy.db_intentsify_campaign_performance` AS

-- Get all campaign lvl data
WITH campaign_data AS (

    SELECT  

        PARSE_DATE('%m/%e/%Y', main._date) AS _date,
        main._campaignid AS _campaign_id,

        -- Campaign metric fields
        CAST(REGEXP_REPLACE(main._overallspend, r'\$|,', '') AS DECIMAL) AS _spend,
        CAST(REGEXP_REPLACE(main._totalimpressions, r',', '') AS DECIMAL) AS _impressions,
        CAST(REGEXP_REPLACE(main._totalclicks, r',', '') AS INT64) AS _clicks,
        CAST(REGEXP_REPLACE(main._totalconversions, r',', '') AS INT64) AS _conversions,
        CAST(REGEXP_REPLACE(main._siteengagement, r',', '') AS INT64) AS _site_engagement,

        -- Intent metric fields
        CAST(REGEXP_REPLACE(side._totalactiveaccounts, r',', '') AS INT64) AS _active_accounts,
        CAST(REGEXP_REPLACE(side._awarenessaccounts, r',', '') AS INT64) AS _awareness_accounts,
        CAST(REGEXP_REPLACE(side._interestaccounts, r',', '') AS INT64) AS _interest_accounts,
        CAST(REGEXP_REPLACE(side._considerationaccounts, r',', '') AS INT64) AS _consideration_accounts,
        CAST(REGEXP_REPLACE(side._decisionaccounts, r',', '') AS INT64) AS _decision_accounts,

        -- Engagement related fields
        CAST(REGEXP_REPLACE(extra._targetaccounts, r',', '') AS INT64) AS _target_accounts,

        CAST(REGEXP_REPLACE(extra._accountsreached, r',', '') AS INT64) AS _reached_accounts,
        CAST(REGEXP_REPLACE(extra._awarenessaccountsreached, r',', '') AS INT64) AS _awareness_reached_accounts,
        CAST(REGEXP_REPLACE(extra._interestaccountsreached, r',', '') AS INT64) AS _interest_reached_accounts,
        CAST(REGEXP_REPLACE(extra._considerationaccountsreached, r',', '') AS INT64) AS _consideration_reached_accounts,
        CAST(REGEXP_REPLACE(extra._decisionaccountsreached, r',', '') AS INT64) AS _decision_reached_accounts,

        CAST(REGEXP_REPLACE(extra._accountsengaged, r',', '') AS INT64) AS _engaged_accounts,
        CAST(REGEXP_REPLACE(extra._awarenessaccountsengaged, r',', '') AS INT64) AS _awareness_engaged_accounts,
        CAST(REGEXP_REPLACE(extra._interestaccountsengaged, r',', '') AS INT64) AS _interest_engaged_accounts,
        CAST(REGEXP_REPLACE(extra._considerationaccountsengaged, r',', '') AS INT64) AS _consideration_engaged_accounts,
        CAST(REGEXP_REPLACE(extra._decisionaccountsengaged, r',', '') AS INT64) AS _decision_engaged_accounts,

        CAST(REGEXP_REPLACE(extra._accountswithconversion, r',', '') AS INT64) AS _converted_accounts

    FROM 
        `abbyy_mysql.db_its_campaign_dp` main

    -- For getting intent related account numbers
    JOIN 
        `abbyy_mysql.db_its_accounts_ia` side
    ON 
        main._date = side._date
    AND 
        main._campaignid = side._campaignid

    -- For getting engagement related account numbers
    JOIN 
        `abbyy_mysql.db_its_accounts_dp` extra
    ON 
        main._date = extra._date
    AND 
        main._campaignid = extra._campaignid
    
),

-- Add airtable fields into the mix
-- Reduce campaign lvl numbers
add_airtable_fields AS (

    SELECT 
        
        main.*,
        side.* EXCEPT(_campaign_id)

    FROM 
        campaign_data AS main

    LEFT JOIN (

        SELECT DISTINCT 

            _campaignid AS _campaign_id, 
            _campaignname AS _campaign_name,
            _campaigntype AS _campaign_type

        FROM
            `abbyy_mysql.db_campaign_segment`
        
        WHERE 
            _campaigntype = 'Intentsify'


    ) side

    USING(_campaign_id)

)

SELECT * FROM add_airtable_fields;


