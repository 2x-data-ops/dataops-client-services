/*
    Deprecated - 6Sense Impact Dashboard will now be separated to smaller chunks where one script contains one table
    Kindly only use this script as reference
*/
-- 6sense Buying Stage Movement

CREATE OR REPLACE TABLE `sandler.db_6sense_buying_stages_movement` AS

-- Set buying stages and their order
WITH stage_order AS (

    SELECT 'Target' AS _buying_stage, 1 AS _order 
    UNION ALL
    SELECT 'Awareness' AS _buying_stage, 2 
    UNION ALL
    SELECT 'Consideration' AS _buying_stage, 3 
    UNION ALL
    SELECT 'Decision' AS _buying_stage, 4 
    UNION ALL
    SELECT 'Purchase' AS _buying_stage, 5

),

-- Get buying stage data
buying_stage_data AS (

    SELECT DISTINCT
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _buyingstageend AS _buying_stage,

        CASE 
            WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate)
        END 
        AS _activities_on
        
    FROM 
        `sandler_mysql.db_account_initial_buying_stage`

),

-- Get first ever buying stage for each account
first_ever_buying_stage AS (

    SELECT DISTINCT 
        _activities_on,
        _6sensecountry,
        _6sensedomain, 
        _6sensecompanyname,
        _buying_stage,
        'Initial' AS _source,
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM
        buying_stage_data
    JOIN (

        SELECT DISTINCT 
            _6sensecountry, 
            _6sensecompanyname, 
            MIN(_activities_on) AS _activities_on 
        FROM 
            buying_stage_data 
        GROUP BY 
            1, 2

    ) 
    USING(
        _6sensecountry, 
        _6sensecompanyname, 
        _activities_on
    )
    ORDER BY
        1 DESC

),

-- Get every other buying stage for each account
every_other_buying_stage AS (

    SELECT 
        * 
    FROM (

        SELECT DISTINCT 
            _activities_on,
            _6sensecountry,
            _6sensedomain, 
            _6sensecompanyname,
            _buying_stage,
            'Non Initial' AS _source,
            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
        FROM
            buying_stage_data
            
    )
    -- Exclude those that are first ever stages
    WHERE 
        CONCAT(_country_account, _activities_on) NOT IN (

            SELECT DISTINCT 
                CONCAT(_country_account, MIN(_activities_on)) 
            FROM 
                first_ever_buying_stage 
            GROUP BY 
                _country_account
        
        )

),

-- Combine both first ever data and every other data
historical_buying_stage AS (

    SELECT * FROM first_ever_buying_stage 
    UNION DISTINCT
    SELECT * FROM every_other_buying_stage

),

-- Get the current stage and previous stage for each historical record of an account
set_buying_stage_order AS (

    SELECT DISTINCT 
        main.* EXCEPT(_current_stage, _prev_stage),
        main._current_stage,

        IF(
            _activities_on = (
                MIN(_activities_on) OVER(
                    PARTITION BY _6sensedomain, _6sensecountry 
                    ORDER BY _activities_on
                )
            ) 
            AND _prev_stage IS NULL, 
            _current_stage, 
            _prev_stage  
        ) 
        AS _prev_stage,

        curr._order AS _curr_order,

        IF(
            _activities_on = (
                MIN(_activities_on) OVER(
                    PARTITION BY _6sensedomain, _6sensecountry 
                    ORDER BY _activities_on
                ) 
            )
            AND _prev_stage IS NULL, 
            curr._order, 
            prev._order  
        ) 
        AS _prev_order

    FROM (

        SELECT DISTINCT 
            _6sensecountry,
            _6sensedomain, 
            _6sensecompanyname,
            _buying_stage AS _current_stage,
            _activities_on,

            LAG(_buying_stage) OVER(
                PARTITION BY _6sensedomain 
                ORDER BY _activities_on ASC
            ) AS _prev_stage,

            _source,
            _country_account
        FROM 
            historical_buying_stage

    ) main
    LEFT JOIN
        stage_order AS curr 
    ON 
        main._current_stage = curr._buying_stage
    LEFT JOIN
        stage_order AS prev 
    ON 
        main._prev_stage = prev._buying_stage

),

-- Set movement of each historical record an account
set_movement AS (

    SELECT * EXCEPT(_order) 
    FROM (

        SELECT DISTINCT 
            *,

            IF(
                _curr_order > _prev_order, 
                "+ve", 
                IF(
                    _curr_order < _prev_order, 
                    "-ve", 
                    "Stagnant"
                )
            ) 
            AS _movement,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC
            ) 
            AS _order

        FROM
            set_buying_stage_order
        ORDER BY 
            _activities_on DESC

    )
    WHERE
        _order = 1
    ORDER BY
        _country_account

)

SELECT * FROM set_movement;


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------


-- 6sense Account Current State

CREATE OR REPLACE TABLE `sandler.db_6sense_account_current_state` AS

-- Get all target accounts and their segments
WITH target_accounts AS (

    SELECT DISTINCT 
        main.*
    FROM (
    
        SELECT DISTINCT 

            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            _industrylegacy AS _6senseindustry,
            _6senseemployeerange,
            _6senserevenuerange,
            'Target' AS _source,

            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END 
            AS _added_on,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM 
            `sandler_mysql.db_segment_target_accounts`

    ) main

    -- Get the earliest date of appearance of each account
    JOIN (

        SELECT DISTINCT 

            MIN(
                CASE 
                    WHEN _extractdate LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                    ELSE PARSE_DATE('%F', _extractdate)
                END 
            ) 
            AS _added_on,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
        FROM
            `sandler_mysql.db_segment_target_accounts`
        GROUP BY 
            2
        ORDER BY 
            1 DESC

    ) scenario 

    ON 
        main._country_account = scenario._country_account 
    AND 
        main._added_on = scenario._added_on
    
    -- Add SEM accounts that do not exist in the target accounts
    UNION DISTINCT

    SELECT DISTINCT 

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _industrylegacy AS _6senseindustry,
        _6senseemployeerange,
        _6senserevenuerange,
        'SEM' AS _source,

        MIN (
            CASE 
                WHEN _date LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _date)
                ELSE PARSE_DATE('%F', _date)
            END 
        )
        AS _added_on,

        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

    FROM 
        `sandler_mysql.db_sem_engagement_new`
    
    WHERE
        _sdc_deleted_at IS NULL

    GROUP BY
        1, 2, 3, 4, 5, 6

),

-- Label accounts that are part of dossier
set_dossier AS (

    SELECT DISTINCT 
        main.*,

        CASE 
            WHEN dossier._dossierbatch IS NOT NULL 
            THEN true 
        END 
        AS _is_dossier

    FROM 
        target_accounts AS main
    LEFT JOIN 
        `sandler_mysql.db_6sense_account_dossier_sent` dossier 
    ON 
        main._6sensedomain = dossier._domain

),

-- Set the ICP category of the account
set_icp AS (

    SELECT DISTINCT 
        main.*,
        icp._icptier AS _icp_tier

    FROM 
        set_dossier AS main
    LEFT JOIN 
        `sandler_mysql.db_static_target_accounts` icp 
    
    USING(_6sensedomain)

),

-- Get date when account had first impression
reached_related_info AS (

    SELECT
        * EXCEPT(rownum)
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
                PARTITION BY CONCAT(_6sensecountry, _6sensecompanyname) 
            )
            AS _first_impressions,

            CASE
                WHEN _websiteengagement = '-'
                THEN CAST(NULL AS STRING)
                ELSE _websiteengagement
            END 
            AS _website_engagement,

            ROW_NUMBER() OVER(
                PARTITION BY CONCAT(_6sensecountry, _6sensecompanyname) 
                ORDER BY (
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                        ELSE PARSE_DATE('%F', _latestimpression)
                    END
                ) DESC
            )
            AS rownum,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM 
            `sandler_mysql.db_campaign_reached_account_new`
        WHERE 
            _campaignid IN (

                SELECT DISTINCT 
                    _campaignid 
                FROM 
                    `sandler_mysql.db_airtable_6sense_segment`
                WHERE 
                    _sdc_deleted_at IS NULL

            )

    )
    WHERE rownum = 1

),

-- Get the date when account first became a 6QA
six_qa_related_info AS (

    SELECT
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            main._6qa_date,
            main._is_6qa,
            main._is_6qa_plus,
            side._country_account,
            
            ROW_NUMBER() OVER(
                PARTITION BY side._country_account
                ORDER BY main._6qa_date 
            )
            AS rownum

        -- Get 6QA date from hubspot
        FROM (

            SELECT DISTINCT

                property_domain.value AS _domain,
                property_name.value AS _company,
                property_country.value AS _country,

                DATE(TIMESTAMP_MILLIS(CAST(property_sixsense_account_sixqa_start_date.value AS INT64))) AS _6qa_date,

                CASE 
                    WHEN property_sixsense_account_sixqa.value = '1'
                    THEN true 
                END 
                AS _is_6qa,


                CASE 
                    WHEN property_n2x___6qa__status.value = 'true'
                    THEN true 
                END 
                AS _is_6qa_plus
                
            FROM 
                `x-marketing.sandler_hubspot.companies`
            WHERE 
                property_sixsense_account_sixqa.value = '1'
            OR 
                property_n2x___6qa__status.value = 'true'

        ) main 

        JOIN 
            target_accounts AS side 
        ON (
                main._domain = side._6sensedomain 
            AND 
                main._country = side._6sensecountry
        )
        OR (
                LOWER(main._company) = LOWER(side._6sensecompanyname) 
            AND 
                main._country = side._6sensecountry
        )

    )
    WHERE
        rownum = 1 

),

-- Get the buying stage info each account
buying_stage_related_info AS (

    SELECT DISTINCT 
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            _prev_stage,
            _prev_order,
            _current_stage,
            _curr_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC 
            ) 
            AS rownum

        FROM
            `sandler.db_6sense_buying_stages_movement`

    )
    WHERE 
        rownum = 1

),

-- Attach all other data parts to target accounts
combined_data AS (

    SELECT DISTINCT 

        target.*, 
        reached.* EXCEPT(_country_account),
        six_qa.* EXCEPT(_country_account),
        stage.* EXCEPT(_country_account)   

    FROM
        set_icp AS target

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


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

-- 6sense Engagement Log

CREATE OR REPLACE TABLE `sandler.db_6sense_engagement_log` AS

-- Get all target accounts and their unique info
WITH target_accounts AS (

    SELECT * FROM `sandler.db_6sense_account_current_state`

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
        side._campaigntype,
        side._campaignname,
        CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account
    
    FROM 
        `sandler_mysql.db_campaign_reached_account_new` main
    
    JOIN (

        SELECT DISTINCT 
            _campaignid, 
            _name AS _campaignname,  
            _campaigntype
        FROM
            `sandler_mysql.db_airtable_6sense_segment`
        WHERE 
            _sdc_deleted_at IS NULL

    ) side

    USING(_campaignid)

),

-- Get campaign reached engagement for 6sense
sixsense_campaign_reached AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,

        MIN(_latestimpression) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _latestimpression
        ) 
        AS _timestamp,

        '6Sense Campaign Reached' AS _engagement,
        '6sense' AS _channel, 
        _campaignname AS _description, 
        1 AS _notes

    FROM
        reached_accounts_data
    WHERE
        _campaigntype = '6sense Advertising'

),

-- Get ad clicks engagement for 6sense
sixsense_ad_clicks AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6Sense Ad Clicks' AS _engagement, 
            '6sense' AS _channel,
            _campaignname AS _description,  
            _clicks AS _notes,

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
        AND
            _campaigntype = '6sense Advertising'

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

            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6Sense Influenced Form Fill' AS _engagement, 
            '6sense' AS _channel,
            _campaignname AS _description,  
            _influencedformfills AS _notes,

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
        AND
            _campaigntype = '6sense Advertising'

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

-- Get campaign reached engagement for Linkedin
linkedin_campaign_reached AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,

        MIN(_latestimpression) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _latestimpression
        ) 
        AS _timestamp,

        'LinkedIn Campaign Reached' AS _engagement,
        'LinkedIn' AS _channel, 
        _campaignname AS _description, 
        1 AS _notes

    FROM
        reached_accounts_data
    WHERE
        _campaigntype = 'LinkedIn'

),

-- Get ad clicks engagement for Linkedin
linkedin_ad_clicks AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            'LinkedIn Ad Clicks' AS _engagement, 
            'LinkedIn' AS _channel,
            _campaignname AS _description,  
            _clicks AS _notes,

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
        AND
            _campaigntype = 'LinkedIn'

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

-- Get form fills engagement for Linkedin
linkedin_form_fills AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            'LinkedIn Influenced Form Fill' AS _engagement, 
            'LinkedIn' AS _channel,
            _campaignname AS _description,  
            _influencedformfills AS _notes,

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
        AND
            _campaigntype = 'LinkedIn'

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

-- Get SEM engagement 
sem_engagements AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account, 
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,

        CASE 
            WHEN _date LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _date)
            ELSE PARSE_DATE('%F', _date)
        END 
        AS _timestamp,

        'SEM Engagement' AS _engagement,
        'SEM' AS _channel,
        _utmcampaign AS _description, 
        1 AS _notes

    FROM
        `sandler_mysql.db_sem_engagement_new` 
    WHERE
        _sdc_deleted_at IS NULL

),

-- Prep the sales intelligence data for use later
sales_intelligence_data AS (

    SELECT 

        _activitytype,
        _activitytarget,
        _contactemail,
        _accountname,
        _country,
        _city,
        _state,
        
        CASE 
            WHEN _date LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _date)
            ELSE PARSE_DATE('%F', _date)
        END  
        AS _date,

        CONCAT(_country, _accountname) AS _country_account,
        COUNT(*) AS _count

    FROM 
        `sandler_mysql.db_sales_intelligence_activities`
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8

),

-- Get campaign reached engagements
sales_intelligence_campaign_reached AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        _activitytype AS _engagement, 
        'Sales Intelligence' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype LIKE '%Reached%'

),

-- Get web visits engagement
web_visits AS (

    SELECT DISTINCT

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        '6Sense Web Visits' AS _engagement, 
        '6sense' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype LIKE '%Web Visit%'

),

-- Get searched keywords engagements
searched_keywords AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        '6Sense Searched Keywords' AS _engagement, 
        '6sense' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype LIKE '%KW Research%'

),

-- Get email opens and clicks engagements
email_engagements AS (

    SELECT DISTINCT 

        _contactemail AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        CONCAT(_activitytype, 'ed') AS _engagement, 
        'Hubspot' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        REGEXP_CONTAINS(_activitytype,'Email Open|Email Click')

),

-- Get all other new engagements from sales intelligence
other_engagements AS (

    SELECT DISTINCT 

        _contactemail AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        
        CASE 
            WHEN REGEXP_CONTAINS(_activitytype,'Bombora') THEN 'Bombora Topic Surged'
            WHEN REGEXP_CONTAINS(_activitytype,'Form Fill') THEN 'Form Filled'
            WHEN REGEXP_CONTAINS(_activitytype,'Email Reply') THEN 'Email Replied'
            WHEN REGEXP_CONTAINS(_activitytype,'Page Click') THEN 'Webpage Clicked'
            WHEN REGEXP_CONTAINS(_activitytype,'Submit') THEN 'Submitted'
            WHEN REGEXP_CONTAINS(_activitytype,'Video Play') THEN 'Video Played'
            WHEN REGEXP_CONTAINS(_activitytype,'Attend') THEN _activitytype
            WHEN REGEXP_CONTAINS(_activitytype,'Register') THEN _activitytype
            ELSE 'Unclassified Engagement'
        END 
        AS _engagement, 
        
        'Sales Intelligence' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        NOT REGEXP_CONTAINS(_activitytype,'Reached|Web Visit|KW Research|Email Open|Email Click')

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
        SELECT * FROM linkedin_campaign_reached
        UNION DISTINCT
        SELECT * FROM linkedin_ad_clicks
        UNION DISTINCT
        SELECT * FROM linkedin_form_fills
        UNION DISTINCT
        SELECT * FROM sem_engagements
        UNION DISTINCT
        SELECT * FROM sales_intelligence_campaign_reached
        UNION DISTINCT
        SELECT * FROM web_visits
        UNION DISTINCT
        SELECT * FROM searched_keywords
        UNION DISTINCT
        SELECT * FROM email_engagements
        UNION DISTINCT
        SELECT * FROM other_engagements
        
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
        SUM(CASE WHEN _engagement = '6Sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
        SUM(CASE WHEN _engagement = '6Sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
        SUM(CASE WHEN _engagement = '6Sense Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_form_fills,
        SUM(CASE WHEN _engagement = 'LinkedIn Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_campaign_reached,
        SUM(CASE WHEN _engagement = 'LinkedIn Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_ad_clicks,
        SUM(CASE WHEN _engagement = 'LinkedIn Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_form_fills,
        SUM(CASE WHEN _engagement = 'SEM Engagement' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_sem_engagements,
        SUM(CASE WHEN _engagement = '6Sense Web Visits' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_web_visits,
        SUM(CASE WHEN _engagement = '6Sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
        SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
        SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click
    FROM 
        combined_data
        
)

SELECT * FROM accumulated_engagement_values;


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------


-- Opportunity Influenced + Accelerated

CREATE OR REPLACE TABLE `sandler.opportunity_influenced_accelerated` AS

-- Get account engagements of target account 
WITH target_account_engagements AS (

    SELECT DISTINCT 

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain, 
        _engagement, 
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        _notes AS _eng_notes,
        _city,
        _state,
        _is_dossier

    FROM 
        `sandler.db_6sense_engagement_log` 

),

-- Get all generated opportunities
opps_created AS (

    SELECT  

        CAST(companies.companyid AS STRING) AS _account_id, 
        companies.property_name.value AS _account_name,
        companies.property_domain.value AS _domain,
        companies.property_country.value AS _country,

        CASE 
            WHEN property_sixsense_account_sixqa.value = '1'
            THEN true 
        END 
        AS _is_6qa,

        CASE 
            WHEN companies.property_n2x___6qa__status.value = 'true'
            THEN true  
        END 
        AS _is_6qa_plus,

        CAST(deals.dealid AS STRING) AS _opp_id,
        deals.property_dealname.value AS _opp_name, 
        CAST(deals.properties.hubspot_owner_id.value AS STRING) AS _opp_owner_id,
        CONCAT(owners.firstname, ' ', owners.lastname) AS _opp_owner_name,
        
        CASE 
            WHEN deals.property_dealtype.value = 'newbusiness' 
            THEN 'New Business' 
            WHEN deals.property_dealtype.value = 'existingbusiness' 
            THEN 'Existing Business' 
        END 
        AS _type,

        stages.pipeline AS _pipeline,
        DATE(deals.property_createdate.value) AS _create_date,
        DATE(deals.property_closedate.value) AS _close_date,
        CAST(deals.property_amount.value AS INT) AS _amount, 
        CAST(deals.property_hs_acv.value AS INT) AS _acv,
        deals.property_closed_lost_reason.value AS _reason,
        DATE(deals.property_dealstage.timestamp) AS _stage_change_date,
        
        DATE_DIFF(
            CURRENT_DATE(), 
            DATE(deals.property_dealstage.timestamp), 
            DAY
        ) AS _days_in_stage,

        INITCAP(stages.label) AS _current_stage,
        stages.probability AS _current_stage_prob

    FROM 
        `sandler_hubspot.deals` deals
    
    -- UNNEST does a cross join by default
    -- Those with NULL in the unnested value will remove the row entirely after cross joining
    -- Use left join to prevent the row from being removed
    LEFT JOIN
        UNNEST(associations.associatedcompanyids) AS deals_company

    LEFT JOIN 
        `sandler_hubspot.companies` companies
    ON 
        deals_company.value = companies.companyid

    LEFT JOIN 
        `sandler_hubspot.owners` owners 
    ON 
        deals.properties.hubspot_owner_id.value = CAST(owners.ownerid AS STRING)

    JOIN (

        SELECT DISTINCT 
            label AS pipeline,
            stages.value.* 
        FROM 
            `sandler_hubspot.deal_pipelines`, 
            UNNEST(stages) AS stages

    ) stages 

    ON 
        deals.property_dealstage.value = stages.stageid 
    WHERE 
        -- Exclude those of renewal and existing business type
        LOWER(deals.property_dealtype.value) IN ('newbusiness', 'existingbusiness')

    -- Take into account of those opps with no account info yet
    AND (
            LOWER(companies.property_name.value) NOT LIKE '%sandler%'
        OR 
            companies.property_name.value IS NULL
    )

    AND 
        deals.property_pipeline.value IS NOT NULL
    AND 
        deals.isdeleted = false

),

-- Get the previous stage of each opportunity
opps_history AS (

    SELECT
        main.*,
        side._previous_stage,
        side._previous_stage_prob
    FROM
        opps_created AS main

    LEFT JOIN (
        
        SELECT DISTINCT 
            _opportunityID AS _opp_id,
            INITCAP(_stage) AS _current_stage,

            LEAD(INITCAP(_stage)) OVER(
                PARTITION BY _opportunityID 
                ORDER BY _timestamp DESC
            ) 
            AS _previous_stage,
            
            LEAD(_probability) OVER(
                PARTITION BY _opportunityID 
                ORDER BY _timestamp DESC
            ) 
            AS _previous_stage_prob,

        FROM
            `sandler.hubspot_opportunity_stage_history`

    ) AS side

    USING(
        _opp_id,
        _current_stage
    )

),

-- Tie opportunities with account engagements
-- Label active opps (all opps except those closed before May 12 2023)
-- Label matched opps (those with engagements)
combined_data AS (

    SELECT
        opp.*,

        CASE
            WHEN opp._previous_stage_prob > opp._current_stage_prob
            THEN 'Downward' 
            ELSE 'Upward'
        END 
        AS _stageMovement, 

        act.*,

        CASE
            WHEN opp._close_date > '2023-05-12'
            THEN true 
        END 
        AS _is_active_opp, 

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
                        DATE_SUB(_create_date, INTERVAL 90 DAY) 
                    AND 
                        DATE(_create_date) 
                AND 
                    REGEXP_CONTAINS(
                        _engagement, 
                        '6Sense Campaign|6Sense Ad|LinkedIn Campaign|LinkedIn Ad|SEM'
                    )
                    
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

-- Label the activty that accelerated the opportunity
set_accelerating_activity AS (

    SELECT 
        *,
        
        CASE 
            WHEN _opp_id NOT IN (

                SELECT DISTINCT
                    _opp_id
                FROM 
                    label_influenced_opportunity
                WHERE 
                    _is_influenced_opp IS NOT NULL

            )
            AND 
                _eng_timestamp >= _create_date 
            AND 
                _eng_timestamp <= _stage_change_date
            AND 
                _current_stage NOT LIKE 'Closed%'
            AND 
                REGEXP_CONTAINS(
                    _engagement, 
                    '6Sense Campaign|6Sense Ad|LinkedIn Campaign|LinkedIn Ad|SEM'
                )

            THEN true
        END 
        AS _is_accelerating_activity

    FROM
        label_influenced_opportunity

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

-- Mark opportunities that were matched but werent influenced or accelerated as stagnant 
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
            THEN
                true 
        END 
        AS _is_stagnant_opp

    FROM 
        label_accelerated_opportunity

)

SELECT * FROM label_stagnant_opportunity;


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------


-- Opportunity Influenced + Accelerated Without Engagements

CREATE OR REPLACE TABLE `sandler.opportunity_summarized` AS

SELECT DISTINCT
 
    _account_id,
    _account_name,
    _country,
    _domain,
    _is_6qa,
    _is_6qa_plus,
    _opp_id,
    _opp_name,
    _opp_owner_id,
    _opp_owner_name,
    _type,
    _pipeline,
    _create_date,
    _close_date,
    _amount,
    _acv,
    _reason,
    _stage_change_date,
    _days_in_stage,
    _current_stage,
    _current_stage_prob,
    _previous_stage,
    _previous_stage_prob,
    _stageMovement,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _is_dossier,
    _is_active_opp,
    _is_matched_opp,
    _is_influenced_opp,
    _is_influencing_activity,
    _is_accelerated_opp,
    _is_accelerating_activity,
    _is_stagnant_opp,

    CASE
        WHEN _engagement LIKE '%6Sense%' THEN '6sense'
        WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
        WHEN _engagement LIKE '%SEM%' THEN 'SEM'
    END 
    AS _channel

FROM 
    `sandler.opportunity_influenced_accelerated`
;

