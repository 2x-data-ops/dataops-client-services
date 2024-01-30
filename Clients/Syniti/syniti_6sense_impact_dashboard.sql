CREATE
OR
REPLACE
TABLE
    `syniti.db_6sense_buying_stages_movement` AS
WITH sixsense_stage_order AS (
        SELECT
            'Target' AS _buying_stage,
            1 AS _order
        UNION ALL
        SELECT
            'Awareness' AS _buying_stage,
            2 AS _order
        UNION ALL
        SELECT
            'Consideration' AS _buying_stage,
            3 AS _order
        UNION ALL
        SELECT
            'Decision' AS _buying_stage,
            4 AS _order
        UNION ALL
        SELECT
            'Purchase' AS _buying_stage,
            5 AS _order
    ),
    sixsense_buying_stage_data AS (
        SELECT
            DISTINCT ROW_NUMBER() OVER (
                PARTITION BY _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
                ORDER BY
                    CASE
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END DESC
            ) AS _rownum,
            CASE
                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END AS _activities_on,
            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            CONCAT(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            ) AS _country_account,
            '6sense' AS _data_source,
            _buyingstagestart AS _previous_stage,
            _buyingstageend AS _current_stage
        FROM
            `syniti_mysql.syniti_db_accounts_buying_stage`
    ),
    latest_sixsense_buying_stage_with_order_and_movement AS (
        SELECT main.*
            EXCEPT (_rownum),
            prev._order AS _previous_stage_order,
            curr._order AS _current_stage_order,
                CASE
                    WHEN curr._order > prev._order THEN '+ve'
                    WHEN prev._order > curr._order THEN '-ve'
                    ELSE 'Stagnant'
                END AS _movement
        FROM
            sixsense_buying_stage_data AS main
            LEFT JOIN sixsense_stage_order AS prev
                ON main._previous_stage = prev._buying_stage
            LEFT JOIN sixsense_stage_order AS curr
                ON main._current_stage = curr._buying_stage
        WHERE main._rownum = 1
    )
SELECT *
FROM
    latest_sixsense_buying_stage_with_order_and_movement;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ACCOUNT CURRENT STATE-------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE
OR
REPLACE
TABLE
    `syniti.db_6sense_account_current_state` AS
WITH target_accounts AS (
        SELECT DISTINCT main.*

        FROM (
                SELECT DISTINCT
                    _6sensecompanyname,
                    _6sensecountry,
                    _6sensedomain,
                    _industrylegacy AS _6senseindustry,
                    _6senseemployeerange,
                    _6senserevenuerange,
                    CASE
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END AS _added_on,
                    '6sense' AS _data_source,

                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
                FROM
                    `syniti_mysql.syniti_db_target_accounts` 
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

                        CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
                        
                    FROM
                        `syniti_mysql.syniti_db_target_accounts`
                    GROUP BY 
                        2
                    ORDER BY 
                        1 DESC

                ) scenario 

                ON 
                    main._country_account = scenario._country_account 
                AND 
                    main._added_on = scenario._added_on

    ),
    reached_related_info AS (
        SELECT *
        EXCEPT (_rownum)
        FROM (
                SELECT
                    DISTINCT MIN(
                        CASE
                            WHEN _extractdate LIKE '%/%'
                            THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                            ELSE PARSE_DATE('%F', _latestimpression)
                        END
                    ) OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry,
                            _6sensedomain
                        )
                    ) AS _first_impressions,
                    CASE
                        WHEN _websiteengagement = '-' THEN CAST(NULL AS STRING)
                        ELSE _websiteengagement
                    END AS _websiteengagement,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry,
                            _6sensedomain
                        )
                        ORDER BY
                            CASE
                                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                                ELSE PARSE_DATE('%F', _latestimpression)
                            END DESC
                    ) AS _rownum,
                    CONCAT(
                        _6sensecompanyname,
                        _6sensecountry,
                        _6sensedomain
                    ) AS _country_account
        FROM
            `syniti_mysql.syniti_db_campaign_reached_accounts`
        )
    WHERE _rownum = 1
),

six_qa_related_info AS (

    SELECT

        * EXCEPT(_rownum)

    FROM (

        SELECT DISTINCT

            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END 
            AS _6qa_date,

            true _is_6qa,

            ROW_NUMBER() OVER(

                PARTITION BY 
                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) 
                ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC

            )
            AS _rownum,

            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account

        FROM 
            `syniti_mysql.syniti_db_6qa_accounts_list`
    
    )

    WHERE 
        _rownum = 1

),

-- Get buying stage info for each account

buying_stage_related_info AS (

    SELECT DISTINCT 
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            _previous_stage,
            _previous_stage_order,
            _current_stage,
            _current_stage_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC 
            ) 
            AS rownum

        FROM
            `syniti.db_6sense_buying_stages_movement`

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

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------6SENSE ENGAGEMENT LOG-------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


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


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------6SENSE AD PERFORMANCE----------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `syniti.db_6sense_ad_performance` AS

WITH ads AS (
SELECT *
    EXCEPT (_rownum)
    FROM (
        SELECT DISTINCT
            _campaignid AS _campaign_id,
            _name AS _advariation,
            _6senseid AS _adid,
            CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64
                ) AS _spend,
            CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
            CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
            CASE 
                        WHEN _date LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _date)
                        WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
                    END AS _date,
            ROW_NUMBER() OVER (
                    PARTITION BY _campaignid,
                    _6senseid,
                    _date
                    ORDER BY CASE 
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _extractdate)
                        WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate)
                    END
                ) AS _rownum
        FROM `syniti_mysql.syniti_db_daily_campaign_performance`
        WHERE _datatype = 'Ad'
    )
    WHERE _rownum = 1
),
-- Get campaign level fields

campaign_fields AS (
    
    SELECT
        * EXCEPT(_extractdate, _rownum)
    FROM (

        SELECT

            _campaignid AS _campaign_id,
            
            CASE
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END
            AS _extractdate,

            CASE 
                WHEN _startDate LIKE '%/%'
                THEN PARSE_DATE('%m/%d/%Y', _startDate)
                WHEN _startDate LIKE '%-%'
                THEN PARSE_DATE('%d-%h-%y', _startDate)
            END
            AS _start_date,

            CASE 
                WHEN _endDate LIKE '%/%'
                THEN PARSE_DATE('%m/%d/%Y', _endDate)
                WHEN _endDate LIKE '%-%'
                THEN PARSE_DATE('%d-%h-%y', _endDate)
            END
            AS _end_date,

            _status AS _campaign_status,
            _name AS _campaign_name,
            _campaigntype AS _campaign_type,
            
            CASE 
                WHEN _accountsnewlyengagedlifetime = '-'
                THEN 0
                ELSE SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
            END 
            AS _newly_engaged_accounts,

            CASE 
                WHEN _accountswithincreasedengagementlifetime = '-'
                THEN 0
                ELSE SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
            END 
            AS _increased_engagement_accounts,

            ROW_NUMBER() OVER(
                PARTITION BY _campaignid
                ORDER BY 
                    CASE
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC
            ) 
            AS _rownum

        FROM 
            `syniti_mysql.syniti_db_campaign_performance`
        WHERE
            _datatype = 'Campaign'

    )
    WHERE 
        _rownum = 1

),

airtable_fields AS (

    SELECT DISTINCT 

        _campaignid AS _campaign_id, 
        _adid AS _ad_id,
        _adgroup AS _ad_group,
        _screenshot
        
    FROM
        `syniti_mysql.syniti_optimization_airtable_ads_6sense`
    WHERE 
        _campaignid != ''
),
combined_data AS (

    SELECT

        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        ads.*,
        airtable_fields._ad_group,
        airtable_fields._screenshot,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts

    FROM 
        ads

    LEFT JOIN
        airtable_fields 
    ON (
            ads._adid = airtable_fields._ad_id
        AND 
            ads._campaign_id = airtable_fields._campaign_id
    )
    OR (
            airtable_fields._ad_id IS NULL
        AND 
            ads._campaign_id = airtable_fields._campaign_id
    )

    LEFT JOIN 
        campaign_fields
    ON 
        ads._campaign_id = campaign_fields._campaign_id

),

-- Add campaign numbers to each ad
campaign_numbers AS (

    SELECT
        *
    FROM
        combined_data 

    -- Get accounts that are being targeted
    LEFT JOIN (
        
        SELECT DISTINCT

            _campaignid AS _campaign_id,
            COUNT(*) AS _target_accounts

        FROM (

            SELECT DISTINCT 

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `syniti_mysql.syniti_db_target_accounts` main
            
            JOIN 
                `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

        )
        GROUP BY 
            1

    ) target

    USING(_campaign_id)

    -- Get accounts that have been reached
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid AS _campaign_id,
            COUNT(*) AS _reached_accounts

        FROM (

            SELECT DISTINCT 

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `syniti_mysql.syniti_db_target_accounts` main
            
            JOIN 
                `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `syniti_mysql.syniti_db_campaign_reached_accounts` extra

            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain,
                _campaignid
            )
            
        )
        GROUP BY 
            1

    ) reach

    USING(_campaign_id)

    -- Get accounts that are 6QA
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid AS _campaign_id,
            COUNT(*) AS _6qa_accounts
        
        FROM (
            
            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid,

            FROM 
                `syniti_mysql.syniti_db_target_accounts` main
            
            JOIN 
                `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `syniti.db_6sense_account_current_state` extra
            
            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            )

            WHERE 
                extra._6qa_date IS NOT NULL

        )
        GROUP BY 
            1

    )

    USING(_campaign_id)

),

-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (

    SELECT
    
        *,
        
        COUNT(*) OVER (
            PARTITION BY _campaign_id
        ) 
        AS _occurrence

    FROM 
        campaign_numbers

),

-- Reduced the campaign numbers by the occurrence
reduced_campaign_numbers AS (

    SELECT

        *,
        _newly_engaged_accounts / _occurrence AS _reduced_newly_engaged_accounts,
        _increased_engagement_accounts / _occurrence AS _reduced_increased_engagement_accounts,
        _target_accounts / _occurrence AS _reduced_target_accounts,
        _reached_accounts / _occurrence AS _reduced_reached_accounts,
        _6qa_accounts / _occurrence AS _reduced_6qa_accounts

    FROM 
        total_ad_occurrence_per_campaign

)

SELECT * FROM reduced_campaign_numbers;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ACCOUNT PERFORMANCES----------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `syniti.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        CASE
            WHEN main._segmentname = 'S4_HANA - NA' THEN 'S4/HANA - NA'
            WHEN main._segmentname = 'S4_HANA - EMEA' THEN 'S4/HANA - EMEA'
            WHEN main._segmentname = 'S4/HANA - APJ' THEN 'S4/HANA - APJ'
            ELSE main._segmentname
        END AS _segmentname,              
        side._segment,
        side._campaignid,
        side._campaignid AS _campaign_id,
        side._campaignname AS _campaign_name
        -- CASE 
        --     WHEN side._campaignname = '[Influitive] [6Sense] ABX Low Intent TAM - October 2023' 
        --     THEN '[Influitive] [6Sense] Low Intent TAM - October 2023'
        --     ELSE side._campaignname
        -- END AS _campaign_name -- temporary while fixing airtable

    FROM 
        `syniti_mysql.syniti_db_target_accounts` main
    
    JOIN 
        `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
    
    ON 
        main._segmentname = side._segment

),

-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (

    SELECT DISTINCT 

        main.* EXCEPT(_campaignid),

        CASE 
            WHEN side._campaignid IS NOT NULL 
            THEN true 
        END 
        AS _is_reached,

        CASE 
            WHEN CAST(REPLACE(side._clicks, ',', '') AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_clicks,

        CASE 
            WHEN CAST(REPLACE(side._impressions, ',', '') AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_impressions

    FROM 
        target_accounts AS main

    LEFT JOIN 
        `syniti_mysql.syniti_db_campaign_reached_accounts` side 

    USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
    )

)

SELECT * FROM reached_accounts;