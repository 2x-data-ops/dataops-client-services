CREATE OR REPLACE TABLE `smartcommnam.db_6sense_reached_account` AS

    WITH reached AS (
    SELECT
        * EXCEPT (_spend, _impressions),
        SAFE_CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
        SAFE_CAST(REGEXP_REPLACE(_spend, r'\$', '') AS FLOAT64) AS _spend,
    FROM `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY  _campaignid, _6sensecompanyname, _6sensecountry, _6sensedomain
        ORDER BY CASE
            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate) END DESC) = 1
    ),
    airtable AS (
        SELECT
            DISTINCT _campaignid,
            _campaignname,
            '' AS _campaigntype
        FROM `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense`
    )
    SELECT
        reached.*,
        airtable.* EXCEPT (_campaignid)
    FROM reached
    LEFT JOIN airtable
        ON reached._campaignid = airtable._campaignid;

-- SCRIPT CONTEXT AND OVERVIEW
-- Analyst needs to build ads performance pages only for the 6sense impact dashboard
-- Currently only ads performance and account performance table is connected in the dashboard


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_buying_stages_movement` AS

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
            SELECT DISTINCT
                ROW_NUMBER() OVER (PARTITION BY _6sensecompanyname, _6sensecountry, _6sensedomain ORDER BY
                        CASE
                            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                            ELSE PARSE_DATE('%F', _extractdate) END DESC) AS _rownum,
                CASE
                    WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                    ELSE PARSE_DATE('%F', _extractdate)
                END AS _activities_on,
                _6sensecompanyname,
                _6sensecountry, 
                _6sensedomain,
                CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account,
                '6sense' AS _data_source,
                _buyingstagestart AS _previous_stage,
                _buyingstageend AS _current_stage
            FROM `smartcommnam_mysql.smartcommnam_db_6sense_buying_stages_nam`
        ),
        latest_sixsense_buying_stage_with_order_and_movement AS (
            SELECT
                main.* EXCEPT (_rownum),
                prev._order AS _previous_stage_order,
                curr._order AS _current_stage_order,
                CASE
                    WHEN curr._order > prev._order THEN '+ve'
                    WHEN prev._order > curr._order THEN '-ve'
                    ELSE 'Stagnant'
                END AS _movement
            FROM sixsense_buying_stage_data AS main
            LEFT JOIN sixsense_stage_order AS prev
                ON main._previous_stage = prev._buying_stage
            LEFT JOIN sixsense_stage_order AS curr
                ON main._current_stage = curr._buying_stage
            WHERE main._rownum = 1
        )
    SELECT *
    FROM latest_sixsense_buying_stage_with_order_and_movement;


--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------- ACCOUNT CURRENT STATE
--------------------------------------------------------------------------
--------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_account_current_state` AS

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
            CASE
                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END AS _added_on,
            '6sense' AS _data_source,
            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
        FROM `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` 
        ) main
                        -- Get the earliest date of appearance of each account
            JOIN (
                SELECT DISTINCT 
                    MIN(CASE
                            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                            ELSE PARSE_DATE('%F', _extractdate)
                        END ) AS _added_on,
                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
                FROM `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam`
            ) scenario 
            ON main._country_account = scenario._country_account 
            AND main._added_on = scenario._added_on
    ),
    reached_related_info AS (  
        SELECT DISTINCT
            MIN(CASE
                    WHEN _extractdate LIKE '%/%' AND _latestimpression LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                    ELSE PARSE_DATE('%F', _latestimpression)
                END) OVER (PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain)) AS _first_impressions,
            CASE
                WHEN _websiteengagement = '-' THEN CAST(NULL AS STRING)
                ELSE _websiteengagement
            END AS _websiteengagement,
            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
        FROM `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam`
        WHERE _campaignid IN (  SELECT DISTINCT 
                                    _campaignid
                                FROM `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense`
                                WHERE _campaignid != '')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) ORDER BY CASE
            WHEN _extractdate LIKE '%/%' AND _latestimpression LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
            ELSE PARSE_DATE('%F', _latestimpression)
            END DESC) = 1      
    ),
    six_qa_related_info AS (
        SELECT DISTINCT
            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END AS _6qa_date,
            true _is_6qa,
            ROW_NUMBER() OVER(PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END DESC) AS _rownum,
            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
        FROM `smartcommnam_mysql.smartcommnam_db_6qa_account_nam`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END DESC) = 1
    ),
-- Get buying stage info for each account
    buying_stage_related_info AS (
        SELECT DISTINCT
            _previous_stage,
            _previous_stage_order,
            _current_stage,
            _current_stage_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,
        FROM `smartcommnam.db_6sense_buying_stages_movement`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY _country_account ORDER BY _activities_on DESC) = 1
    ),
-- Attach all other data parts to target accounts
    combined_data AS (
        SELECT DISTINCT 
            target.*, 
            reached.* EXCEPT(_country_account),
            six_qa.* EXCEPT(_country_account),
            stage.* EXCEPT(_country_account)   
        FROM target_accounts AS target
        LEFT JOIN reached_related_info AS reached 
            USING (_country_account)
        LEFT JOIN six_qa_related_info AS six_qa 
            USING(_country_account) 
        LEFT JOIN buying_stage_related_info AS stage
            USING(_country_account) 
    )
    SELECT * FROM combined_data;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ADS PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_ad_performance` AS

WITH ads AS (
    SELECT DISTINCT
        _campaignid,
        _name AS _advariation,
        _6senseid AS _adid,
        _accountvtr,
        CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
        CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
        -- SAFE_CAST(_impressions AS INTEGER) AS _impressions,
        SAFE_CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
        -- CASE 
        --     WHEN _budget = '-' THEN NULL
        --     ELSE SAFE_CAST(REGEXP_REPLACE(_budget, r'[^0-9.-]', '') AS FLOAT64)
        -- END AS _budget,
        CASE 
            WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
            WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
        END AS _date,
    FROM `smartcommnam_mysql.smartcommnam_db_6sense_campaign_performance_nam`
    WHERE _datatype = 'Ad'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignid, _6senseid, _date ORDER BY
        CASE 
            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate) END) = 1
    ),
-- Get campaign level fields

    campaign_fields AS (
        SELECT
            _campaignid,
            _accountvtr,
            _linkedincampaignid,
            -- _segment,
            CASE 
                WHEN _budget = '-' THEN NULL
                ELSE SAFE_CAST(REGEXP_REPLACE(_budget, r'[^0-9.-]', '') AS FLOAT64)
            END AS _budget,
            -- CASE
            --     WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            --     ELSE PARSE_DATE('%F', _extractdate)
            -- END AS _extractdate,
            CASE 
                WHEN _startDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _startDate)
                WHEN _startDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _startDate)
            END AS _start_date,
            CASE 
                WHEN _endDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _endDate)
                WHEN _endDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _endDate)
            END AS _end_date,
            _status AS _campaign_status,
            _name AS _campaign_name,
            _campaigntype AS _campaign_type, 
            CASE 
                WHEN _accountsnewlyengagedlifetime = '-' THEN 0
                ELSE SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
            END AS _newly_engaged_accounts,
            CASE 
                WHEN _accountswithincreasedengagementlifetime = '-' THEN 0
                ELSE SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
            END AS _increased_engagement_accounts, 
        FROM `smartcommnam_mysql.smartcommnam_db_6sense_campaign_performance_nam`
        WHERE _datatype = 'Campaign'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignid ORDER BY
            CASE
                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate) END DESC) = 1
    ),
    ads_campaign_combined AS (
        SELECT ads.*,
            campaign_fields._linkedincampaignid,
            -- campaign_fields._campaign_id,
            campaign_fields._campaign_name,
            campaign_fields._campaign_type,
            campaign_fields._campaign_status,
            campaign_fields._start_date,
            campaign_fields._end_date,
            campaign_fields._budget,
            campaign_fields._newly_engaged_accounts,
            campaign_fields._increased_engagement_accounts,
            -- campaign_fields._segment
        FROM ads
        JOIN campaign_fields
        ON ads._campaignid = campaign_fields._campaignid
    ),
    airtable_fields AS (
        SELECT DISTINCT 
            _campaignid, 
            _adid,
            _adgroup,
            _screenshot
        FROM `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense`
        WHERE _campaignid != ''
    ),
    combined_data AS (
        SELECT
            ads_campaign_combined.*,
            airtable_fields._adgroup,
            airtable_fields._screenshot,
        FROM ads_campaign_combined
        LEFT JOIN airtable_fields 
        ON (ads_campaign_combined._adid = airtable_fields._adid
            AND ads_campaign_combined._campaignid = airtable_fields._campaignid
        )
        OR (airtable_fields._adid IS NULL
            AND ads_campaign_combined._campaignid = airtable_fields._campaignid)

        LEFT JOIN campaign_fields
        ON ads_campaign_combined._campaignid = campaign_fields._campaignid
    ),

-- Add campaign numbers to each ad
    campaign_numbers AS (
        SELECT
            *
        FROM combined_data 
    -- Get accounts that are being targeted
        LEFT JOIN (
            SELECT DISTINCT
                _campaignid,
                COUNT(*) AS _target_accounts
            FROM (
                SELECT DISTINCT 
                    main._6sensecompanyname,
                    main._6sensecountry,
                    main._6sensedomain,
                    main._segmentname,
                    side._campaignid
                FROM `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
                JOIN `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
                ON main._segmentname = side._segment)
        ) target
    USING(_campaignid)

    -- Get accounts that have been reached
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid,
            COUNT(*) AS _reached_accounts
        FROM (
            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid
            FROM `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
            JOIN `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
            ON main._segmentname = side._segment
            JOIN `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` extra
            USING(_6sensecompanyname, _6sensecountry, _6sensedomain, _campaignid)
        )
    ) reach
    USING(_campaignid)

    -- Get accounts that are 6QA
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid,
            COUNT(*) AS _6qa_accounts
        FROM (
            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid,
            FROM `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
            JOIN `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
            ON main._segmentname = side._segment
            JOIN `smartcommnam.db_6sense_account_current_state` extra
            USING(_6sensecompanyname, _6sensecountry, _6sensedomain)
            WHERE extra._6qa_date IS NOT NULL
        )
    )
    USING(_campaignid)

    -- Get actr for each campaign - click divide reach
    LEFT JOIN (
      WITH main AS (
        SELECT DISTINCT 
          main._6sensecompanyname,
          main._6sensecountry,
          main._6sensedomain,
          main._segmentname,
          side._campaignid,
          CASE WHEN extra._clicks != '0' THEN 1 ELSE 0 END AS _clicked_account,
          CASE WHEN extra._campaignid != '' THEN 1 ELSE 0 END AS _reached_account
        FROM `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
        JOIN `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
        ON main._segmentname = side._segment
      JOIN `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` extra
        USING(_6sensecompanyname, _6sensecountry, _6sensedomain, _campaignid)
      )
      SELECT
          _campaignid,
          SAFE_DIVIDE(SUM(_clicked_account), SUM(_reached_account)) AS _accountctr  
      FROM main
      GROUP BY _campaignid
    )
    USING(_campaignid)
    ),


-- Get frequency of ad occurrence of each campaign
    total_ad_occurrence_per_campaign AS (
        SELECT
            *,
            COUNT(*) OVER (PARTITION BY _campaignid) AS _occurrence
        FROM campaign_numbers
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
        FROM total_ad_occurrence_per_campaign
    )
    SELECT * FROM reduced_campaign_numbers;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT+ENGAGEMENT PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_engagement_log` AS

WITH target_accounts AS (
    SELECT *
    FROM `smartcommnam.db_6sense_account_current_state`
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
            END AS _latestimpression,
            CASE 
                WHEN main._extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
                ELSE PARSE_DATE('%F', main._extractdate)
            END AS _activities_on, 
            main._campaignid,
            -- Need label to distingush 6sense and Linkedin campaigns
            -- side._campaigntype,
            side._campaignname,
            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
        FROM `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` main
        JOIN (
            SELECT DISTINCT 
                _campaignid, 
                _campaignname,  
                -- _campaigntype
            FROM `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense`
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
            MIN(_latestimpression) OVER(PARTITION BY _country_account, _campaignname
                ORDER BY _latestimpression) AS _timestamp,
            '6sense Campaign Reached' AS _engagement,
            '6sense' AS _engagement_data_source, 
            _campaignname AS _description, 
            1 AS _notes
        FROM reached_accounts_data
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
                -- Get last period's clicks to compare
                LAG(_clicks) OVER(
                    PARTITION BY _country_account, _campaignname
                    ORDER BY _activities_on
                ) AS _old_notes
            FROM reached_accounts_data 
            WHERE _clicks >= 1
            -- AND
            --     _campaigntype = '6sense Advertising' 
        )
        -- Get those who have increased in numbers from the last period
        WHERE (_notes - COALESCE(_old_notes, 0)) >= 1
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
                -- Get last period's clicks to compare
                LAG(_influencedformfills) OVER(
                    PARTITION BY _country_account, _campaignname
                    ORDER BY _activities_on) AS _old_notes
            FROM reached_accounts_data 
            WHERE _influencedformfills >= 1
            -- AND
            --     _campaigntype = '6sense Advertising'
        )
        -- Get those who have increased in numbers from the last period
        WHERE (_notes - COALESCE(_old_notes, 0)) >= 1
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
            WHEN _activitydate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _activitydate)
            ELSE PARSE_DATE('%F', _activitydate)
        END AS _activitydate,
        COUNT(*) AS _count
    FROM `smartcommnam_mysql.smartcommnam_db_6sense_activity_summary_nam`
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

    account_activity_summary_keyword_researched AS (
    SELECT DISTINCT
        _country_account,
        _activitydate AS _timestamp,
        _activitytype AS _engagement, 
        'Activity Summary Account' AS _engagement_data_source,
        _activitytarget AS _description,
        _count AS _notes
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
        _count AS _notes
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
            _count AS _notes
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
        ) activities
    JOIN target_accounts
    USING (_country_account)
    ),

-- Get accumulated values for each engagement
    accumulated_engagement_values AS (
        SELECT
            *,
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
            SUM(CASE WHEN _engagement = 'Current Bombora Company Surge Topics' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_bombora_topics
            -- SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
            -- SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click
    FROM combined_data     
    )
SELECT * FROM accumulated_engagement_values;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------



CREATE OR REPLACE TABLE `smartcommnam.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (
    SELECT DISTINCT 
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        -- CASE
        --     WHEN main.segment_name = 'S4_HANA - NA' THEN 'S4/HANA - NA'
        --     WHEN main.segment_name = 'S4_HANA - EMEA' THEN 'S4/HANA - EMEA'
        --     WHEN main.segment_name = 'S4/HANA - APJ' THEN 'S4/HANA - APJ'
        --     ELSE main.segment_name
        -- END AS _segmentname,              
        side._segment,
        side._campaignid,
        side._campaignname
    FROM `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
    JOIN `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
    ON main._segmentname = side._segment
    ),

-- Mark those target accounts that have been reached by their campaigns
    reached_accounts AS (
        SELECT DISTINCT 
            main.* EXCEPT(_campaignid),
        CASE 
            WHEN side._campaignid IS NOT NULL THEN true
            END AS _is_reached,
        CASE 
            WHEN SAFE_CAST(side._clicks AS INTEGER) > 0 THEN true 
            END AS _has_clicks,
        CASE 
            WHEN SAFE_CAST(side._impressions AS INTEGER) > 0 THEN true 
            END AS _has_impressions
        FROM target_accounts AS main
        LEFT JOIN `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` side
        USING(_6sensecompanyname, _6sensecountry, _6sensedomain, _campaignid)
    )
SELECT * FROM reached_accounts;

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
-----------------------OPPORTUNITY FROM SALESFORCE--------------
--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `smartcommnam.opportunity_influenced_accelerated` AS

-- Get account engagements of target account 
WITH target_account_engagements AS (
    SELECT DISTINCT 
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain, 
        _6qa_date,
        _engagement, 
        ROW_NUMBER() OVER() AS _eng_id,
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        -- _campaign_name,
        _notes AS _eng_notes,
        CASE
            WHEN _engagement LIKE '%6sense%' THEN '6sense'
            WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
        END AS _channel
    FROM `smartcommnam.db_6sense_engagement_log`
    -- WHERE _campaign_name != 'Upsert - Replication Q3/4 main'
    --     AND _campaign_name != "Unify - Global - Q3 '23 Match Tiger & Iceberg - Main"
    ),

-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
    opps_created AS (
        WITH closedConversionRate AS (
            SELECT DISTINCT
                opp.id,
                isocode,
                opp.closedate,
                rate.conversionrate,
                opp.amount / rate.conversionrate AS converted
            FROM `x-marketing.smartcomm_salesforce.DatedConversionRate` rate
            LEFT JOIN `x-marketing.smartcomm_salesforce.Opportunity` opp
                ON rate.isoCode = opp.currencyisocode
                AND opp.closedate >= rate.startDate
                AND opp.closedate < rate.nextStartDate
            WHERE opp.isclosed = true
            -- ORDER BY rate.startDate DESC
        ),
        openConversionRate AS (
            SELECT DISTINCT
                opp.id,
                isocode,
                rate.conversionrate,
                rate.lastmodifieddate,
                opp.closedate,
                -- opp.total_price__c,
                ROW_NUMBER() 
            FROM `x-marketing.smartcomm_salesforce.DatedConversionRate` rate
            LEFT JOIN `x-marketing.smartcomm_salesforce.Opportunity` opp
                ON opp.currencyisocode = rate.isocode
            WHERE opp.isclosed = false
                AND opp.currencyisocode != 'USD'
            QUALIFY ROW_NUMBER() OVER(PARTITION BY isocode ORDER BY rate.lastmodifieddate DESC) = 1
        ),
        opps_main AS (
            SELECT DISTINCT
                opp.accountid AS _account_id, 
                act.name AS _account_name,
                REGEXP_REPLACE(act.website, r'^(https?://)?www\.(.*?)(?:/|$)', r'\2') AS _domain,
                COALESCE(act.shippingcountry, act.billingcountry) AS _country,
                opp.id AS _opp_id,
                opp.name AS _opp_name,
                own.name AS _opp_owner_name,
                opp.type AS _opp_type,
                DATE(opp.createddate) AS _created_date,
                DATE(opp.closedate) AS _closed_date,
                opp.amount AS _amount,
                opp.currencyisocode,
                opp.isclosed,
                opp.region__c AS _region,
                -- For filling up those opps with missing first stage in the opp history
                opp.stagename AS _current_stage,
                DATE(opp.laststagechangedate) AS _stage_change_date
            FROM `smartcomm_salesforce.Opportunity` opp
            LEFT JOIN `smartcomm_salesforce.Account` act
                ON opp.accountid = act.id 
            LEFT JOIN`smartcomm_salesforce.User` own
                ON opp.ownerid = own.id 
            WHERE opp.isdeleted = false
            --no need filtering - get all the the opportunity
            --   AND 
            --       opp.createddate >= '2023-01-01'
        )
        SELECT
            *
        FROM (
            SELECT DISTINCT
            opps_main.* EXCEPT(_amount),
            -- Opportunity.opportunityID,
            -- Opportunity.createddate,
            -- Opportunity.isclosed,
            -- Opportunity.currencyisocode,
            _amount AS original_amount,
            CASE 
                WHEN isclosed = true AND currencyisocode != 'USD' THEN (closedConversionRate.conversionRate)
                WHEN isclosed = false AND currencyisocode != 'USD' THEN (openConversionRate.conversionRate )
            END AS conversionRate,
            CASE 
                WHEN isclosed = true AND currencyisocode != 'USD' THEN (closedConversionRate.converted)
                WHEN isclosed = false AND currencyisocode != 'USD' THEN ((_amount / openConversionRate.conversionrate) )
                ELSE _amount
            END AS _amount_converted,
            -- sfdc_activity_casesafeid__c,
            -- application_specialist__c,
            -- Event_Status__c,
            -- Web_Location__c
            FROM opps_main
            LEFT JOIN closedConversionRate
                ON closedConversionRate.id = opps_main._opp_id
            LEFT JOIN openConversionRate
                ON openConversionRate.isocode = opps_main.currencyisocode
            )
    --no need filtering - get all the the opportunity
    --   WHERE _created_date >= '2023-01-01'
    ),

    -- Get all historical stages of opp
    -- Perform necessary cleaning of the data
    opps_historical_stage AS (
    SELECT
        main.*,
        side._previous_stage_prob,
        side._next_stage_prob
    FROM (
        SELECT DISTINCT 
            opportunityid AS _opp_id,
            createddate AS _historical_stage_change_timestamp,
            DATE(createddate) AS _historical_stage_change_date,
            oldvalue AS _previous_stage,
            newvalue AS _next_stage
        FROM `smartcomm_salesforce.OpportunityFieldHistory` 
        WHERE field = 'StageName'
        AND isdeleted = false
    ) main
    JOIN (
        SELECT DISTINCT 
            opportunityid AS _opp_id,
            createddate AS _historical_stage_change_timestamp,
            oldvalue__fl AS _previous_stage_prob,
            newvalue__fl AS _next_stage_prob,
        FROM `smartcomm_salesforce.OpportunityFieldHistory`
        WHERE field = 'ForecastProbability__c'
        AND isdeleted = false
    ) side
    USING (_opp_id, _historical_stage_change_timestamp)
    ),

    -- There are several stages that can occur on the same day
    -- Get unique stage on each day 
    unique_opps_historical_stage AS (
        SELECT
            * EXCEPT(_rownum),
            -- Setting the rank of the historical stage based on stage change date
            ROW_NUMBER() OVER(PARTITION BY _opp_id ORDER BY _historical_stage_change_date DESC) AS _stage_rank
        FROM (
            SELECT
                *, 
                -- Those on same day are differentiated by timestamp
                ROW_NUMBER() OVER (PARTITION BY _opp_id, _historical_stage_change_date ORDER BY _historical_stage_change_timestamp DESC) AS _rownum
            FROM opps_historical_stage
        )
        WHERE _rownum = 1
    ),

    -- Generate a log to store stage history from latest to earliest
    get_aggregated_stage_history_text AS (
        SELECT
            *,
            STRING_AGG( 
                CONCAT(
                    '[ ', _historical_stage_change_date, ' ]',
                    ' : ', _next_stage),'; ') 
            OVER (PARTITION BY _opp_id ORDER BY _stage_rank) AS _stage_history
        FROM unique_opps_historical_stage
    ),

    -- Obtain the current stage and the stage date in this CTE 
    get_current_stage_and_date AS (
        SELECT
            *,
            CASE 
                WHEN _stage_rank = 1 THEN _historical_stage_change_date
            END AS _stage_change_date,
            CASE 
                WHEN _stage_rank = 1 THEN _next_stage
            END AS _current_stage
        FROM get_aggregated_stage_history_text
    ),

    -- Add the stage related fields to the opps data
    opps_history AS (
        SELECT
            -- Remove the current stage from the opp created CTE
            main.* EXCEPT(_current_stage, _stage_change_date),
            -- Fill the current stage and date for an opp
            -- Will be the same in each row of an opp
            -- If no stage and date, get the stage and date from the opp created CTE
            COALESCE(
                main._stage_change_date,
                MAX(side._stage_change_date) OVER (PARTITION BY side._opp_id),
                main._created_date) AS _stage_change_date,
            COALESCE(
                MAX(side._current_stage) OVER (PARTITION BY side._opp_id),
                main._current_stage) AS _current_stage,

            -- Set the stage history to aid crosscheck
            MAX(side._stage_history) OVER (PARTITION BY side._opp_id) AS _stage_history,

            -- The stage and date fields here represent those of each historical stage
            -- Will be different in each row of an opp
            side._historical_stage_change_date,
            side._next_stage AS _historical_stage,

            -- Set the stage movement 
            CASE
                WHEN side._previous_stage_prob > side._next_stage_prob THEN 'Downward' 
                ELSE 'Upward'
            END AS _stage_movement
        FROM opps_created main
        LEFT JOIN get_current_stage_and_date side
            ON main._opp_id = side._opp_id
    ),

    -- Tie opportunities with stage history and account engagements
    combined_data AS (
    SELECT
        opp.*,
        act.*,
        CASE
            WHEN act._engagement IS NOT NULL THEN true 
        END AS _is_matched_opp
    FROM opps_history opp
    LEFT JOIN target_account_engagements act     
       ON (opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
            -- opp._domain = act._6sensedomain
        AND LENGTH(opp._domain) > 1
        AND LENGTH(act._6sensedomain) > 1)
    
        OR (opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
                -- opp._domain = act._6sensedomain
            AND LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
            AND LENGTH(opp._account_name) > 1
            AND LENGTH(act._6sensecompanyname) > 1)

        OR (LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
            AND LENGTH(opp._account_name) > 1
            AND LENGTH(act._6sensecompanyname) > 1)
    ),

    -- Label the activty that influenced the opportunity
    set_influencing_activity AS (
        SELECT
            *,
            CASE 
                WHEN DATE(_eng_timestamp) BETWEEN DATE_SUB(_created_date, INTERVAL 90 DAY) AND DATE(_created_date) THEN true 
            END AS _is_influencing_activity
        FROM combined_data
    ),

    -- Mark every other rows of the opportunity as influenced 
    -- If there is at least one influencing activity
    label_influenced_opportunity AS (
        SELECT
            *,
            MAX(_is_influencing_activity) OVER(PARTITION BY _opp_id)AS _is_influenced_opp
        FROM set_influencing_activity

    ),

    -- Label the activty that accelerated the opportunity
    set_accelerating_activity AS (
        SELECT 
            *,
            CASE 
                WHEN _is_influenced_opp IS NULL AND _eng_timestamp > _created_date AND _eng_timestamp <= _historical_stage_change_date AND _stage_movement = 'Upward'
                -- AND 
                --     REGEXP_CONTAINS(
                --         _engagement, 
                --         '6sense Campaign|6sense Ad|6sense Form|LinkedIn Campaign|LinkedIn Ad'
                --     )
                THEN true
            END AS _is_accelerating_activity
        FROM label_influenced_opportunity

    ),

    -- Mark every other rows of the opportunity as accelerated 
    -- If there is at least one accelerating activity
    label_accelerated_opportunity AS (
        SELECT
            *,
            MAX(_is_accelerating_activity) OVER (PARTITION BY _opp_id) AS _is_accelerated_opp
        FROM set_accelerating_activity

    ),

    -- Label the activty that accelerated an influenced opportunity
    set_accelerating_activity_for_influenced_opportunity AS (
        SELECT 
            *,
            CASE 
                WHEN _is_influenced_opp IS NOT NULL AND _eng_timestamp > _created_date AND _eng_timestamp <= _historical_stage_change_date AND _stage_movement = 'Upward'
                -- AND 
                --     REGEXP_CONTAINS(
                --         _engagement, 
                --         '6sense Campaign|6sense Ad|6sense Form|LinkedIn Campaign|LinkedIn Ad'
                --     )
                THEN true
            END AS _is_later_accelerating_activity
        FROM label_accelerated_opportunity

    ),

    -- Mark every other rows of the opportunity as infuenced cum accelerated 
    -- If there is at least one accelerating activity for the incluenced opp
    label_influenced_opportunity_that_continue_to_accelerate AS ( 
        SELECT
            *,
            MAX(_is_later_accelerating_activity) OVER(PARTITION BY _opp_id) AS _is_later_accelerated_opp
        FROM set_accelerating_activity_for_influenced_opportunity
    ),

    -- Mark opportunities that were matched but werent influenced or accelerated or influenced cum accelerated as stagnant 
    label_stagnant_opportunity AS (
        SELECT
            *,
            CASE
                WHEN _is_matched_opp = true 
                AND _is_influenced_opp IS NULL 
                AND _is_accelerated_opp IS NULL 
                AND _is_later_accelerated_opp IS NULL
                THEN true 
            END AS _is_stagnant_opp
        FROM label_influenced_opportunity_that_continue_to_accelerate

    ),

    -- Get the latest stage of each opportunity 
    -- While carrying forward all its boolean fields' value caused by its historical changes 
    latest_stage_opportunity_only AS (
        SELECT DISTINCT
            -- Remove fields that are unique for each historical stage of opp
            * EXCEPT(_historical_stage_change_date, _historical_stage, _stage_movement),
            -- For removing those with values in the activity boolean fields
            -- Different historical stages may have caused the influencing or accelerating
            -- This is unlike the opportunity boolean that is uniform among the all historical stage of opp 
        FROM label_stagnant_opportunity
        QUALIFY ROW_NUMBER() OVER (PARTITION BY _opp_id, _eng_id ORDER BY _is_influencing_activity DESC, _is_accelerating_activity DESC, _is_later_accelerating_activity DESC) = 1 
    )
SELECT * FROM latest_stage_opportunity_only;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- Opportunity Influenced + Accelerated Without Engagements

CREATE OR REPLACE TABLE `smartcommnam.opportunity_summarized` AS

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
      _amount_converted,
      _region,
      _stage_change_date,
      _current_stage,
      _stage_history,
      _6sensecompanyname,
      _6sensecountry,
      _6sensedomain,
      -- _campaign_name,
      _is_matched_opp,
      _is_influenced_opp,
      MAX(_is_influencing_activity) OVER (PARTITION BY _opp_id, _channel) AS _is_influencing_activity,
      _is_accelerated_opp,
      MAX(_is_accelerating_activity) OVER (PARTITION BY _opp_id, _channel) AS _is_accelerating_activity,
      _is_later_accelerated_opp,
      MAX(_is_later_accelerating_activity) OVER (PARTITION BY _opp_id, _channel) AS _is_later_accelerating_activity,
      _is_stagnant_opp,
      _channel
      FROM `smartcommnam.opportunity_influenced_accelerated`;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT ACTIVITY SUMMARY (SOURCE FROM MYSQL)
----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `smartcommnam.db_6sense_activity_summary`
CLUSTER BY _activitytype AS
    SELECT *
    FROM `smartcommnam_mysql.smartcommnam_db_6sense_activity_summary_nam`;